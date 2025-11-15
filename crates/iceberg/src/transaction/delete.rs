// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! DELETE operation implementation.
//!
//! This module provides the DELETE operation for removing rows from Iceberg tables
//! based on filter predicates. DELETE supports two execution modes:
//!
//! - **MergeOnRead (MOR)**: Writes position delete files marking rows for deletion.
//!   Fast but requires readers to merge deletes. Best for small deletes.
//!
//! - **CopyOnWrite (COW)**: Rewrites data files without deleted rows.
//!   Slower but cleaner. Best when deleting large portions of files.
//!
//! - **Auto**: Automatically chooses between MOR and COW based on heuristics.
//!
//! # Example
//!
//! ```rust,no_run
//! use iceberg::transaction::Transaction;
//! use iceberg::expr::Reference;
//! # use iceberg::Result;
//!
//! # async fn example(table: iceberg::table::Table) -> Result<()> {
//! // Create a transaction
//! let tx = Transaction::new(&table);
//!
//! // Delete rows where age > 100
//! let delete_action = tx
//!     .delete()
//!     .with_filter(Reference::new("age").greater_than(100))
//!     .with_auto_mode(0.2); // Use COW if >20% of rows deleted
//!
//! // Apply and commit
//! let tx = delete_action.apply(tx)?;
//! // tx.commit(&catalog).await?;
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::error::Result;
use crate::expr::Predicate;
use crate::spec::{ManifestEntry, ManifestFile, Operation};
use crate::table::Table;
use crate::transaction::snapshot::{SnapshotProduceOperation, SnapshotProducer};
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind};

/// Mode for DELETE operation execution.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DeleteMode {
    /// Write position delete files (Merge-on-Read).
    ///
    /// This mode writes position delete files that mark specific rows for deletion.
    /// Reading requires merging deletes with data files. Fast for writes, slower for reads.
    ///
    /// Best for:
    /// - Small number of deleted rows relative to file size
    /// - Write-heavy workloads
    /// - When you want to minimize write latency
    MergeOnRead,

    /// Rewrite data files without deleted rows (Copy-on-Write).
    ///
    /// This mode rewrites affected data files with deleted rows removed.
    /// Reading is fast, but writes are expensive.
    ///
    /// Best for:
    /// - Large portion of rows deleted from files
    /// - Read-heavy workloads
    /// - When you want to minimize read overhead
    CopyOnWrite,

    /// Automatically choose based on delete ratio heuristic.
    ///
    /// The threshold is the ratio of deleted rows to total rows in affected files.
    /// If the ratio exceeds the threshold, use CopyOnWrite; otherwise use MergeOnRead.
    ///
    /// For example, `Auto { cow_threshold: 0.2 }` means:
    /// - If >20% of rows are deleted → use CopyOnWrite
    /// - If ≤20% of rows are deleted → use MergeOnRead
    Auto {
        /// Threshold ratio (0.0-1.0) above which to use CopyOnWrite.
        cow_threshold: f64,
    },
}

impl Default for DeleteMode {
    fn default() -> Self {
        // Default to Auto mode with 20% threshold
        // This balances write performance and read performance
        DeleteMode::Auto {
            cow_threshold: 0.2,
        }
    }
}

/// DELETE operation action.
///
/// This action removes rows from the table based on a filter predicate.
/// Rows matching the filter are deleted.
///
/// # Example
///
/// ```rust,no_run
/// use iceberg::transaction::Transaction;
/// use iceberg::expr::Reference;
/// # use iceberg::Result;
///
/// # async fn example(table: iceberg::table::Table) -> Result<()> {
/// let tx = Transaction::new(&table);
///
/// // Delete rows where status = 'inactive'
/// let delete_action = tx
///     .delete()
///     .with_filter(Reference::new("status").equal_to("inactive"))
///     .with_merge_on_read_mode();
///
/// let tx = delete_action.apply(tx)?;
/// # Ok(())
/// # }
/// ```
pub struct DeleteAction {
    /// Filter predicate - rows matching this filter will be deleted
    delete_filter: Option<Predicate>,
    /// Execution mode (MOR, COW, or Auto)
    delete_mode: DeleteMode,
    /// Properties to set on the snapshot
    snapshot_properties: HashMap<String, String>,
    /// Commit UUID for tracking
    commit_uuid: Option<Uuid>,
    /// Key metadata for encryption
    key_metadata: Option<Vec<u8>>,
}

impl DeleteAction {
    pub(crate) fn new() -> Self {
        Self {
            delete_filter: None,
            delete_mode: DeleteMode::default(),
            snapshot_properties: HashMap::default(),
            commit_uuid: None,
            key_metadata: None,
        }
    }

    /// Set the filter for rows to delete.
    ///
    /// All rows matching this filter will be deleted from the table.
    ///
    /// # Arguments
    ///
    /// * `filter` - A predicate defining which rows to delete
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use iceberg::expr::Reference;
    /// # use iceberg::transaction::Transaction;
    /// # use iceberg::Result;
    ///
    /// # fn example(tx: Transaction) -> Result<()> {
    /// // Delete rows where age > 100
    /// let action = tx.delete()
    ///     .with_filter(Reference::new("age").greater_than(100));
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_filter(mut self, filter: Predicate) -> Self {
        self.delete_filter = Some(filter);
        self
    }

    /// Use MergeOnRead mode (write position delete files).
    ///
    /// This is the fastest mode for writes but requires readers to merge deletes.
    pub fn with_merge_on_read_mode(mut self) -> Self {
        self.delete_mode = DeleteMode::MergeOnRead;
        self
    }

    /// Use CopyOnWrite mode (rewrite data files).
    ///
    /// This mode rewrites data files without deleted rows.
    /// Slower for writes, faster for reads.
    pub fn with_copy_on_write_mode(mut self) -> Self {
        self.delete_mode = DeleteMode::CopyOnWrite;
        self
    }

    /// Use Auto mode with a custom threshold.
    ///
    /// If the ratio of deleted rows exceeds `cow_threshold`, CopyOnWrite is used;
    /// otherwise MergeOnRead is used.
    ///
    /// # Arguments
    ///
    /// * `cow_threshold` - Ratio (0.0-1.0) above which to use CopyOnWrite
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use iceberg::transaction::Transaction;
    /// # use iceberg::expr::Reference;
    /// # use iceberg::Result;
    ///
    /// # fn example(tx: Transaction) -> Result<()> {
    /// // Use COW if >30% of rows are deleted, MOR otherwise
    /// let action = tx.delete()
    ///     .with_filter(Reference::new("deleted").equal_to(true))
    ///     .with_auto_mode(0.3);
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_auto_mode(mut self, cow_threshold: f64) -> Self {
        if cow_threshold < 0.0 || cow_threshold > 1.0 {
            // Clamp to valid range
            let clamped = cow_threshold.max(0.0).min(1.0);
            self.delete_mode = DeleteMode::Auto {
                cow_threshold: clamped,
            };
        } else {
            self.delete_mode = DeleteMode::Auto { cow_threshold };
        }
        self
    }

    /// Set commit UUID for the snapshot.
    pub fn set_commit_uuid(mut self, commit_uuid: Uuid) -> Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }

    /// Set key metadata for manifest files.
    pub fn set_key_metadata(mut self, key_metadata: Vec<u8>) -> Self {
        self.key_metadata = Some(key_metadata);
        self
    }

    /// Set snapshot summary properties.
    pub fn set_snapshot_properties(
        mut self,
        snapshot_properties: HashMap<String, String>,
    ) -> Self {
        self.snapshot_properties = snapshot_properties;
        self
    }
}

#[async_trait]
impl TransactionAction for DeleteAction {
    async fn commit(self: Arc<Self>, _table: &Table) -> Result<ActionCommit> {
        // Validate that a filter was provided
        let delete_filter = self.delete_filter.as_ref().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "DELETE operation requires a filter predicate. Use with_filter() to specify which rows to delete."
            )
        })?;

        // TODO: Phase 1 - Implement MergeOnRead strategy
        // For now, return an error indicating this is not yet implemented
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            format!(
                "DELETE operation is not yet fully implemented. \
                Filter: {:?}, Mode: {:?}. \
                Coming in Phase 1, Week 4-8 of the implementation plan.",
                delete_filter, self.delete_mode
            ),
        ))

        // TODO: Implementation steps:
        //
        // 1. Scan table to find files matching the delete filter
        //    - Use table.scan().with_filter(delete_filter).plan_files().await?
        //    - Get list of affected data files
        //
        // 2. For each affected file:
        //    - Read the file
        //    - Apply filter to find matching rows
        //    - Record positions of deleted rows
        //
        // 3. Based on delete mode:
        //    a) MergeOnRead:
        //       - Use PositionDeleteFileWriter to write delete files
        //       - Add delete files to snapshot
        //    b) CopyOnWrite:
        //       - Rewrite data files without deleted rows
        //       - Remove old files, add new files to snapshot
        //    c) Auto:
        //       - Calculate delete ratio
        //       - Choose MOR or COW based on threshold
        //
        // 4. Create snapshot with:
        //    - operation: Operation::Delete
        //    - deleted_data_files: files removed (COW) or empty (MOR)
        //    - added_delete_files: position deletes (MOR) or empty (COW)
        //    - added_data_files: rewritten files (COW) or empty (MOR)
        //
        // 5. Return ActionCommit with table updates and requirements
    }
}

struct DeleteOperation;

impl SnapshotProduceOperation for DeleteOperation {
    fn operation(&self) -> Operation {
        Operation::Delete
    }

    async fn delete_entries(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        // TODO: Return manifest entries for deleted data files
        Ok(vec![])
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        // For DELETE, we keep existing manifests that don't contain deleted data
        let Some(snapshot) = snapshot_produce.table.metadata().current_snapshot() else {
            return Ok(vec![]);
        };

        let manifest_list = snapshot
            .load_manifest_list(
                snapshot_produce.table.file_io(),
                &snapshot_produce.table.metadata_ref(),
            )
            .await?;

        // TODO: Filter out manifests containing only deleted files
        // For now, return all manifests
        Ok(manifest_list.entries().iter().cloned().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::Reference;
    use crate::spec::Datum;

    #[test]
    fn test_delete_mode_default() {
        let mode = DeleteMode::default();
        match mode {
            DeleteMode::Auto { cow_threshold } => {
                assert_eq!(cow_threshold, 0.2);
            }
            _ => panic!("Default should be Auto mode with 0.2 threshold"),
        }
    }

    #[test]
    fn test_delete_action_builder() {
        let action = DeleteAction::new()
            .with_filter(Reference::new("age").greater_than(Datum::int(100)))
            .with_merge_on_read_mode();

        assert!(action.delete_filter.is_some());
        assert_eq!(action.delete_mode, DeleteMode::MergeOnRead);
    }

    #[test]
    fn test_delete_action_auto_mode_clamping() {
        // Test that out-of-range thresholds are clamped
        let action = DeleteAction::new().with_auto_mode(1.5);
        match action.delete_mode {
            DeleteMode::Auto { cow_threshold } => {
                assert_eq!(cow_threshold, 1.0);
            }
            _ => panic!("Should be Auto mode"),
        }

        let action = DeleteAction::new().with_auto_mode(-0.5);
        match action.delete_mode {
            DeleteMode::Auto { cow_threshold } => {
                assert_eq!(cow_threshold, 0.0);
            }
            _ => panic!("Should be Auto mode"),
        }
    }

    #[test]
    fn test_delete_mode_selection() {
        let mor_action = DeleteAction::new().with_merge_on_read_mode();
        assert_eq!(mor_action.delete_mode, DeleteMode::MergeOnRead);

        let cow_action = DeleteAction::new().with_copy_on_write_mode();
        assert_eq!(cow_action.delete_mode, DeleteMode::CopyOnWrite);

        let auto_action = DeleteAction::new().with_auto_mode(0.3);
        match auto_action.delete_mode {
            DeleteMode::Auto { cow_threshold } => {
                assert_eq!(cow_threshold, 0.3);
            }
            _ => panic!("Should be Auto mode"),
        }
    }
}
