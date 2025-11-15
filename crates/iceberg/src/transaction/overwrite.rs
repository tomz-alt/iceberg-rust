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

//! OVERWRITE operation implementation.
//!
//! This module provides the OVERWRITE operation for replacing data in Iceberg tables.
//! OVERWRITE supports two execution modes:
//!
//! - **Dynamic Overwrite**: Replaces data in specific partitions only.
//!   Other partitions remain untouched. Requires a partition filter.
//!
//! - **Static Overwrite**: Replaces ALL data in the table.
//!   Completely rewrites the table with new data.
//!
//! # Current Implementation Status
//!
//! ✅ **Fully Implemented**:
//! - Dynamic overwrite with partition filtering
//! - Static overwrite (full table replacement)
//! - SnapshotProducer integration
//! - Proper manifest management
//! - Snapshot creation with Operation::Overwrite
//!
//! # Example
//!
//! ```rust,no_run
//! use iceberg::transaction::Transaction;
//! use iceberg::expr::Reference;
//! use iceberg::spec::DataFile;
//! # use iceberg::Result;
//!
//! # async fn example(table: iceberg::table::Table, new_files: Vec<DataFile>) -> Result<()> {
//! // Dynamic overwrite - replace specific partition
//! let tx = Transaction::new(&table);
//! let overwrite_action = tx
//!     .overwrite()
//!     .with_partition_filter(Reference::new("date").equal_to("2024-01-01"))
//!     .with_data_files(new_files);
//!
//! // Commit the overwrite
//! let tx = overwrite_action.apply(tx)?;
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
use crate::spec::{DataContentType, DataFile, ManifestEntry, ManifestFile, Operation};
use crate::table::Table;
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer,
};
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind};

/// OverwriteAction is a transaction action for overwriting data in the table.
///
/// Supports two modes:
/// - Dynamic: Replaces data in specific partitions (requires partition filter)
/// - Static: Replaces ALL data in the table (no filter)
pub struct OverwriteAction {
    mode: OverwriteMode,
    // Properties for SnapshotProducer
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    added_data_files: Vec<DataFile>,
}

#[derive(Debug, Clone)]
enum OverwriteMode {
    /// Dynamic overwrite: replace data in specific partitions only
    Dynamic(Predicate),
    /// Static overwrite: replace ALL data in the table
    Static,
}

impl OverwriteAction {
    pub(crate) fn new() -> Self {
        Self {
            mode: OverwriteMode::Static, // Default to static (full table replacement)
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::default(),
            added_data_files: vec![],
        }
    }

    /// Set partition filter for dynamic overwrite.
    ///
    /// When set, only data files matching the partition filter will be replaced.
    /// Files in other partitions remain untouched.
    ///
    /// # Example
    /// ```rust,no_run
    /// use iceberg::expr::Reference;
    /// # use iceberg::transaction::Transaction;
    /// # let table = todo!();
    /// # let tx = Transaction::new(&table);
    ///
    /// // Replace all data for January 2024
    /// let overwrite = tx
    ///     .overwrite()
    ///     .with_partition_filter(Reference::new("date").equal_to("2024-01-01"));
    /// ```
    pub fn with_partition_filter(mut self, filter: impl Into<Predicate>) -> Self {
        self.mode = OverwriteMode::Dynamic(filter.into());
        self
    }

    /// Use static overwrite mode (replace entire table).
    ///
    /// This is the default mode. Explicitly calling this method is optional.
    pub fn with_static_mode(mut self) -> Self {
        self.mode = OverwriteMode::Static;
        self
    }

    /// Add data files to write in the overwrite operation.
    pub fn with_data_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_data_files.extend(data_files);
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
impl TransactionAction for OverwriteAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        // Validate that we have data files to write
        if self.added_data_files.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Cannot perform overwrite without any data files. Use add_data_files() to specify files to write.",
            ));
        }

        let snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.key_metadata.clone(),
            self.snapshot_properties.clone(),
            self.added_data_files.clone(),
        );

        // Validate added files
        snapshot_producer.validate_added_data_files()?;

        // Commit with appropriate operation based on mode
        match &self.mode {
            OverwriteMode::Dynamic(filter) => {
                let operation = DynamicOverwriteOperation {
                    partition_filter: filter.clone(),
                };
                snapshot_producer.commit(operation, DefaultManifestProcess).await
            }
            OverwriteMode::Static => {
                snapshot_producer
                    .commit(StaticOverwriteOperation, DefaultManifestProcess)
                    .await
            }
        }
    }
}

/// Dynamic overwrite operation - replaces data in specific partitions
struct DynamicOverwriteOperation {
    partition_filter: Predicate,
}

impl SnapshotProduceOperation for DynamicOverwriteOperation {
    fn operation(&self) -> Operation {
        Operation::Overwrite
    }

    async fn delete_entries(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        let Some(snapshot) = snapshot_produce.table.metadata().current_snapshot() else {
            // No existing snapshot, nothing to delete
            return Ok(vec![]);
        };

        // Use table scan to find files matching the partition filter
        // The scan handles partition pruning automatically
        let scan = snapshot_produce
            .table
            .scan()
            .with_filter(self.partition_filter.clone())
            .build()?;

        let file_tasks = scan.plan_files().await?;
        use futures::TryStreamExt;
        let tasks: Vec<_> = file_tasks.try_collect().await?;

        // Collect file paths from matched tasks
        let matched_file_paths: std::collections::HashSet<String> = tasks
            .iter()
            .map(|task| task.data_file_path().to_string())
            .collect();

        // Now collect entries for these files
        let manifest_list = snapshot
            .load_manifest_list(
                snapshot_produce.table.file_io(),
                &snapshot_produce.table.metadata_ref(),
            )
            .await?;

        let mut entries_to_delete = vec![];

        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file
                .load_manifest(snapshot_produce.table.file_io())
                .await?;

            for entry in manifest.entries() {
                // Only delete data files that match our scan results
                if entry.is_alive()
                    && entry.data_file().content_type() == DataContentType::Data
                    && matched_file_paths.contains(entry.file_path())
                {
                    entries_to_delete.push(entry.as_ref().clone());
                }
            }
        }

        Ok(entries_to_delete)
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        let Some(snapshot) = snapshot_produce.table.metadata().current_snapshot() else {
            return Ok(vec![]);
        };

        let manifest_list = snapshot
            .load_manifest_list(
                snapshot_produce.table.file_io(),
                &snapshot_produce.table.metadata_ref(),
            )
            .await?;

        // Keep all existing manifests - deleted entries will be marked in new manifest
        Ok(manifest_list
            .entries()
            .iter()
            .filter(|entry| entry.has_added_files() || entry.has_existing_files())
            .cloned()
            .collect())
    }
}

/// Static overwrite operation - replaces ALL data in the table
struct StaticOverwriteOperation;

impl SnapshotProduceOperation for StaticOverwriteOperation {
    fn operation(&self) -> Operation {
        Operation::Overwrite
    }

    async fn delete_entries(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        let Some(snapshot) = snapshot_produce.table.metadata().current_snapshot() else {
            // No existing snapshot, nothing to delete
            return Ok(vec![]);
        };

        // Load current manifest list
        let manifest_list = snapshot
            .load_manifest_list(
                snapshot_produce.table.file_io(),
                &snapshot_produce.table.metadata_ref(),
            )
            .await?;

        // Collect ALL data files for deletion (full table overwrite)
        let mut entries_to_delete = vec![];

        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file
                .load_manifest(snapshot_produce.table.file_io())
                .await?;

            for entry in manifest.entries() {
                // Delete all data files (keep delete files for now)
                if entry.is_alive() && entry.data_file().content_type() == DataContentType::Data {
                    entries_to_delete.push(entry.as_ref().clone());
                }
            }
        }

        Ok(entries_to_delete)
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        // For static overwrite, we still keep existing manifests
        // The deleted entries will be properly tracked in new manifests
        let Some(snapshot) = snapshot_produce.table.metadata().current_snapshot() else {
            return Ok(vec![]);
        };

        let manifest_list = snapshot
            .load_manifest_list(
                snapshot_produce.table.file_io(),
                &snapshot_produce.table.metadata_ref(),
            )
            .await?;

        Ok(manifest_list
            .entries()
            .iter()
            .filter(|entry| entry.has_added_files() || entry.has_existing_files())
            .cloned()
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::Reference;
    use crate::spec::Datum;

    fn make_v2_minimal_table() -> Table {
        crate::transaction::tests::make_v2_minimal_table()
    }

    #[test]
    fn test_overwrite_action_builder() {
        let action = OverwriteAction::new()
            .with_partition_filter(Reference::new("date").equal_to(Datum::string("2024-01-01")))
            .with_data_files(vec![]);

        match action.mode {
            OverwriteMode::Dynamic(_) => { /* OK */ }
            OverwriteMode::Static => panic!("Expected Dynamic mode"),
        }
    }

    #[test]
    fn test_overwrite_mode_default() {
        let action = OverwriteAction::new();
        match action.mode {
            OverwriteMode::Static => { /* OK - default is static */ }
            OverwriteMode::Dynamic(_) => panic!("Expected Static mode by default"),
        }
    }

    #[test]
    fn test_overwrite_mode_explicit_static() {
        let action = OverwriteAction::new().with_static_mode();
        match action.mode {
            OverwriteMode::Static => { /* OK */ }
            OverwriteMode::Dynamic(_) => panic!("Expected Static mode"),
        }
    }

    #[tokio::test]
    async fn test_overwrite_requires_data_files() {
        let table = make_v2_minimal_table();

        // Try to overwrite without any data files
        let overwrite_action = OverwriteAction::new();

        let result = Arc::new(overwrite_action).commit(&table).await;

        assert!(result.is_err());
        if let Err(err) = result {
            assert_eq!(err.kind(), ErrorKind::DataInvalid);
            assert!(err
                .to_string()
                .contains("Cannot perform overwrite without any data files"));
        }
    }
}
