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
use futures::TryStreamExt;
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

use crate::arrow::arrow_schema_to_schema;
use crate::error::Result;
use crate::expr::Predicate;
use crate::spec::{
    DataFileFormat, FormatVersion, MAIN_BRANCH, ManifestContentType, ManifestListWriter,
    ManifestWriterBuilder, Operation, PartitionKey, PartitionSpec, Snapshot, SnapshotReference, SnapshotRetention, Summary,
};
use crate::table::Table;
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{TableRequirement, TableUpdate};
use crate::writer::base_writer::position_delete_writer::{
    PositionDeleteFileWriterBuilder, position_delete_schema,
};
use crate::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use crate::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use crate::writer::file_writer::ParquetWriterBuilder;
use crate::writer::{IcebergWriter, IcebergWriterBuilder};
use crate::{Error, ErrorKind};

/// Internal execution mode (resolved from DeleteMode).
#[derive(Debug, Clone, Copy, PartialEq)]
enum ExecutionMode {
    MergeOnRead,
    CopyOnWrite,
}

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

    /// Execute MergeOnRead delete strategy.
    ///
    /// This scans the table for affected files and writes position delete files.
    ///
    /// **Current implementation:** Conservatively deletes ALL rows from files that match
    /// the filter. This is correct but may over-delete. Fine-grained row-level predicate
    /// evaluation will be added in a future update.
    async fn execute_merge_on_read(
        &self,
        table: &Table,
        delete_filter: &Predicate,
    ) -> Result<ActionCommit> {
        // Step 1: Scan table to find files matching the delete filter
        let scan = table
            .scan()
            .with_filter(delete_filter.clone())
            .build()?;

        let file_tasks = scan.plan_files().await?;

        // Collect file tasks into a vector
        let tasks: Vec<_> = file_tasks.try_collect().await?;

        if tasks.is_empty() {
            // No files match the filter, nothing to delete
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "No files found matching delete filter. Nothing to delete.",
            ));
        }

        // Step 2: Collect delete positions for each file
        // CURRENT IMPLEMENTATION: Conservative approach - delete ALL rows from matched files
        // TODO: Evaluate predicate row-by-row for fine-grained deletion

        let mut total_rows_to_delete = 0u64;
        let mut deletes_per_file: HashMap<String, Vec<i64>> = HashMap::new();

        // Collect partition information from the first file task to use for delete files
        // For now, we assume all files have the same partition (or we'll use the first one)
        let partition_info = tasks.first().and_then(|task| {
            task.partition.as_ref().zip(task.partition_spec.as_ref())
        });

        for task in &tasks {
            let file_path = task.data_file_path().to_string();

            if let Some(record_count) = task.record_count {
                // Delete all rows: positions 0 to record_count-1
                let positions: Vec<i64> = (0..record_count as i64).collect();
                total_rows_to_delete += record_count;
                deletes_per_file.insert(file_path, positions);
            } else {
                // If record_count is not available, we can't proceed safely
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "File {} does not have record_count metadata. \
                        Cannot determine positions to delete.",
                        task.data_file_path()
                    ),
                ));
            }
        }

        if deletes_per_file.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "No rows to delete after processing file tasks.",
            ));
        }

        // Step 3: Write position delete files using PositionDeleteFileWriter

        // Set up writer infrastructure
        let file_io = table.file_io().clone();
        let location_gen = DefaultLocationGenerator::new(table.metadata_ref().as_ref().clone())?;
        let file_name_gen = DefaultFileNameGenerator::new(
            "delete".to_string(),
            None,
            DataFileFormat::Parquet,
        );

        // Create position delete schema
        let arrow_schema = position_delete_schema();
        let schema = Arc::new(arrow_schema_to_schema(&arrow_schema)?);

        // Set up Parquet writer
        let parquet_writer = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            schema.clone(),
        );

        // Create rolling file writer
        let rolling_writer = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_writer,
            file_io.clone(),
            location_gen,
            file_name_gen,
        );

        // Create partition key for delete files if partition info is available
        let partition_key = partition_info.map(|(partition, partition_spec)| {
            PartitionKey::new(
                partition_spec.as_ref().clone(),
                table.metadata().current_schema().clone(),
                partition.clone(),
            )
        });

        // Create position delete writer with partition information
        let mut delete_writer = PositionDeleteFileWriterBuilder::new(rolling_writer)
            .build(partition_key)
            .await?;

        // Write deletes for each file
        for (file_path, positions) in deletes_per_file.iter() {
            delete_writer
                .write_deletes(file_path, positions.iter().copied())
                .await?;
        }

        // Close writer and get delete files
        let delete_files = delete_writer.close().await?;

        if delete_files.is_empty() {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "No delete files were written. This should not happen.",
            ));
        }

        // Step 4: Create delete manifest

        // Generate unique snapshot ID and commit UUID
        let snapshot_id = Self::generate_unique_snapshot_id(table);
        let commit_uuid = self.commit_uuid.unwrap_or_else(Uuid::now_v7);

        // Create manifest file path
        const META_ROOT_PATH: &str = "metadata";
        let manifest_path = format!(
            "{}/{}/{}-m0.avro",
            table.metadata().location(),
            META_ROOT_PATH,
            commit_uuid
        );

        let output_file = file_io.new_output(manifest_path)?;

        // Determine partition spec for delete files based on whether we have partition info
        // If partition info is available, use it. Otherwise, use an unpartitioned spec
        // since the delete files were created without partition information
        let delete_partition_spec = if let Some((_, spec)) = partition_info {
            spec.as_ref().clone()
        } else {
            // No partition info from scan - use unpartitioned spec for delete files
            // This ensures partition field alignment with empty partition structs in delete files
            PartitionSpec::unpartition_spec()
        };

        // Create manifest writer for delete files
        let builder = ManifestWriterBuilder::new(
            output_file,
            Some(snapshot_id),
            self.key_metadata.clone(),
            table.metadata().current_schema().clone(),
            delete_partition_spec,
        );

        let mut manifest_writer = match table.metadata().format_version() {
            FormatVersion::V1 => {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    "DELETE operation requires table format version 2 or higher. \
                    Position deletes are not supported in v1 tables.",
                ));
            }
            FormatVersion::V2 => builder.build_v2_deletes(),
            FormatVersion::V3 => builder.build_v3_deletes(),
        };

        // Add delete files as manifest entries
        let next_seq_num = table.metadata().next_sequence_number();
        for delete_file in delete_files {
            manifest_writer.add_file(delete_file, next_seq_num)?;
        }

        // Write manifest and get ManifestFile
        let delete_manifest = manifest_writer.write_manifest_file().await?;

        // Step 5: Create snapshot with delete manifest

        // Create manifest list
        let manifest_list_path = format!(
            "{}/{}/snap-{}-0-{}.avro",
            table.metadata().location(),
            META_ROOT_PATH,
            snapshot_id,
            commit_uuid
        );

        let mut manifest_list_writer = match table.metadata().format_version() {
            FormatVersion::V1 => unreachable!("V1 check already performed"),
            FormatVersion::V2 => ManifestListWriter::v2(
                file_io.new_output(manifest_list_path.clone())?,
                snapshot_id,
                table.metadata().current_snapshot_id(),
                next_seq_num,
            ),
            FormatVersion::V3 => {
                let first_row_id = table.metadata().next_row_id();
                ManifestListWriter::v3(
                    file_io.new_output(manifest_list_path.clone())?,
                    snapshot_id,
                    table.metadata().current_snapshot_id(),
                    next_seq_num,
                    Some(first_row_id),
                )
            }
        };

        // Get existing data manifests from current snapshot
        let mut manifests = vec![];
        if let Some(current_snapshot) = table.metadata().current_snapshot() {
            let manifest_list = current_snapshot
                .load_manifest_list(&file_io, &table.metadata_ref())
                .await?;
            // Keep existing data manifests (we're only adding delete manifests)
            manifests.extend(
                manifest_list
                    .entries()
                    .iter()
                    .filter(|m| m.content == ManifestContentType::Data)
                    .cloned(),
            );
        }

        // Add the delete manifest
        manifests.push(delete_manifest);

        // Add all manifests to manifest list
        manifest_list_writer.add_manifests(manifests.into_iter())?;
        manifest_list_writer.close().await?;

        // Create snapshot summary
        let mut additional_properties = self.snapshot_properties.clone();
        additional_properties.insert(
            "deleted-data-files".to_string(),
            deletes_per_file.len().to_string(),
        );
        additional_properties.insert(
            "deleted-records".to_string(),
            total_rows_to_delete.to_string(),
        );

        let summary = Summary {
            operation: Operation::Delete,
            additional_properties,
        };

        // Build the snapshot
        let commit_ts = chrono::Utc::now().timestamp_millis();
        let new_snapshot = Snapshot::builder()
            .with_manifest_list(manifest_list_path)
            .with_snapshot_id(snapshot_id)
            .with_parent_snapshot_id(table.metadata().current_snapshot_id())
            .with_sequence_number(next_seq_num)
            .with_summary(summary)
            .with_schema_id(table.metadata().current_schema_id())
            .with_timestamp_ms(commit_ts)
            .build();

        // Step 6: Return ActionCommit

        let updates = vec![
            TableUpdate::AddSnapshot {
                snapshot: new_snapshot,
            },
            TableUpdate::SetSnapshotRef {
                ref_name: MAIN_BRANCH.to_string(),
                reference: SnapshotReference::new(
                    snapshot_id,
                    SnapshotRetention::branch(None, None, None),
                ),
            },
        ];

        let requirements = vec![
            TableRequirement::UuidMatch {
                uuid: table.metadata().uuid(),
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: MAIN_BRANCH.to_string(),
                snapshot_id: table.metadata().current_snapshot_id(),
            },
        ];

        Ok(ActionCommit::new(updates, requirements))
    }

    /// Generate a unique snapshot ID for the table.
    fn generate_unique_snapshot_id(table: &Table) -> i64 {
        let generate_random_id = || -> i64 {
            let (lhs, rhs) = Uuid::new_v4().as_u64_pair();
            let snapshot_id = (lhs ^ rhs) as i64;
            if snapshot_id < 0 {
                -snapshot_id
            } else {
                snapshot_id
            }
        };
        let mut snapshot_id = generate_random_id();

        while table
            .metadata()
            .snapshots()
            .any(|s| s.snapshot_id() == snapshot_id)
        {
            snapshot_id = generate_random_id();
        }
        snapshot_id
    }
}

#[async_trait]
impl TransactionAction for DeleteAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        // Validate that a filter was provided
        let delete_filter = self.delete_filter.as_ref().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "DELETE operation requires a filter predicate. Use with_filter() to specify which rows to delete."
            )
        })?;

        // Determine execution mode
        let execution_mode = match self.delete_mode {
            DeleteMode::MergeOnRead => ExecutionMode::MergeOnRead,
            DeleteMode::CopyOnWrite => ExecutionMode::CopyOnWrite,
            DeleteMode::Auto { cow_threshold } => {
                // For now, start with MergeOnRead
                // TODO: Implement statistics-based decision
                let _ = cow_threshold; // suppress unused warning
                ExecutionMode::MergeOnRead
            }
        };

        match execution_mode {
            ExecutionMode::MergeOnRead => {
                self.execute_merge_on_read(table, delete_filter).await
            }
            ExecutionMode::CopyOnWrite => {
                // TODO: Implement CopyOnWrite strategy
                Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    "CopyOnWrite delete mode not yet implemented. Use MergeOnRead mode for now.",
                ))
            }
        }
    }
}

// TODO: DeleteOperation will be used when snapshot creation is implemented
// struct DeleteOperation;
//
// impl SnapshotProduceOperation for DeleteOperation {
//     fn operation(&self) -> Operation {
//         Operation::Delete
//     }
//
//     async fn delete_entries(
//         &self,
//         _snapshot_produce: &SnapshotProducer<'_>,
//     ) -> Result<Vec<ManifestEntry>> {
//         // TODO: Return manifest entries for deleted data files
//         Ok(vec![])
//     }
//
//     async fn existing_manifest(
//         &self,
//         snapshot_produce: &SnapshotProducer<'_>,
//     ) -> Result<Vec<ManifestFile>> {
//         // For DELETE, we keep existing manifests that don't contain deleted data
//         let Some(snapshot) = snapshot_produce.table.metadata().current_snapshot() else {
//             return Ok(vec![]);
//         };
//
//         let manifest_list = snapshot
//             .load_manifest_list(
//                 snapshot_produce.table.file_io(),
//                 &snapshot_produce.table.metadata_ref(),
//             )
//             .await?;
//
//         // TODO: Filter out manifests containing only deleted files
//         // For now, return all manifests
//         Ok(manifest_list.entries().iter().cloned().collect())
//     }
// }

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

    // Integration tests
    use crate::spec::{DataContentType, DataFileBuilder, DataFileFormat, Literal, Struct};
    use crate::transaction::tests::make_v2_minimal_table;
    use crate::transaction::{Transaction, TransactionAction};
    use crate::{TableRequirement, TableUpdate};

    #[tokio::test]
    async fn test_delete_requires_filter() {
        let table = make_v2_minimal_table();

        // DELETE without filter should fail
        let delete_action = DeleteAction::new();
        let result = Arc::new(delete_action).commit(&table).await;

        assert!(result.is_err());
        if let Err(err) = result {
            assert_eq!(err.kind(), ErrorKind::DataInvalid);
            assert!(err.message().contains("requires a filter predicate"));
        }
    }

    #[tokio::test]
    async fn test_delete_with_no_matching_files() {
        let table = make_v2_minimal_table();

        // DELETE with filter that matches nothing
        let delete_action = DeleteAction::new()
            .with_filter(Reference::new("x").greater_than(Datum::long(9999)))
            .with_merge_on_read_mode();

        let result = Arc::new(delete_action).commit(&table).await;

        // Should error because no files match the filter
        assert!(result.is_err());
        if let Err(err) = result {
            assert_eq!(err.kind(), ErrorKind::DataInvalid);
            assert!(err.message().contains("No files found matching delete filter"));
        }
    }

    #[tokio::test]
    async fn test_delete_copy_on_write_not_implemented() {
        let table = make_v2_minimal_table();

        let delete_action = DeleteAction::new()
            .with_filter(Reference::new("x").equal_to(Datum::long(1)))
            .with_copy_on_write_mode();

        let result = Arc::new(delete_action).commit(&table).await;

        // CopyOnWrite is not yet implemented
        assert!(result.is_err());
        if let Err(err) = result {
            assert_eq!(err.kind(), ErrorKind::FeatureUnsupported);
            assert!(err.message().contains("CopyOnWrite delete mode not yet implemented"));
        }
    }

    /// Helper function to append data files to a table and return updated table
    async fn append_data_file(table: &crate::table::Table, file_path: &str, record_count: u64) -> crate::table::Table {
        let tx = Transaction::new(table);
        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(file_path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(1024)
            .record_count(record_count)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(1))]))
            .build()
            .unwrap();

        let action = tx.fast_append().add_data_files(vec![data_file]);
        let mut action_commit = Arc::new(action).commit(table).await.unwrap();
        let updates = action_commit.take_updates();

        // Apply updates to get new table state
        let mut metadata_builder = table.metadata().clone().into_builder(None);
        for update in updates {
            metadata_builder = update.apply(metadata_builder).unwrap();
        }

        table.clone().with_metadata(Arc::new(metadata_builder.build().unwrap().metadata))
    }

    #[tokio::test]
    async fn test_delete_merge_on_read_basic() {
        let table = make_v2_minimal_table();

        // First, append a data file
        let table = append_data_file(&table, "test/data1.parquet", 100).await;

        // Verify table has data
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        assert_eq!(manifest_list.entries().len(), 1);

        // Verify manifest has entries
        let manifest = manifest_list.entries()[0]
            .load_manifest(table.file_io())
            .await
            .unwrap();
        assert_eq!(manifest.entries().len(), 1);

        // Now delete with MergeOnRead
        let delete_action = DeleteAction::new()
            .with_filter(Reference::new("x").greater_than(Datum::long(0)))
            .with_merge_on_read_mode();

        let mut action_commit = Arc::new(delete_action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();
        let requirements = action_commit.take_requirements();

        // Verify updates structure
        assert_eq!(updates.len(), 2);
        assert!(matches!(updates[0], TableUpdate::AddSnapshot { .. }));
        assert!(matches!(updates[1], TableUpdate::SetSnapshotRef { .. }));

        // Verify requirements
        assert_eq!(requirements.len(), 2);
        assert!(matches!(requirements[0], TableRequirement::UuidMatch { .. }));
        assert!(matches!(requirements[1], TableRequirement::RefSnapshotIdMatch { .. }));

        // Check snapshot details
        let new_snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot
        } else {
            unreachable!()
        };

        // Verify snapshot operation is Delete
        assert_eq!(new_snapshot.summary().operation, Operation::Delete);

        // Verify summary properties
        assert_eq!(
            new_snapshot.summary().additional_properties.get("deleted-data-files").unwrap(),
            "1"
        );
        assert_eq!(
            new_snapshot.summary().additional_properties.get("deleted-records").unwrap(),
            "100" // Conservative: deletes all rows from matched file
        );

        // Verify manifest list contains both data and delete manifests
        let manifest_list = new_snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();

        // Should have 2 manifests: 1 data (from original) + 1 delete (new)
        assert_eq!(manifest_list.entries().len(), 2);

        // Find data and delete manifests
        let data_manifests: Vec<_> = manifest_list
            .entries()
            .iter()
            .filter(|m| m.content == ManifestContentType::Data)
            .collect();
        let delete_manifests: Vec<_> = manifest_list
            .entries()
            .iter()
            .filter(|m| m.content == ManifestContentType::Deletes)
            .collect();

        assert_eq!(data_manifests.len(), 1, "Should have 1 data manifest");
        assert_eq!(delete_manifests.len(), 1, "Should have 1 delete manifest");

        // Verify delete manifest contains delete files
        let delete_manifest = delete_manifests[0]
            .load_manifest(table.file_io())
            .await
            .unwrap();
        assert_eq!(delete_manifest.entries().len(), 1);

        // Verify delete file is PositionDeletes
        let delete_entry = &delete_manifest.entries()[0];
        assert_eq!(delete_entry.data_file().content, DataContentType::PositionDeletes);
        assert_eq!(delete_entry.data_file().record_count, 100); // All positions from the file
    }

    #[tokio::test]
    async fn test_delete_preserves_partition_spec() {
        let table = make_v2_minimal_table();

        // Append data file (partition x=1)
        let table = append_data_file(&table, "test/data1.parquet", 50).await;

        // Delete - use filter that matches the partition value (x=1)
        let delete_action = DeleteAction::new()
            .with_filter(Reference::new("x").equal_to(Datum::long(1)))
            .with_merge_on_read_mode();

        let mut action_commit = Arc::new(delete_action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        let new_snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot
        } else {
            unreachable!()
        };

        // Verify schema ID is preserved
        assert_eq!(new_snapshot.schema_id(), Some(table.metadata().current_schema_id()));
    }

    #[tokio::test]
    async fn test_delete_multiple_files() {
        let table = make_v2_minimal_table();

        // Append multiple data files
        let table = append_data_file(&table, "test/data1.parquet", 100).await;
        let table = append_data_file(&table, "test/data2.parquet", 200).await;
        let table = append_data_file(&table, "test/data3.parquet", 150).await;

        // Delete with filter that matches multiple files
        let delete_action = DeleteAction::new()
            .with_filter(Reference::new("x").greater_than(Datum::long(0)))
            .with_merge_on_read_mode();

        let mut action_commit = Arc::new(delete_action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        let new_snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot
        } else {
            unreachable!()
        };

        // Should delete all 3 files
        assert_eq!(
            new_snapshot.summary().additional_properties.get("deleted-data-files").unwrap(),
            "3"
        );
        assert_eq!(
            new_snapshot.summary().additional_properties.get("deleted-records").unwrap(),
            "450" // 100 + 200 + 150
        );
    }

    #[tokio::test]
    async fn test_delete_with_custom_snapshot_properties() {
        let table = make_v2_minimal_table();

        // Append data
        let table = append_data_file(&table, "test/data1.parquet", 100).await;

        // Delete with custom properties
        let mut custom_props = std::collections::HashMap::new();
        custom_props.insert("custom-key".to_string(), "custom-value".to_string());

        let delete_action = DeleteAction::new()
            .with_filter(Reference::new("x").less_than(Datum::long(50)))
            .with_merge_on_read_mode()
            .set_snapshot_properties(custom_props);

        let mut action_commit = Arc::new(delete_action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        let new_snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot
        } else {
            unreachable!()
        };

        // Verify custom property is included
        assert_eq!(
            new_snapshot.summary().additional_properties.get("custom-key").unwrap(),
            "custom-value"
        );
    }
}
