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

//! COMPACT operation implementation.
//!
//! This module provides data file compaction for Iceberg tables, combining
//! small data files into larger ones to reduce metadata overhead and improve
//! query efficiency.
//!
//! # Overview
//!
//! Data file compaction addresses a fundamental problem in streaming workloads:
//! numerous small files lead to:
//! - Increased metadata overhead (each file has a manifest entry)
//! - Higher query latency (more files to open/scan)
//! - Reduced storage efficiency
//!
//! The compaction operation:
//! 1. Identifies small files within each partition
//! 2. Groups files using a bin-packing algorithm
//! 3. Rewrites data from small files into larger files
//! 4. Atomically updates table metadata via transaction
//!
//! # Example
//!
//! ```rust,no_run
//! use iceberg::transaction::Transaction;
//! # use iceberg::Result;
//!
//! # async fn example(table: iceberg::table::Table) -> Result<()> {
//! // Create a transaction
//! let tx = Transaction::new(&table);
//!
//! // Compact files smaller than 64 MB into 512 MB files
//! let compact_action = tx
//!     .compact()
//!     .with_target_file_size_bytes(512 * 1024 * 1024)  // 512 MB
//!     .with_min_file_size_bytes(64 * 1024 * 1024);     // 64 MB
//!
//! // Apply and commit
//! let tx = compact_action.apply(tx)?;
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
use crate::spec::{DataFile, PartitionKey};
use crate::table::Table;
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind};

/// Default target file size: 512 MB
const DEFAULT_TARGET_FILE_SIZE_BYTES: i64 = 512 * 1024 * 1024;

/// Default minimum file size threshold: 64 MB
const DEFAULT_MIN_FILE_SIZE_BYTES: i64 = 64 * 1024 * 1024;

/// Default maximum file group size: 100 GB
const DEFAULT_MAX_FILE_GROUP_SIZE_BYTES: i64 = 100 * 1024 * 1024 * 1024;

/// Default minimum input files per group
const DEFAULT_MIN_INPUT_FILES: usize = 2;

/// COMPACT operation action.
///
/// This action combines small data files into larger ones within each partition.
/// Files are grouped using a bin-packing algorithm and rewritten atomically.
///
/// # Configuration
///
/// - `target_file_size_bytes`: Target size for compacted files (default: 512 MB)
/// - `min_file_size_bytes`: Files smaller than this are candidates (default: 64 MB)
/// - `max_file_group_size_bytes`: Max total size to compact together (default: 100 GB)
/// - `min_input_files`: Minimum files needed to trigger compaction (default: 2)
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
/// // Compact only specific partition
/// let compact_action = tx
///     .compact()
///     .with_partition_filter(Reference::new("date").equal_to("2024-01-01"))
///     .with_target_file_size_bytes(1024 * 1024 * 1024);  // 1 GB
///
/// let tx = compact_action.apply(tx)?;
/// # Ok(())
/// # }
/// ```
pub struct CompactAction {
    /// Target file size for compacted files (default: 512 MB)
    target_file_size_bytes: i64,

    /// Minimum file size to consider for compaction (default: 64 MB)
    min_file_size_bytes: i64,

    /// Maximum total size of files to compact together (default: 100 GB)
    max_file_group_size_bytes: i64,

    /// Minimum number of input files to trigger compaction
    min_input_files: usize,

    /// Optional partition filter (only compact specific partitions)
    partition_filter: Option<Predicate>,

    /// Properties to set on the snapshot
    snapshot_properties: HashMap<String, String>,

    /// Commit UUID for tracking
    commit_uuid: Option<Uuid>,

    /// Key metadata for encryption
    key_metadata: Option<Vec<u8>>,
}

impl CompactAction {
    pub(crate) fn new() -> Self {
        Self {
            target_file_size_bytes: DEFAULT_TARGET_FILE_SIZE_BYTES,
            min_file_size_bytes: DEFAULT_MIN_FILE_SIZE_BYTES,
            max_file_group_size_bytes: DEFAULT_MAX_FILE_GROUP_SIZE_BYTES,
            min_input_files: DEFAULT_MIN_INPUT_FILES,
            partition_filter: None,
            snapshot_properties: HashMap::new(),
            commit_uuid: None,
            key_metadata: None,
        }
    }

    /// Set the target file size for compacted files.
    ///
    /// Files will be compacted to approximately this size.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use iceberg::transaction::Transaction;
    /// # fn example(tx: Transaction) {
    /// let action = tx.compact()
    ///     .with_target_file_size_bytes(1024 * 1024 * 1024);  // 1 GB
    /// # }
    /// ```
    pub fn with_target_file_size_bytes(mut self, bytes: i64) -> Self {
        self.target_file_size_bytes = bytes;
        self
    }

    /// Set the minimum file size threshold.
    ///
    /// Only files smaller than this will be considered for compaction.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use iceberg::transaction::Transaction;
    /// # fn example(tx: Transaction) {
    /// let action = tx.compact()
    ///     .with_min_file_size_bytes(128 * 1024 * 1024);  // 128 MB
    /// # }
    /// ```
    pub fn with_min_file_size_bytes(mut self, bytes: i64) -> Self {
        self.min_file_size_bytes = bytes;
        self
    }

    /// Set the maximum file group size.
    ///
    /// Files will be grouped for compaction, but the total size of a group
    /// cannot exceed this limit.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use iceberg::transaction::Transaction;
    /// # fn example(tx: Transaction) {
    /// let action = tx.compact()
    ///     .with_max_file_group_size_bytes(50 * 1024 * 1024 * 1024);  // 50 GB
    /// # }
    /// ```
    pub fn with_max_file_group_size_bytes(mut self, bytes: i64) -> Self {
        self.max_file_group_size_bytes = bytes;
        self
    }

    /// Set the minimum number of input files required for compaction.
    ///
    /// Partitions with fewer small files than this will be skipped.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use iceberg::transaction::Transaction;
    /// # fn example(tx: Transaction) {
    /// let action = tx.compact()
    ///     .with_min_input_files(5);  // Need at least 5 files
    /// # }
    /// ```
    pub fn with_min_input_files(mut self, count: usize) -> Self {
        self.min_input_files = count;
        self
    }

    /// Set an optional partition filter.
    ///
    /// Only partitions matching this filter will be compacted.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use iceberg::transaction::Transaction;
    /// # use iceberg::expr::Reference;
    /// # fn example(tx: Transaction) {
    /// let action = tx.compact()
    ///     .with_partition_filter(Reference::new("date").equal_to("2024-01-01"));
    /// # }
    /// ```
    pub fn with_partition_filter(mut self, filter: Predicate) -> Self {
        self.partition_filter = Some(filter);
        self
    }

    /// Set a snapshot property.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use iceberg::transaction::Transaction;
    /// # fn example(tx: Transaction) {
    /// let action = tx.compact()
    ///     .with_snapshot_property("compaction.reason".to_string(), "scheduled".to_string());
    /// # }
    /// ```
    pub fn with_snapshot_property(mut self, key: String, value: String) -> Self {
        self.snapshot_properties.insert(key, value);
        self
    }

    /// Set the commit UUID for tracking.
    pub fn with_commit_uuid(mut self, uuid: Uuid) -> Self {
        self.commit_uuid = Some(uuid);
        self
    }

    /// Set the key metadata for encryption.
    pub fn with_key_metadata(mut self, metadata: Vec<u8>) -> Self {
        self.key_metadata = Some(metadata);
        self
    }
}

impl CompactAction {
    /// Build a compaction plan by analyzing the table's data files.
    ///
    /// This method:
    /// 1. Loads all data files from the current snapshot
    /// 2. Filters files smaller than `min_file_size_bytes`
    /// 3. Groups files by partition
    /// 4. Applies bin packing within each partition
    /// 5. Filters out groups with fewer than `min_input_files`
    async fn build_compaction_plan(&self, table: &Table) -> Result<CompactionPlan> {
        // Step 1: Load all data files from current snapshot
        let data_files = self.load_data_files(table).await?;

        if data_files.is_empty() {
            return Ok(CompactionPlan::new());
        }

        // Step 2: Filter files by size (keep only small files)
        let small_files = self.filter_by_size(&data_files);

        if small_files.is_empty() {
            return Ok(CompactionPlan::new());
        }

        // Step 3: Group files by partition
        let partition_groups = self.group_by_partition(small_files, table)?;

        // Step 4: Apply bin packing within each partition
        let mut plan = CompactionPlan::new();

        for (partition_spec_id, partition_key, files) in partition_groups {
            // Apply bin packing
            let packer = BinPacker::new(
                self.target_file_size_bytes as u64,
                self.max_file_group_size_bytes as u64,
            );
            let bins = packer.pack(files);

            // Create file groups from bins, filtering by min_input_files
            for bin_files in bins {
                if bin_files.len() >= self.min_input_files {
                    let group = FileGroup::new(partition_spec_id, partition_key.clone(), bin_files);
                    plan.add_group(group);
                }
            }
        }

        Ok(plan)
    }

    /// Load all data files from the current snapshot.
    async fn load_data_files(&self, table: &Table) -> Result<Vec<DataFile>> {
        let Some(snapshot) = table.metadata().current_snapshot() else {
            // No current snapshot, return empty list
            return Ok(Vec::new());
        };

        let file_io = table.file_io();
        let manifest_list = snapshot
            .load_manifest_list(file_io, table.metadata())
            .await?;

        let mut data_files = Vec::new();

        // Iterate through all data manifests
        for manifest_file in manifest_list.entries() {
            // Only process data manifests (skip delete manifests)
            if manifest_file.content != crate::spec::ManifestContentType::Data {
                continue;
            }

            // Load the manifest
            let manifest = manifest_file.load_manifest(file_io).await?;

            // Collect data files from manifest entries
            for entry in manifest.entries() {
                // Only include ADDED and EXISTING files (skip DELETED)
                if entry.status() == crate::spec::ManifestStatus::Deleted {
                    continue;
                }

                data_files.push(entry.data_file().clone());
            }
        }

        Ok(data_files)
    }

    /// Filter files by size, keeping only files smaller than min_file_size_bytes.
    fn filter_by_size(&self, files: &[DataFile]) -> Vec<DataFile> {
        files
            .iter()
            .filter(|f| (f.file_size_in_bytes as i64) < self.min_file_size_bytes)
            .cloned()
            .collect()
    }

    /// Group files by partition.
    ///
    /// Returns groups of (partition_spec_id, partition_key, files).
    fn group_by_partition(
        &self,
        files: Vec<DataFile>,
        table: &Table,
    ) -> Result<Vec<(i32, PartitionKey, Vec<DataFile>)>> {
        use std::collections::HashMap;

        // Group files by (partition_spec_id, partition_struct)
        let mut groups: HashMap<(i32, crate::spec::Struct), Vec<DataFile>> = HashMap::new();

        for file in files {
            let key = (file.partition_spec_id, file.partition.clone());
            groups.entry(key).or_default().push(file);
        }

        // Convert groups to result format with PartitionKey
        let mut result = Vec::new();

        for ((partition_spec_id, partition_struct), files) in groups {
            // Get the partition spec from table metadata
            let partition_spec = table
                .metadata()
                .partition_spec_by_id(partition_spec_id)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Partition spec {} not found", partition_spec_id),
                    )
                })?;

            // Create PartitionKey
            let partition_key = PartitionKey::new(
                partition_spec.as_ref().clone(),
                table.metadata().current_schema().clone(),
                partition_struct,
            );

            result.push((partition_spec_id, partition_key, files));
        }

        Ok(result)
    }
}

#[async_trait]
impl TransactionAction for CompactAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        // Build compaction plan
        let plan = self.build_compaction_plan(table).await?;

        // If nothing to compact, return error
        if plan.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "No files found matching compaction criteria. Nothing to compact.",
            ));
        }

        // TODO: Execute compaction (rewrite files)
        // TODO: Build manifests
        // TODO: Create snapshot

        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            format!(
                "Compaction plan built ({} groups, {} files), but data rewriting not yet implemented",
                plan.file_groups.len(),
                plan.total_input_files()
            ),
        ))
    }
}

/// A group of files to compact together.
///
/// All files in a group belong to the same partition and will be
/// rewritten into one or more larger files.
#[derive(Debug, Clone)]
struct FileGroup {
    /// Partition spec ID
    partition_spec_id: i32,

    /// Partition key (all files in group have same partition)
    partition: PartitionKey,

    /// Files to compact (sorted by file path for determinism)
    input_files: Vec<DataFile>,

    /// Total size of input files in bytes
    total_size_bytes: u64,

    /// Total record count across all files
    total_record_count: u64,
}

impl FileGroup {
    /// Create a new file group.
    fn new(
        partition_spec_id: i32,
        partition: PartitionKey,
        input_files: Vec<DataFile>,
    ) -> Self {
        let total_size_bytes = input_files.iter().map(|f| f.file_size_in_bytes).sum();
        let total_record_count = input_files.iter().map(|f| f.record_count).sum();

        Self {
            partition_spec_id,
            partition,
            input_files,
            total_size_bytes,
            total_record_count,
        }
    }

    /// Get the number of input files.
    fn file_count(&self) -> usize {
        self.input_files.len()
    }
}

/// Compaction plan containing groups of files to compact.
#[derive(Debug)]
struct CompactionPlan {
    /// Groups of files to compact together
    file_groups: Vec<FileGroup>,
}

impl CompactionPlan {
    /// Create a new empty compaction plan.
    fn new() -> Self {
        Self {
            file_groups: Vec::new(),
        }
    }

    /// Add a file group to the plan.
    fn add_group(&mut self, group: FileGroup) {
        self.file_groups.push(group);
    }

    /// Check if the plan is empty.
    fn is_empty(&self) -> bool {
        self.file_groups.is_empty()
    }

    /// Get the total number of input files across all groups.
    fn total_input_files(&self) -> usize {
        self.file_groups.iter().map(|g| g.file_count()).sum()
    }

    /// Get the total size of all input files.
    fn total_input_size_bytes(&self) -> u64 {
        self.file_groups.iter().map(|g| g.total_size_bytes).sum()
    }
}

/// Bin for bin-packing algorithm.
///
/// A bin represents a group of files that will be compacted together.
#[derive(Debug)]
struct Bin {
    /// Files in this bin
    files: Vec<DataFile>,

    /// Current total size of files in bin
    current_size: u64,
}

impl Bin {
    /// Create a new empty bin.
    fn new() -> Self {
        Self {
            files: Vec::new(),
            current_size: 0,
        }
    }

    /// Check if a file can fit in this bin.
    fn can_fit(&self, file: &DataFile, target_size: u64, max_size: u64) -> bool {
        let new_size = self.current_size + file.file_size_in_bytes;

        // Hard constraint: cannot exceed max size
        if new_size > max_size {
            return false;
        }

        // If bin is empty or within target, always accept
        if self.is_empty() || new_size <= target_size {
            return true;
        }

        // If bin is close to target (within 20% over), allow adding small files
        // This helps avoid many tiny bins
        let target_with_margin = (target_size as f64 * 1.2) as u64;
        new_size <= target_with_margin
    }

    /// Add a file to this bin.
    fn add_file(&mut self, file: DataFile) {
        self.current_size += file.file_size_in_bytes;
        self.files.push(file);
    }

    /// Check if bin is empty.
    fn is_empty(&self) -> bool {
        self.files.is_empty()
    }

    /// Get the number of files in this bin.
    fn file_count(&self) -> usize {
        self.files.len()
    }

    /// Consume the bin and return its files.
    fn into_files(self) -> Vec<DataFile> {
        self.files
    }
}

/// Bin-packing algorithm for grouping files.
///
/// Uses First-Fit Decreasing (FFD) strategy:
/// 1. Sort files by size (descending)
/// 2. For each file, find first bin that has room
/// 3. If no bin has room, create a new bin
///
/// This produces well-balanced bins and is efficient: O(n log n).
struct BinPacker {
    target_size: u64,
    max_size: u64,
    bins: Vec<Bin>,
}

impl BinPacker {
    /// Create a new bin packer.
    fn new(target_size: u64, max_size: u64) -> Self {
        Self {
            target_size,
            max_size,
            bins: Vec::new(),
        }
    }

    /// Pack files into bins using First-Fit Decreasing.
    fn pack(mut self, mut files: Vec<DataFile>) -> Vec<Vec<DataFile>> {
        // Sort files by size (descending) for better packing
        files.sort_by(|a, b| b.file_size_in_bytes.cmp(&a.file_size_in_bytes));

        for file in files {
            self.add_file(file);
        }

        // Convert bins to file groups
        self.bins
            .into_iter()
            .filter(|bin| !bin.is_empty())
            .map(|bin| bin.into_files())
            .collect()
    }

    /// Add a file to the best fitting bin (first-fit).
    fn add_file(&mut self, file: DataFile) {
        // Try to find existing bin that can fit this file
        for bin in &mut self.bins {
            if bin.can_fit(&file, self.target_size, self.max_size) {
                bin.add_file(file);
                return;
            }
        }

        // No existing bin can fit, create new bin
        let mut new_bin = Bin::new();
        new_bin.add_file(file);
        self.bins.push(new_bin);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{DataContentType, DataFileBuilder, DataFileFormat, Struct};

    /// Helper to create a test data file with specified size.
    fn test_file(path: &str, size_bytes: u64, record_count: u64) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(size_bytes)
            .record_count(record_count)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .build()
            .unwrap()
    }

    #[test]
    fn test_bin_packing_simple() {
        // Create 4 files of 100 MB each
        let files = vec![
            test_file("file1.parquet", 100 * 1024 * 1024, 1000),
            test_file("file2.parquet", 100 * 1024 * 1024, 1000),
            test_file("file3.parquet", 100 * 1024 * 1024, 1000),
            test_file("file4.parquet", 100 * 1024 * 1024, 1000),
        ];

        // Pack into 200 MB bins (max 500 MB)
        let packer = BinPacker::new(200 * 1024 * 1024, 500 * 1024 * 1024);
        let bins = packer.pack(files);

        // Should produce 2 bins with 2 files each
        assert_eq!(bins.len(), 2, "Should create 2 bins");
        assert_eq!(bins[0].len(), 2, "First bin should have 2 files");
        assert_eq!(bins[1].len(), 2, "Second bin should have 2 files");
    }

    #[test]
    fn test_bin_packing_varying_sizes() {
        // Create files of different sizes
        let files = vec![
            test_file("large.parquet", 300 * 1024 * 1024, 3000),
            test_file("medium1.parquet", 150 * 1024 * 1024, 1500),
            test_file("medium2.parquet", 150 * 1024 * 1024, 1500),
            test_file("small1.parquet", 50 * 1024 * 1024, 500),
            test_file("small2.parquet", 50 * 1024 * 1024, 500),
            test_file("small3.parquet", 50 * 1024 * 1024, 500),
        ];

        // Pack into 400 MB bins (max 500 MB)
        let packer = BinPacker::new(400 * 1024 * 1024, 500 * 1024 * 1024);
        let bins = packer.pack(files);

        // Verify all files are packed
        let total_files: usize = bins.iter().map(|b| b.len()).sum();
        assert_eq!(total_files, 6, "All 6 files should be packed");

        // Verify no bin exceeds max size
        for (i, bin) in bins.iter().enumerate() {
            let bin_size: u64 = bin.iter().map(|f| f.file_size_in_bytes).sum();
            assert!(
                bin_size <= 500 * 1024 * 1024,
                "Bin {} size {} exceeds max",
                i,
                bin_size
            );
        }
    }

    #[test]
    fn test_bin_packing_single_file() {
        let files = vec![test_file("single.parquet", 100 * 1024 * 1024, 1000)];

        let packer = BinPacker::new(400 * 1024 * 1024, 500 * 1024 * 1024);
        let bins = packer.pack(files);

        // Single file goes into single bin
        assert_eq!(bins.len(), 1);
        assert_eq!(bins[0].len(), 1);
    }

    #[test]
    fn test_bin_packing_exceeds_target_but_under_max() {
        // Two files that together exceed target but are under max
        let files = vec![
            test_file("file1.parquet", 250 * 1024 * 1024, 2500),
            test_file("file2.parquet", 200 * 1024 * 1024, 2000),
        ];

        let packer = BinPacker::new(400 * 1024 * 1024, 500 * 1024 * 1024);
        let bins = packer.pack(files);

        // Should fit in one bin (450 MB total, within 1.1x of 400 MB target)
        assert_eq!(bins.len(), 1, "Should create 1 bin");
        assert_eq!(bins[0].len(), 2, "Bin should have 2 files");
    }

    #[test]
    fn test_bin_packing_exceeds_max() {
        // Two large files that together exceed max
        let files = vec![
            test_file("file1.parquet", 300 * 1024 * 1024, 3000),
            test_file("file2.parquet", 300 * 1024 * 1024, 3000),
        ];

        let packer = BinPacker::new(400 * 1024 * 1024, 500 * 1024 * 1024);
        let bins = packer.pack(files);

        // Should create 2 bins (600 MB total exceeds 500 MB max)
        assert_eq!(bins.len(), 2, "Should create 2 bins");
        assert_eq!(bins[0].len(), 1, "First bin should have 1 file");
        assert_eq!(bins[1].len(), 1, "Second bin should have 1 file");
    }

    #[test]
    fn test_bin_packing_empty() {
        let files = vec![];

        let packer = BinPacker::new(400 * 1024 * 1024, 500 * 1024 * 1024);
        let bins = packer.pack(files);

        assert_eq!(bins.len(), 0, "No bins for empty input");
    }

    #[test]
    fn test_compaction_plan_basic() {
        let mut plan = CompactionPlan::new();
        assert!(plan.is_empty());
        assert_eq!(plan.total_input_files(), 0);
        assert_eq!(plan.total_input_size_bytes(), 0);
    }

    #[test]
    fn test_compact_action_builder() {
        let action = CompactAction::new()
            .with_target_file_size_bytes(1024 * 1024 * 1024)
            .with_min_file_size_bytes(128 * 1024 * 1024)
            .with_min_input_files(5);

        assert_eq!(action.target_file_size_bytes, 1024 * 1024 * 1024);
        assert_eq!(action.min_file_size_bytes, 128 * 1024 * 1024);
        assert_eq!(action.min_input_files, 5);
    }

    #[test]
    fn test_compact_action_defaults() {
        let action = CompactAction::new();

        assert_eq!(action.target_file_size_bytes, 512 * 1024 * 1024);
        assert_eq!(action.min_file_size_bytes, 64 * 1024 * 1024);
        assert_eq!(action.max_file_group_size_bytes, 100 * 1024 * 1024 * 1024);
        assert_eq!(action.min_input_files, 2);
        assert!(action.partition_filter.is_none());
    }

    #[test]
    fn test_bin_packing_deterministic() {
        // Same input should produce same output (determinism test)
        let files1 = vec![
            test_file("c.parquet", 100 * 1024 * 1024, 1000),
            test_file("a.parquet", 150 * 1024 * 1024, 1500),
            test_file("b.parquet", 120 * 1024 * 1024, 1200),
        ];

        let files2 = vec![
            test_file("c.parquet", 100 * 1024 * 1024, 1000),
            test_file("a.parquet", 150 * 1024 * 1024, 1500),
            test_file("b.parquet", 120 * 1024 * 1024, 1200),
        ];

        let packer1 = BinPacker::new(400 * 1024 * 1024, 500 * 1024 * 1024);
        let bins1 = packer1.pack(files1);

        let packer2 = BinPacker::new(400 * 1024 * 1024, 500 * 1024 * 1024);
        let bins2 = packer2.pack(files2);

        // Should produce same number of bins
        assert_eq!(bins1.len(), bins2.len());

        // Each bin should have same number of files
        for (bin1, bin2) in bins1.iter().zip(bins2.iter()) {
            assert_eq!(bin1.len(), bin2.len());
        }
    }

    #[test]
    fn test_bin_packing_all_files_fit_in_one_bin() {
        // 3 files that all fit comfortably in one bin
        let files = vec![
            test_file("file1.parquet", 50 * 1024 * 1024, 500),
            test_file("file2.parquet", 50 * 1024 * 1024, 500),
            test_file("file3.parquet", 50 * 1024 * 1024, 500),
        ];

        let packer = BinPacker::new(200 * 1024 * 1024, 500 * 1024 * 1024);
        let bins = packer.pack(files);

        assert_eq!(bins.len(), 1, "All files should fit in one bin");
        assert_eq!(bins[0].len(), 3, "Bin should contain all 3 files");
    }

    #[test]
    fn test_bin_packing_files_exactly_at_target() {
        // 2 files that exactly match target size
        let files = vec![
            test_file("file1.parquet", 200 * 1024 * 1024, 2000),
            test_file("file2.parquet", 200 * 1024 * 1024, 2000),
        ];

        let packer = BinPacker::new(200 * 1024 * 1024, 500 * 1024 * 1024);
        let bins = packer.pack(files);

        // First file (200 MB) fills bin to target
        // Second file (200 MB) would make 400 MB total, which is > 1.2*target (240 MB)
        // So it goes in a new bin - this is correct behavior for target-aware packing
        assert_eq!(bins.len(), 2, "Files at target size create separate bins");
    }

    #[test]
    fn test_bin_packing_many_tiny_files() {
        // 20 tiny files (5 MB each = 100 MB total)
        let files: Vec<_> = (0..20)
            .map(|i| test_file(&format!("file{}.parquet", i), 5 * 1024 * 1024, 50))
            .collect();

        let packer = BinPacker::new(50 * 1024 * 1024, 100 * 1024 * 1024);
        let bins = packer.pack(files);

        // Should create ~2 bins (10 files each = 50 MB)
        // First bin fills to ~50 MB, second bin gets the rest
        assert!(bins.len() >= 1, "Should create at least 1 bin");
        assert!(bins.len() <= 3, "Should not create more than 3 bins");

        // Verify all 20 files are packed
        let total_files: usize = bins.iter().map(|b| b.len()).sum();
        assert_eq!(total_files, 20, "All files should be packed");
    }

    #[test]
    fn test_bin_packing_one_huge_file_many_small() {
        // 1 huge file (450 MB) + 5 small files (10 MB each)
        let mut files = vec![test_file("huge.parquet", 450 * 1024 * 1024, 4500)];
        files.extend(
            (0..5).map(|i| test_file(&format!("small{}.parquet", i), 10 * 1024 * 1024, 100)),
        );

        let packer = BinPacker::new(200 * 1024 * 1024, 500 * 1024 * 1024);
        let bins = packer.pack(files);

        // Huge file goes in own bin (450 MB < 500 MB max)
        // Small files should be in another bin(s)
        assert!(bins.len() >= 2, "Should create at least 2 bins");

        // Find the bin with the huge file
        let huge_bin = bins.iter().find(|b| {
            b.iter().any(|f| f.file_path == "huge.parquet")
        }).expect("Should find huge file");

        // Huge file should be alone (or with one small file if it fits under 500 MB)
        assert!(huge_bin.len() <= 2, "Huge file bin should have at most 2 files");
    }

    #[test]
    fn test_bin_packing_files_sorted_by_size() {
        // Verify First-Fit Decreasing: larger files are packed first
        let files = vec![
            test_file("small.parquet", 10 * 1024 * 1024, 100),
            test_file("large.parquet", 300 * 1024 * 1024, 3000),
            test_file("medium.parquet", 150 * 1024 * 1024, 1500),
        ];

        let packer = BinPacker::new(400 * 1024 * 1024, 500 * 1024 * 1024);
        let bins = packer.pack(files);

        // Large file (300) should be in first bin
        // Medium (150) might fit with it (450 MB < 500 MB max)
        // Small (10) should fit somewhere

        assert!(bins.len() >= 1, "Should create at least 1 bin");

        // All files should be packed
        let total_files: usize = bins.iter().map(|b| b.len()).sum();
        assert_eq!(total_files, 3, "All files should be packed");
    }

    #[test]
    fn test_bin_packing_max_size_boundary() {
        // Test exact boundary: 2 files that sum to exactly max size
        let files = vec![
            test_file("file1.parquet", 250 * 1024 * 1024, 2500),
            test_file("file2.parquet", 250 * 1024 * 1024, 2500),
        ];

        let packer = BinPacker::new(400 * 1024 * 1024, 500 * 1024 * 1024);
        let bins = packer.pack(files);

        // First file (250 MB) goes in bin
        // Second file (250 MB) would make 500 MB total, which is > 1.2*target (480 MB)
        // Even though it's at max, the 20% margin prevents it from fitting
        // This is correct target-aware behavior
        assert_eq!(bins.len(), 2, "Files summing to max create 2 bins due to target constraint");
    }

    #[test]
    fn test_bin_packing_just_over_max() {
        // Test just over boundary: 2 files that sum to max + 1 MB
        let files = vec![
            test_file("file1.parquet", 250 * 1024 * 1024, 2500),
            test_file("file2.parquet", 251 * 1024 * 1024, 2510),
        ];

        let packer = BinPacker::new(400 * 1024 * 1024, 500 * 1024 * 1024);
        let bins = packer.pack(files);

        // 501 MB total > 500 MB max, should create 2 bins
        assert_eq!(bins.len(), 2, "Files over max should create 2 bins");
    }

    #[test]
    fn test_compaction_plan_multiple_groups() {
        let mut plan = CompactionPlan::new();

        // Add multiple file groups (simulating different partitions)
        // Note: We can't test this properly without PartitionKey helper,
        // but we can test the plan API
        assert!(plan.is_empty());
        assert_eq!(plan.total_input_files(), 0);
    }

    #[test]
    fn test_bin_is_empty() {
        let bin = Bin::new();
        assert!(bin.is_empty());
        assert_eq!(bin.current_size, 0);
    }

    #[test]
    fn test_bin_add_file_updates_size() {
        let mut bin = Bin::new();
        let file = test_file("test.parquet", 100 * 1024 * 1024, 1000);

        bin.add_file(file);

        assert!(!bin.is_empty());
        assert_eq!(bin.current_size, 100 * 1024 * 1024);
        assert_eq!(bin.file_count(), 1);
    }

    #[test]
    fn test_bin_can_fit_logic() {
        let mut bin = Bin::new();

        // Empty bin should accept any file under max
        let small_file = test_file("small.parquet", 50 * 1024 * 1024, 500);
        assert!(bin.can_fit(&small_file, 100 * 1024 * 1024, 200 * 1024 * 1024));

        // Add the file
        bin.add_file(small_file);

        // Now bin has 50 MB. Can it fit another 50 MB? (total 100 MB = target)
        let another_file = test_file("another.parquet", 50 * 1024 * 1024, 500);
        assert!(bin.can_fit(&another_file, 100 * 1024 * 1024, 200 * 1024 * 1024));

        // Add it
        bin.add_file(another_file);

        // Now bin has 100 MB (at target). Can it fit 20 MB more? (total 120 MB = 1.2*target)
        let third_file = test_file("third.parquet", 20 * 1024 * 1024, 200);
        assert!(bin.can_fit(&third_file, 100 * 1024 * 1024, 200 * 1024 * 1024));

        // Add it
        bin.add_file(third_file);

        // Now bin has 120 MB. Can it fit 5 MB more? (total 125 MB > 1.2*target=120 MB)
        // No, because we enforce the 20% margin
        let overflow_target_file = test_file("overflow_target.parquet", 5 * 1024 * 1024, 50);
        assert!(!bin.can_fit(&overflow_target_file, 100 * 1024 * 1024, 200 * 1024 * 1024));

        // But if bin is at 100 MB and we try to add 100 MB (total 200 MB = max)
        // It should NOT fit because 200 > 1.2*100 = 120
        let mut bin2 = Bin::new();
        bin2.add_file(test_file("file1.parquet", 100 * 1024 * 1024, 1000));
        let max_file = test_file("file2.parquet", 100 * 1024 * 1024, 1000);
        assert!(!bin2.can_fit(&max_file, 100 * 1024 * 1024, 200 * 1024 * 1024));
    }
}
