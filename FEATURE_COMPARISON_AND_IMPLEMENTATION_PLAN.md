# Iceberg-Rust Feature Comparison and Implementation Plan

## Executive Summary

This document provides a comprehensive comparison between iceberg-rust and Apache Iceberg Java implementation, with a detailed implementation plan for missing features. The focus is on **write operations** and **compaction/maintenance** capabilities.

**Current Status**: iceberg-rust is production-ready for **read-heavy workloads with append-only writes**. It lacks critical write operations (DELETE, UPDATE, MERGE) and table maintenance features (compaction, snapshot expiration, orphan file cleanup).

---

## 1. Feature Comparison Matrix

### ✅ Fully Implemented in iceberg-rust

| Feature Category | Feature | Rust Status | Java Status | Notes |
|-----------------|---------|-------------|-------------|-------|
| **Read Operations** | Table Scanning | ✅ | ✅ | Full support with filters and projections |
| | Snapshot Time-Travel | ✅ | ✅ | Read historical snapshots |
| | Partition Pruning | ✅ | ✅ | Query optimization |
| | Expression Filtering | ✅ | ✅ | Predicate pushdown |
| **Write Operations** | Fast Append | ✅ | ✅ | Add data files without metadata compaction |
| | Regular Append | ✅ | ✅ | Add data with metadata management |
| **File Formats** | Parquet | ✅ | ✅ | Full read/write support |
| | Avro | ✅ | ✅ | Full read/write support |
| | Puffin | ✅ | ✅ | Metadata blob storage |
| **Delete Files** | Equality Deletes | ✅ | ✅ | Reading and writing |
| | Position Deletes (Reading) | ✅ | ✅ | Basic support |
| **Catalogs** | Memory Catalog | ✅ | ✅ | |
| | REST Catalog | ✅ | ✅ | |
| | Hive MetaStore | ✅ | ✅ | |
| | AWS Glue | ✅ | ✅ | |
| | SQL Catalog | ✅ | ✅ | |
| **Storage** | S3 | ✅ | ✅ | |
| | GCS | ✅ | ✅ | |
| | Azure | 🧪 | ✅ | Experimental in Rust |
| | Local FS | ✅ | ✅ | |
| **Schema** | Schema Evolution | ✅ | ✅ | |
| | Partition Evolution | ✅ | ✅ | |
| **Partitioning** | Partition Transforms | ✅ | ✅ | Bucket, truncate, etc. |
| | Partition Writers | ✅ | ✅ | Fanout and clustered |

### ❌ Missing in iceberg-rust

| Feature Category | Feature | Rust Status | Java Status | Priority | Complexity |
|-----------------|---------|-------------|-------------|----------|------------|
| **Write Operations** | Overwrite | ❌ | ✅ | **HIGH** | Medium |
| | Replace Partitions | ❌ | ✅ | **HIGH** | Medium |
| | Delete (by filter) | ❌ | ✅ | **CRITICAL** | High |
| | Update | ❌ | ✅ | **CRITICAL** | High |
| | Merge (UPSERT) | ❌ | ✅ | **HIGH** | Very High |
| | Row Delta | ❌ | ✅ | **HIGH** | High |
| **Delete Files** | Position Delete Writer | ⚠️ | ✅ | **CRITICAL** | Medium |
| | Delete Vector Support | ⚠️ | ✅ | Medium | High |
| **Compaction** | Data File Compaction | ❌ | ✅ | **CRITICAL** | High |
| | Manifest Rewrite | ❌ | ✅ | **HIGH** | Medium |
| | Sort Order Optimization | ❌ | ✅ | Medium | High |
| **Maintenance** | Snapshot Expiration | ❌ | ✅ | **CRITICAL** | Medium |
| | Orphan File Deletion | ❌ | ✅ | **HIGH** | Medium |
| | Metadata Cleanup | ❌ | ✅ | **HIGH** | Low |
| **File Formats** | ORC | ❌ | ✅ | Low | High |
| **Statistics** | Update Statistics | ⚠️ | ✅ | Medium | Medium |
| | Update Partition Stats | ❌ | ✅ | Medium | Medium |

**Legend**: ✅ Fully Implemented | ⚠️ Partial/Basic | 🧪 Experimental | ❌ Not Implemented

---

## 2. Critical Missing Features Analysis

### 2.1 Write Operations Gap

#### Missing Operations:

1. **DELETE (by filter/predicate)**
   - **What**: Remove rows matching a filter condition
   - **Why Critical**: Required for GDPR compliance, data corrections, and data lifecycle management
   - **Java API**: `table.newDelete().deleteFromRowFilter(expression).commit()`
   - **Rust Status**: No implementation found

2. **UPDATE**
   - **What**: Modify existing rows based on conditions
   - **Why Critical**: Data corrections, status updates, enrichment workflows
   - **Java API**: Part of merge/row-delta operations
   - **Rust Status**: No implementation found

3. **MERGE (UPSERT)**
   - **What**: Conditional insert/update/delete in single operation
   - **Why Critical**: CDC workflows, incremental data loads, deduplication
   - **Java API**: Complex merge with multiple conditions
   - **Rust Status**: No implementation found

4. **OVERWRITE**
   - **What**: Replace data files while removing overwritten files
   - **Why Critical**: Batch data replacement, partition rebuilds
   - **Java API**: `table.newOverwrite()` and `table.newReplacePartitions()`
   - **Rust Status**: No transaction action found

5. **Position Delete Writer**
   - **What**: Write position delete files to mark specific rows for deletion
   - **Why Critical**: Required for DELETE and UPDATE operations
   - **Java API**: Part of `newRowDelta()` and `newDelete()`
   - **Rust Status**: TODO comment exists, basic reader present

### 2.2 Compaction and Maintenance Gap

#### Missing Maintenance Operations:

1. **Data File Compaction**
   - **What**: Combine small data files into larger ones
   - **Why Critical**: Query performance, reduce metadata overhead, cost optimization
   - **Java API**: Custom rewrite logic or `RewriteDataFilesAction`
   - **Rust Status**: `Operation::Replace` enum exists but no transaction action
   - **Impact**: Tables with streaming writes accumulate thousands of small files

2. **Manifest Rewrite**
   - **What**: Consolidate and optimize manifest files
   - **Why Critical**: Faster scan planning, reduced metadata size
   - **Java API**: `table.rewriteManifests()`
   - **Rust Status**: No implementation

3. **Snapshot Expiration**
   - **What**: Remove old snapshots and unreferenced data files
   - **Why Critical**: Disk space management, cost control
   - **Java API**: `table.expireSnapshots()`
   - **Rust Status**: No implementation
   - **Impact**: Metadata and storage grow unbounded

4. **Orphan File Deletion**
   - **What**: Clean up files left by failed writes
   - **Why Critical**: Storage cost optimization, cleanup after failures
   - **Java API**: `deleteOrphanFiles()` action
   - **Rust Status**: No implementation

5. **Metadata File Cleanup**
   - **What**: Remove old JSON metadata files
   - **Why Critical**: Reduce metadata directory size
   - **Java API**: Automatic via `write.metadata.delete-after-commit.enabled`
   - **Rust Status**: No implementation

---

## 3. Detailed Implementation Plan: Write Operations

### Priority 1: Position Delete File Writer (Foundation)

**Rationale**: Required for all DELETE, UPDATE, and MERGE operations.

#### Implementation Steps:

1. **Create `PositionDeleteFileWriter`** (`crates/iceberg/src/writer/position_delete_writer.rs`)
   ```rust
   pub struct PositionDeleteFileWriter {
       // Schema: (file_path: string, pos: long, [optional row schema fields])
       inner_writer: ParquetWriter,
       current_file_path: Option<String>,
       delete_positions: Vec<i64>,
   }
   ```

2. **Key Methods**:
   - `write_deletes(file_path: &str, positions: Vec<i64>)`: Write position deletes for a file
   - `write_deletes_with_row(file_path: &str, positions: Vec<i64>, rows: RecordBatch)`: Include deleted row data
   - `close() -> Result<Vec<DataFile>>`: Finalize and return delete files

3. **Schema Requirements**:
   - Required fields: `file_path` (string), `pos` (long)
   - Optional: Include full or partial row schema for deleted rows (spec v2+)

4. **File Naming**: Use `{partition-path}/delete-{uuid}.parquet`

5. **Statistics**: Track `delete_file_count`, `record_count`, `file_size_in_bytes`

**Estimated Effort**: 2-3 weeks (Medium complexity)

---

### Priority 2: DELETE Operation (by filter)

**Rationale**: Critical for data compliance (GDPR), corrections, and lifecycle management.

#### Implementation Steps:

1. **Create `DeleteAction`** (`crates/iceberg/src/transaction/delete.rs`)
   ```rust
   pub struct DeleteAction<'a> {
       table: &'a Table,
       delete_filter: Option<Predicate>,
       snapshot_properties: HashMap<String, String>,
   }
   ```

2. **Key API**:
   ```rust
   impl Transaction {
       pub fn delete(&mut self) -> DeleteAction {
           DeleteAction::new(self)
       }
   }

   impl DeleteAction {
       pub fn delete_from_row_filter(mut self, filter: Predicate) -> Self;
       pub fn set_snapshot_property(mut self, key: String, value: String) -> Self;
       pub async fn commit(self) -> Result<Transaction>;
   }
   ```

3. **Execution Strategy**:
   - **Option A (Lazy Delete)**: Write position delete files, don't rewrite data
     - Scan table with filter to find matching files and positions
     - Write position delete files using `PositionDeleteFileWriter`
     - Add delete files to snapshot via `SnapshotProducer`
     - Set `operation` to `Operation::Delete`

   - **Option B (Copy-on-Write Delete)**: Rewrite data files without deleted rows
     - For small deletes or partitioned tables
     - Read affected data files, filter out deleted rows
     - Write new data files, remove old ones
     - More expensive but simpler to query

4. **Decision Logic**:
   - Use Option A (position deletes) for:
     - Large tables
     - Small number of deleted rows
     - When `write.delete.mode=merge-on-read`
   - Use Option B (COW) for:
     - Small files
     - High percentage of rows deleted
     - When `write.delete.mode=copy-on-write`

5. **Transaction Integration**:
   - Create new snapshot with delete files
   - Update sequence numbers
   - Handle conflicts (concurrent writes to same files)

**Estimated Effort**: 4-5 weeks (High complexity)

---

### Priority 3: UPDATE Operation

**Rationale**: Required for data corrections and enrichment workflows.

#### Implementation Steps:

1. **Create `UpdateAction`** (`crates/iceberg/src/transaction/update.rs`)
   ```rust
   pub struct UpdateAction<'a> {
       table: &'a Table,
       update_filter: Option<Predicate>,
       assignments: Vec<(String, Expression)>, // field -> new value
   }
   ```

2. **Key API**:
   ```rust
   impl Transaction {
       pub fn update(&mut self) -> UpdateAction {
           UpdateAction::new(self)
       }
   }

   impl UpdateAction {
       pub fn update_from_row_filter(mut self, filter: Predicate) -> Self;
       pub fn set(mut self, field: &str, value: Expression) -> Self;
       pub async fn commit(self) -> Result<Transaction>;
   }
   ```

3. **Execution Strategy** (Copy-on-Write with Deletes):
   - Scan table to find files with matching rows
   - For each affected file:
     - Read data
     - Apply filter and updates
     - Separate into: updated rows vs unchanged rows
     - Write updated rows to new data file
     - Write position deletes for original updated rows OR delete entire old file
   - Add new data files and delete files to snapshot

4. **Alternative Strategy** (Pure Copy-on-Write):
   - Read entire affected data files
   - Apply updates
   - Write completely new data files
   - Remove old data files
   - Simpler but more expensive

5. **Optimization**:
   - Batch updates by file
   - Use partition pruning to limit scope
   - Consider file size thresholds for rewrite

**Estimated Effort**: 5-6 weeks (High complexity)

---

### Priority 4: OVERWRITE and REPLACE PARTITIONS

**Rationale**: Common pattern for batch data pipelines and partition rebuilds.

#### Implementation Steps:

1. **Create `OverwriteAction`** (`crates/iceberg/src/transaction/overwrite.rs`)
   ```rust
   pub struct OverwriteAction<'a> {
       table: &'a Table,
       overwrite_filter: Option<Predicate>,
       validate_conflicts: bool,
       data_files: Vec<DataFile>,
   }
   ```

2. **Key API**:
   ```rust
   impl Transaction {
       pub fn overwrite(&mut self) -> OverwriteAction;
       pub fn replace_partitions(&mut self) -> ReplacePartitionsAction;
   }

   impl OverwriteAction {
       pub fn overwrite_by_row_filter(mut self, filter: Predicate) -> Self;
       pub fn add_file(mut self, file: DataFile) -> Self;
       pub fn validate_no_conflicting_data(mut self, enable: bool) -> Self;
       pub async fn commit(self) -> Result<Transaction>;
   }
   ```

3. **Execution**:
   - Find existing data files matching the overwrite filter
   - Remove those files from snapshot
   - Add new data files
   - Validate no conflicts (if enabled): ensure no concurrent writes to same partitions

4. **Replace Partitions** (Dynamic Overwrite):
   - Automatically determine partitions from new data files
   - Remove all existing files in those partitions
   - Add new data files
   - More user-friendly than manual filter specification

**Estimated Effort**: 3-4 weeks (Medium complexity)

---

### Priority 5: MERGE Operation (Advanced)

**Rationale**: Industry-standard UPSERT operation for CDC and incremental loads.

#### Implementation Steps:

1. **Create `MergeAction`** (`crates/iceberg/src/transaction/merge.rs`)
   ```rust
   pub struct MergeAction<'a> {
       table: &'a Table,
       source_data: RecordBatch, // or Iterator<RecordBatch>
       merge_condition: Predicate,
       matched_actions: Vec<MatchedAction>,
       not_matched_actions: Vec<NotMatchedAction>,
   }

   pub enum MatchedAction {
       Update(Vec<(String, Expression)>),
       Delete,
   }

   pub enum NotMatchedAction {
       Insert,
   }
   ```

2. **Key API**:
   ```rust
   impl MergeAction {
       pub fn on(mut self, condition: Predicate) -> Self;
       pub fn when_matched_update(mut self, assignments: Vec<(String, Expression)>) -> Self;
       pub fn when_matched_delete(mut self) -> Self;
       pub fn when_not_matched_insert(mut self) -> Self;
       pub async fn commit(self) -> Result<Transaction>;
   }
   ```

3. **Execution Strategy**:
   - **Phase 1: Join and Classify**
     - Scan target table with partition/filter optimization
     - Join with source data on merge condition
     - Classify rows: matched (update/delete) vs not matched (insert)

   - **Phase 2: Apply Actions**
     - For matched rows:
       - Apply updates: write new data file + position deletes for old rows
       - Apply deletes: write position delete files
     - For not matched rows:
       - Write as new data files (append)

   - **Phase 3: Commit**
     - Add all new data files and delete files to snapshot
     - Remove overwritten files (if using COW strategy)

4. **Optimization Strategies**:
   - Use partition pruning to limit target scan
   - Batch operations by file
   - Consider hybrid COW + MOR based on update ratio
   - Spill to disk for large joins

5. **Challenges**:
   - Memory management for large source datasets
   - Efficient join implementation (hash join vs sort-merge)
   - Handling schema differences between source and target
   - Complex transaction management

**Estimated Effort**: 8-10 weeks (Very High complexity)

---

## 4. Detailed Implementation Plan: Compaction and Maintenance

### Priority 1: Data File Compaction

**Rationale**: Essential for maintaining query performance and controlling costs.

#### Implementation Steps:

1. **Create `RewriteDataFilesAction`** (`crates/iceberg/src/transaction/rewrite_data_files.rs`)
   ```rust
   pub struct RewriteDataFilesAction<'a> {
       table: &'a Table,
       target_file_size: u64,
       min_file_size: u64,
       max_file_size: u64,
       partition_filter: Option<Predicate>,
       rewrite_strategy: RewriteStrategy,
   }

   pub enum RewriteStrategy {
       BinPack,      // Combine small files up to target size
       Sort,         // Sort data while compacting
       ZOrder,       // Z-order clustering (advanced)
   }
   ```

2. **Key API**:
   ```rust
   impl Transaction {
       pub fn rewrite_data_files(&mut self) -> RewriteDataFilesAction;
   }

   impl RewriteDataFilesAction {
       pub fn target_file_size_bytes(mut self, size: u64) -> Self;
       pub fn filter_partitions(mut self, filter: Predicate) -> Self;
       pub fn strategy(mut self, strategy: RewriteStrategy) -> Self;
       pub async fn commit(self) -> Result<RewriteResult>;
   }

   pub struct RewriteResult {
       pub rewritten_data_files_count: usize,
       pub added_data_files_count: usize,
       pub rewritten_bytes: u64,
       pub added_bytes: u64,
   }
   ```

3. **Bin-Pack Strategy Implementation**:
   ```
   1. Scan table to get all data files
   2. Filter by partition if specified
   3. Group files by partition
   4. For each partition:
      a. Identify small files (< min_file_size)
      b. Bin-pack into groups targeting target_file_size
      c. For each group:
         - Read all small files
         - Concatenate RecordBatches
         - Write new consolidated file
         - Add to new_files list
      d. Track old files for removal
   5. Create snapshot with added files and removed files
   6. Set operation to Operation::Replace
   ```

4. **Sort Strategy** (Advanced):
   - Sort data by sort order while compacting
   - Improves query performance via better pruning
   - More expensive (full sort required)

5. **File Selection Logic**:
   ```rust
   fn select_files_for_compaction(
       files: Vec<DataFile>,
       min_size: u64,
       max_size: u64,
   ) -> Vec<Vec<DataFile>> {
       // Group small files into bins
       // Skip files already near target size
       // Return groups to rewrite
   }
   ```

6. **Configuration**:
   - `write.target-file-size-bytes`: Default target (128MB-512MB)
   - `write.parquet.row-group-size-bytes`: Parquet-specific
   - `write.distribution-mode`: Partition distribution

**Estimated Effort**: 5-6 weeks (High complexity)

---

### Priority 2: Snapshot Expiration

**Rationale**: Critical for preventing unbounded storage growth.

#### Implementation Steps:

1. **Create `ExpireSnapshotsAction`** (`crates/iceberg/src/transaction/expire_snapshots.rs`)
   ```rust
   pub struct ExpireSnapshotsAction<'a> {
       table: &'a Table,
       expire_older_than: Option<i64>, // timestamp
       retain_last: usize, // minimum snapshots to keep
       max_snapshot_age_ms: Option<i64>,
       snapshot_ids_to_expire: Vec<i64>,
   }
   ```

2. **Key API**:
   ```rust
   impl Table {
       pub fn expire_snapshots(&self) -> ExpireSnapshotsAction;
   }

   impl ExpireSnapshotsAction {
       pub fn expire_older_than(mut self, timestamp_ms: i64) -> Self;
       pub fn retain_last(mut self, count: usize) -> Self;
       pub fn expire_snapshot_id(mut self, id: i64) -> Self;
       pub async fn commit(self) -> Result<ExpireResult>;
   }

   pub struct ExpireResult {
       pub deleted_data_files: usize,
       pub deleted_manifest_files: usize,
       pub deleted_manifest_lists: usize,
   }
   ```

3. **Execution Logic**:
   ```
   1. Load all snapshots from table metadata
   2. Determine snapshots to expire based on criteria:
      - Age (expire_older_than)
      - Count (keep last N via retain_last)
      - Explicit IDs (snapshot_ids_to_expire)
   3. Build set of snapshots to retain (never expire current + branches + tags)
   4. For each snapshot to expire:
      a. Find all manifest lists
      b. Find all manifests
      c. Find all data files
      d. Check if files are referenced by retained snapshots
      e. Delete unreferenced files
   5. Update table metadata to remove expired snapshots
   6. Delete old metadata.json files (optional)
   ```

4. **Safety Checks**:
   - Never expire current snapshot
   - Never expire snapshots referenced by branches/tags
   - Respect `retain_last` minimum
   - Confirm no concurrent readers (best effort)

5. **Deletion Strategy**:
   - Build reference counting: which files are used by which snapshots
   - Only delete files with zero references after expiration
   - Use async deletion for large file counts

6. **Configuration**:
   - `snapshot.expire.min-snapshots-to-keep`: Default 1
   - `snapshot.expire.max-snapshot-age-ms`: Default 5 days
   - `snapshot.gc.enabled`: Enable automatic expiration

**Estimated Effort**: 4-5 weeks (Medium-High complexity)

---

### Priority 3: Manifest Rewrite

**Rationale**: Improves scan planning performance.

#### Implementation Steps:

1. **Create `RewriteManifestsAction`** (`crates/iceberg/src/transaction/rewrite_manifests.rs`)
   ```rust
   pub struct RewriteManifestsAction<'a> {
       table: &'a Table,
       use_caching: bool,
       target_manifest_size_bytes: u64,
       min_count_to_merge: usize,
   }
   ```

2. **Key API**:
   ```rust
   impl Table {
       pub fn rewrite_manifests(&self) -> RewriteManifestsAction;
   }

   impl RewriteManifestsAction {
       pub fn cluster_by(mut self, columns: Vec<String>) -> Self;
       pub async fn commit(self) -> Result<RewriteResult>;
   }
   ```

3. **Execution**:
   ```
   1. Read current snapshot manifest list
   2. Group manifests by partition spec
   3. For each group:
      - If manifests are small or fragmented:
        a. Read all manifest entries
        b. Cluster/sort by specified columns
        c. Write new consolidated manifests
      - Track old manifests for removal
   4. Write new manifest list
   5. Create snapshot pointing to new manifest list
   ```

4. **Clustering Strategy**:
   - Group by file path prefix (partition values)
   - Improves partition pruning during scans
   - Reduces manifest read count for queries

**Estimated Effort**: 3-4 weeks (Medium complexity)

---

### Priority 4: Orphan File Deletion

**Rationale**: Clean up storage after write failures.

#### Implementation Steps:

1. **Create `DeleteOrphanFilesAction`** (`crates/iceberg/src/maintenance/delete_orphan_files.rs`)
   ```rust
   pub struct DeleteOrphanFilesAction<'a> {
       table: &'a Table,
       location: String,
       older_than: i64, // timestamp
       dry_run: bool,
   }
   ```

2. **Key API**:
   ```rust
   impl Table {
       pub fn delete_orphan_files(&self) -> DeleteOrphanFilesAction;
   }

   impl DeleteOrphanFilesAction {
       pub fn location(mut self, path: String) -> Self;
       pub fn older_than(mut self, timestamp_ms: i64) -> Self;
       pub fn dry_run(mut self, enable: bool) -> Self;
       pub async fn execute(self) -> Result<Vec<String>>;
   }
   ```

3. **Execution**:
   ```
   1. List all files in table location (recursively)
   2. Load current table metadata
   3. Build set of referenced files:
      - All data files from all live snapshots
      - All manifest files
      - All manifest lists
      - All metadata files
      - All statistics files
   4. For each file in location:
      - If not in referenced set AND older than threshold:
        a. Mark as orphan
        b. Delete (if not dry_run)
   5. Return list of deleted files
   ```

4. **Safety**:
   - Default `older_than` should be 3+ days
   - Never delete files newer than threshold (may be in-progress writes)
   - Respect table metadata retention settings

**Estimated Effort**: 2-3 weeks (Medium complexity)

---

### Priority 5: Metadata Cleanup

**Rationale**: Prevent metadata directory bloat.

#### Implementation Steps:

1. **Create automatic cleanup in commit path**:
   ```rust
   // In SnapshotProducer or table update logic
   fn cleanup_metadata_files(
       file_io: &FileIO,
       location: &str,
       max_versions: usize,
   ) -> Result<()> {
       // List metadata files
       // Sort by version
       // Delete oldest beyond max_versions
   }
   ```

2. **Configuration**:
   - `write.metadata.previous-versions-max`: Default 100
   - `write.metadata.delete-after-commit.enabled`: Default false

3. **Execution**: Automatically run after successful commit

**Estimated Effort**: 1 week (Low complexity)

---

## 5. Implementation Priorities and Phases

### Phase 1: Foundation (12-16 weeks)
**Goal**: Enable basic write and maintenance operations

1. **Position Delete File Writer** (2-3 weeks)
2. **DELETE Operation** (4-5 weeks)
3. **OVERWRITE Operation** (3-4 weeks)
4. **Snapshot Expiration** (4-5 weeks)

**Deliverable**: Users can delete data, overwrite partitions, and clean up old snapshots

---

### Phase 2: Optimization (10-14 weeks)
**Goal**: Table maintenance and performance optimization

5. **Data File Compaction** (5-6 weeks)
6. **Manifest Rewrite** (3-4 weeks)
7. **Orphan File Deletion** (2-3 weeks)
8. **Metadata Cleanup** (1 week)

**Deliverable**: Full table maintenance capabilities

---

### Phase 3: Advanced Write Operations (11-16 weeks)
**Goal**: Complex data modification patterns

9. **UPDATE Operation** (5-6 weeks)
10. **MERGE Operation** (8-10 weeks)

**Deliverable**: Industry-standard CDC and UPSERT workflows

---

### Phase 4: Advanced Features (Optional)
**Goal**: Performance and advanced optimizations

11. **Sort Compaction** (3-4 weeks)
12. **Z-Order Clustering** (4-5 weeks)
13. **Statistics Updates** (2-3 weeks)
14. **ORC Format Support** (6-8 weeks)

---

## 6. Architecture Recommendations

### 6.1 Transaction Module Restructuring

**Current Structure**:
```
transaction/
├── mod.rs
├── fast_append.rs
├── update_table_properties.rs
└── ...
```

**Recommended Structure**:
```
transaction/
├── mod.rs
├── write/
│   ├── append.rs
│   ├── delete.rs
│   ├── update.rs
│   ├── overwrite.rs
│   └── merge.rs
├── maintenance/
│   ├── rewrite_data_files.rs
│   ├── rewrite_manifests.rs
│   ├── expire_snapshots.rs
│   └── orphan_files.rs
└── metadata/
    ├── update_properties.rs
    ├── update_schema.rs
    └── ...
```

### 6.2 Writer Module Enhancement

**Add position delete writer**:
```
writer/
├── data_file_writer.rs
├── equality_delete_writer.rs
├── position_delete_writer.rs  // NEW
└── ...
```

### 6.3 Execution Strategy Framework

**Create pluggable execution strategies**:
```rust
pub trait ExecutionStrategy {
    async fn execute(&self, context: &ExecutionContext) -> Result<ExecutionResult>;
}

pub struct CopyOnWriteStrategy;
pub struct MergeOnReadStrategy;
pub struct HybridStrategy;
```

### 6.4 Configuration Management

**Centralize write and compaction configs**:
```rust
pub struct WriteConfig {
    pub mode: WriteMode, // COW vs MOR
    pub target_file_size_bytes: u64,
    pub delete_mode: DeleteMode,
    pub distribution_mode: DistributionMode,
}

pub struct CompactionConfig {
    pub strategy: CompactionStrategy,
    pub target_file_size_bytes: u64,
    pub min_file_size_bytes: u64,
    pub max_file_size_bytes: u64,
}
```

---

## 7. Testing Strategy

### 7.1 Unit Tests
- Test each writer independently (position delete, data file)
- Test file selection logic for compaction
- Test snapshot expiration reference counting
- Test merge join logic

### 7.2 Integration Tests
- End-to-end DELETE, UPDATE, MERGE operations
- Concurrent write conflict handling
- Compaction on large tables (1000+ files)
- Snapshot expiration with complex snapshot graphs

### 7.3 Compatibility Tests
- Verify Rust-written tables readable by Java/Spark
- Verify Rust can read Java-written delete files
- Cross-implementation compaction verification

### 7.4 Performance Benchmarks
- Compaction throughput (files/sec, GB/sec)
- DELETE operation latency vs data volume
- Memory usage for large MERGE operations
- Snapshot expiration time on 10k+ snapshot tables

---

## 8. Risk Assessment

### High Risk
- **MERGE complexity**: Complex join logic, memory management, schema handling
- **Concurrent writes**: Conflict detection and resolution
- **Data correctness**: DELETE/UPDATE must never lose or corrupt data

### Medium Risk
- **Performance**: Compaction must scale to TB-scale tables
- **Memory usage**: Large operations may OOM without careful streaming
- **Compatibility**: Must match Java behavior exactly

### Low Risk
- **Snapshot expiration**: Well-defined algorithm
- **Manifest rewrite**: Straightforward reorganization
- **Metadata cleanup**: Simple file deletion

---

## 9. Success Metrics

### Feature Parity
- ✅ All critical write operations implemented (DELETE, UPDATE, OVERWRITE)
- ✅ All critical maintenance operations implemented (compaction, expiration)
- ✅ Cross-implementation compatibility verified

### Performance
- ✅ Compaction processes ≥1GB/sec on modern hardware
- ✅ DELETE operation latency ≤2x append latency
- ✅ Snapshot expiration completes in <10 minutes for 10k snapshots

### Adoption
- ✅ Used in production for full read-write workloads
- ✅ Integration with query engines (DataFusion, etc.)
- ✅ Positive community feedback

---

## 10. Dependencies and Blockers

### External Dependencies
- None (all features can be implemented with existing Rust ecosystem)

### Internal Dependencies
- **Arrow**: Already integrated, sufficient
- **Parquet**: Already integrated, sufficient
- **Object Store**: Already integrated, sufficient

### Potential Blockers
- **API Design**: Requires community consensus on Transaction API shape
- **Testing Infrastructure**: May need test data generators for large-scale tests
- **Documentation**: Each feature needs spec documentation and examples

---

## 11. Recommended Next Steps

1. **Gather Community Feedback** (1-2 weeks)
   - Share this plan with iceberg-rust maintainers
   - Discuss API design preferences
   - Prioritize features based on user demand

2. **Create RFCs** (2-3 weeks)
   - RFC for Write Operations API
   - RFC for Maintenance Operations API
   - RFC for Transaction API enhancements

3. **Implement Phase 1** (12-16 weeks)
   - Position Delete Writer
   - DELETE Operation
   - OVERWRITE Operation
   - Snapshot Expiration

4. **Release and Gather Feedback** (2-4 weeks)
   - Beta release with Phase 1 features
   - Gather production feedback
   - Fix critical issues

5. **Continue with Phase 2-4** (6-12 months)
   - Iterative implementation
   - Continuous testing and refinement
   - Community engagement

---

## 12. Conclusion

Iceberg-rust has a **solid foundation** for read operations and basic append-only writes. However, it lacks **critical write operations** (DELETE, UPDATE, MERGE) and **essential maintenance capabilities** (compaction, snapshot expiration).

Implementing these features will:
- ✅ Enable production read-write workloads
- ✅ Support regulatory compliance (GDPR deletes)
- ✅ Prevent storage bloat and cost overruns
- ✅ Match feature parity with Java implementation
- ✅ Establish Rust as viable alternative for Iceberg workloads

**Total Estimated Timeline**: 12-18 months for full feature parity
**Minimum Viable Product**: Phase 1 (12-16 weeks) for critical operations

This is an ambitious but achievable roadmap that will significantly enhance iceberg-rust's production readiness.
