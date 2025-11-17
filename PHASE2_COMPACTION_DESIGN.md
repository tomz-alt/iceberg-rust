# Phase 2: Data File Compaction - Design Document

**Date:** November 15, 2025
**Status:** Design Phase
**Spec Reference:** [Compact Data Files](https://iceberg.apache.org/docs/latest/maintenance/#compact-data-files)

---

## Overview

Data file compaction is a critical maintenance operation that combines small data files into larger ones, reducing metadata overhead and improving query efficiency. This is especially important for streaming workloads that produce numerous small files.

### Problem Statement

**From Iceberg Spec:**
> "More data files leads to more metadata stored in manifest files, and small data files causes an unnecessary amount of metadata and less efficient queries from file open costs."

**Impact of Small Files:**
- Increased metadata overhead (each file has a manifest entry)
- Higher query latency (more files to open/scan)
- Reduced storage efficiency
- Slower table operations (snapshot management, etc.)

---

## Spec-Compliant Design

### Core Requirements (from Iceberg Spec)

1. **File Identification:** Identify small files that need compaction
2. **Bin Packing:** Group files into compaction groups (within same partition)
3. **Data Rewriting:** Read data from small files, write to larger files
4. **Atomic Operation:** Use transactions to ensure all-or-nothing commits
5. **Metadata Updates:** Update manifests and snapshots correctly
6. **Partition Preservation:** Files must stay in their original partitions

### Configuration Parameters

Based on Iceberg's `rewrite_data_files` procedure:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `target_file_size_bytes` | i64 | 512 MB | Target size for compacted files |
| `min_file_size_bytes` | i64 | 64 MB | Files smaller than this are candidates |
| `max_file_group_size_bytes` | i64 | 100 GB | Max total size of files to compact together |
| `min_input_files` | usize | 2 | Minimum files needed to trigger compaction |
| `partial_progress_enabled` | bool | false | Commit partial results if some groups succeed |
| `partial_progress_max_commits` | usize | 10 | Max partial commits before final commit |

---

## Implementation Architecture

### 1. Module Structure

```
crates/iceberg/src/transaction/compact.rs  (new)
```

Following the pattern of `delete.rs`, `overwrite.rs`, etc.

### 2. Key Components

#### A. `CompactAction` Struct

```rust
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
}
```

#### B. `CompactionPlan` Struct

```rust
struct CompactionPlan {
    /// Groups of files to compact together
    file_groups: Vec<FileGroup>,
}

struct FileGroup {
    /// Partition spec ID
    partition_spec_id: i32,

    /// Partition key (all files in group have same partition)
    partition: PartitionKey,

    /// Files to compact (sorted by file path for determinism)
    input_files: Vec<DataFile>,

    /// Total size of input files
    total_size_bytes: i64,

    /// Total record count
    total_record_count: u64,
}
```

### 3. Algorithm: Bin Packing Strategy

**Goal:** Group files into bins where each bin produces one compacted file

**Strategy: First-Fit Decreasing (FFD)**

```
1. Load all data files from current snapshot
2. Filter files by size (keep only files < min_file_size_bytes)
3. Group files by partition (partition_spec_id + partition_key)
4. For each partition group:
   a. Sort files by size (descending)
   b. Create bins:
      - Each bin targets target_file_size_bytes
      - Bin cannot exceed max_file_group_size_bytes
      - Use first-fit: add file to first bin that has room
   c. Only keep bins with >= min_input_files
5. Return compaction plan with all bins
```

**Why First-Fit Decreasing?**
- Simple and efficient: O(n log n)
- Good approximation ratio (~11/9 OPT)
- Produces balanced bins
- Deterministic (same input → same output)

### 4. Data Rewriting Logic

**For each FileGroup:**

```
1. Create compacted file writer:
   - Use RollingFileWriterBuilder
   - Target size: target_file_size_bytes
   - Same partition spec as input files
   - Same schema as table

2. For each input file in group:
   a. Open file reader (Parquet)
   b. Read all rows
   c. Write rows to compacted file writer
   d. Track metrics (record count, column bounds, etc.)

3. Close writer, get compacted DataFile(s)
   - May produce multiple files if total size exceeds target

4. Update manifests:
   - Mark input files as DELETED
   - Mark output files as ADDED

5. Create new snapshot with updated manifests
```

### 5. Transaction Flow

**Following the pattern from `delete.rs` and `overwrite.rs`:**

```rust
impl TransactionAction for CompactAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        // 1. Build compaction plan
        let plan = self.build_compaction_plan(table).await?;

        // 2. Execute compaction (rewrite files)
        let rewrite_results = self.execute_compaction(table, plan).await?;

        // 3. Build manifest updates
        let manifest_file = self.build_manifests(table, rewrite_results).await?;

        // 4. Create new snapshot
        let snapshot = self.build_snapshot(table, manifest_file).await?;

        // 5. Return TableUpdates and TableRequirements
        Ok(ActionCommit::new(
            vec![
                TableUpdate::AddSnapshot { snapshot },
                TableUpdate::SetSnapshotRef { /* ... */ },
            ],
            vec![
                TableRequirement::RefSnapshotIdMatch { /* ... */ },
            ],
        ))
    }
}
```

---

## Implementation Phases

### Phase 1: Core Infrastructure (Week 17-18)

**Tasks:**
1. Create `compact.rs` module with `CompactAction` struct
2. Implement `build_compaction_plan()`:
   - File filtering by size
   - Partition grouping
   - Bin packing algorithm
3. Write unit tests for bin packing logic
4. Add `compact()` method to `Transaction`

**Success Criteria:**
- Compaction plan correctly identifies files to compact
- Bin packing produces valid groups
- Unit tests pass (bin packing correctness)

### Phase 2: Data Rewriting (Week 19-20)

**Tasks:**
1. Implement `execute_compaction()`:
   - File reading (Parquet)
   - File writing (RollingFileWriter)
   - Schema handling
   - Metrics tracking
2. Implement `build_manifests()`:
   - Create manifest entries for deleted files
   - Create manifest entries for added files
   - Write manifest files
3. Implement `build_snapshot()`:
   - Snapshot summary with stats
   - Proper sequence numbers
   - Manifest list creation

**Success Criteria:**
- Can read and rewrite data files
- Manifests correctly track file changes
- Snapshots are valid

### Phase 3: Testing & Validation (Week 21-22)

**Tasks:**
1. Write unit tests:
   - File filtering
   - Partition grouping
   - Edge cases (empty table, single file, etc.)
2. Write integration tests:
   - Compact small files, verify row counts
   - Java compatibility test (create files in Rust, compact, read in Java)
   - Round-trip test (Java compact, Rust read)
3. Performance benchmarks:
   - Measure compaction throughput
   - Compare file sizes before/after
   - Measure query performance improvement

**Success Criteria:**
- All unit tests pass
- Java compatibility validated
- Performance meets expectations

---

## Testing Strategy

### Unit Tests

```rust
#[test]
fn test_bin_packing_simple() {
    // Test basic bin packing with 3 small files
}

#[test]
fn test_partition_grouping() {
    // Ensure files from different partitions aren't grouped
}

#[test]
fn test_min_file_threshold() {
    // Files >= min_file_size_bytes should be excluded
}

#[test]
fn test_min_input_files() {
    // Groups with < min_input_files should be excluded
}
```

### Integration Tests

```rust
#[tokio::test]
async fn test_compact_small_files() {
    // 1. Create table with 10 small files (5 MB each)
    // 2. Run compaction with target_file_size = 25 MB
    // 3. Verify: 2 files remain (25 MB each)
    // 4. Verify: Row count matches (no data loss)
    // 5. Verify: Can query compacted data
}

#[tokio::test]
async fn test_java_compat_compact() {
    // 1. Create table in Java with small files
    // 2. Compact in Rust
    // 3. Read in Java, verify correctness
}
```

### Performance Benchmarks

```rust
#[bench]
fn bench_compact_1000_files() {
    // Measure time to compact 1000 files (100 MB each) → 100 files (1 GB each)
    // Target: < 30 seconds for 100 GB
}
```

---

## Edge Cases & Error Handling

### Edge Cases

1. **Empty table:** Return early, no compaction needed
2. **All files large:** No files match criteria, return early
3. **Single small file:** Cannot compact (min_input_files = 2)
4. **Partition with one file:** Skip partition
5. **Files larger than target:** Leave as-is
6. **Schema evolution:** Handle files with different schema versions

### Error Conditions

| Error | Handling |
|-------|----------|
| Read failure (corrupted file) | Fail entire compaction, rollback |
| Write failure (disk full) | Fail entire compaction, rollback |
| Commit conflict | Retry with exponential backoff |
| Invalid partition spec | Fail with clear error message |

### Transaction Safety

- **Atomicity:** All-or-nothing via transaction commit
- **Isolation:** Read snapshot at start, commit only if no conflicts
- **Durability:** Files written before metadata commit
- **Consistency:** Manifests always reference valid files

---

## Success Metrics

### Functional Metrics

- ✅ Compacts files within same partition
- ✅ Produces files close to target size
- ✅ Preserves all data (row count match)
- ✅ Updates manifests correctly
- ✅ Creates valid snapshots
- ✅ 100% Java compatible

### Performance Metrics

- **Throughput:** >= 500 MB/s compaction speed
- **Efficiency:** Produces files 90-110% of target size
- **Overhead:** Metadata overhead < 1% of data size

### Quality Metrics

- **Test Coverage:** >= 90%
- **Edge Cases:** All edge cases handled
- **Error Handling:** Clear error messages
- **Documentation:** All public APIs documented

---

## Spec Compliance Checklist

- [ ] Reads files from current snapshot
- [ ] Filters files by size
- [ ] Groups files by partition
- [ ] Rewrites data to larger files
- [ ] Updates manifests (delete old, add new)
- [ ] Creates new snapshot atomically
- [ ] Handles partition specs correctly
- [ ] Preserves schema evolution
- [ ] Tracks sequence numbers
- [ ] Compatible with Java Iceberg
- [ ] Handles concurrent commits gracefully

---

## Dependencies

### Existing Infrastructure (Already Available)

- ✅ `ManifestWriter` - for writing manifests
- ✅ `ManifestListWriter` - for writing manifest lists
- ✅ `Snapshot` - snapshot creation
- ✅ `Transaction` - transaction framework
- ✅ `RollingFileWriter` - for writing Parquet files
- ✅ `ParquetWriter` - Parquet file writing
- ✅ `FileIO` - file I/O operations
- ✅ `PartitionSpec` - partition handling

### New Dependencies Needed

- **None** - Can build on existing infrastructure!

---

## Open Questions

1. **Sorting within compacted files:**
   - Should we sort data by a column (z-order)?
   - Start simple: preserve input order
   - Add sorting in future enhancement

2. **Parallel compaction:**
   - Compact multiple partitions in parallel?
   - Start simple: sequential
   - Add parallelism in optimization phase

3. **Partial progress commits:**
   - Commit after each partition or all at once?
   - Start simple: all-or-nothing
   - Add partial progress in future

4. **Delete file handling:**
   - Compact position delete files too?
   - Start simple: data files only
   - Add delete file compaction later

---

## Implementation Timeline

| Week | Phase | Deliverables |
|------|-------|--------------|
| 17-18 | Design & Planning | Design doc ✅, Bin packing impl, Unit tests |
| 19-20 | Core Implementation | Data rewriting, Manifest updates, Transaction integration |
| 21-22 | Testing & Optimization | Integration tests, Java compat, Benchmarks |

**Total Duration:** 6 weeks
**Next Steps:** Begin Phase 1 implementation

---

## Conclusion

This design follows the official Apache Iceberg specification for data file compaction and leverages our existing transaction infrastructure. The implementation will be:

- ✅ **Spec-compliant:** Follows Iceberg maintenance spec
- ✅ **Java-compatible:** Works with Java-created tables
- ✅ **Production-ready:** Atomic, safe, well-tested
- ✅ **Extensible:** Room for future optimizations

**Ready to begin implementation!** 🚀
