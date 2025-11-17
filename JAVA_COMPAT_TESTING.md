# Java Compatibility Testing Plan

## Objective
Validate that our Rust Iceberg implementation is 100% compatible with Apache Iceberg's Java implementation.

## Test Strategy

### 1. Bidirectional Table Operations

#### A. Java → Rust (Read Compatibility)
- **Goal**: Verify our Rust code can read and work with tables created by Java Iceberg
- **Test Cases**:
  1. Read basic table with data files
  2. Read table with position delete files
  3. Read table with equality delete files
  4. Read table with multiple snapshots
  5. Read partitioned tables
  6. Read tables with schema evolution
  7. Read tables after Java DELETE operation
  8. Read tables after Java OVERWRITE operation
  9. Read tables after Java EXPIRE SNAPSHOTS

#### B. Rust → Java (Write Compatibility)
- **Goal**: Verify tables created/modified by our Rust code work with Java Iceberg
- **Test Cases**:
  1. Create table in Rust, read in Java
  2. Write data files in Rust, read in Java
  3. Write position deletes in Rust, read in Java
  4. Perform DELETE in Rust, verify in Java
  5. Perform OVERWRITE in Rust, verify in Java
  6. Expire snapshots in Rust, verify in Java

### 2. Metadata Compatibility

#### Test Areas:
- **Table Metadata JSON**: Ensure identical structure
- **Manifest Lists**: Binary-compatible format
- **Manifests**: Binary-compatible format
- **Snapshot References**: Proper snapshot tracking
- **Partition Specs**: Correct partition handling
- **Schema Evolution**: Compatible schema updates

### 3. Data File Compatibility

#### Test Areas:
- **Parquet Data Files**: Same encoding/compression
- **Position Delete Files**: Binary-compatible format
- **File Statistics**: Correct min/max/null counts
- **Partition Values**: Proper partition encoding

## Test Environment Setup

### Prerequisites
```bash
# Java Environment
- Java 17+
- Apache Iceberg Java library (latest stable)
- Spark 3.5+ with Iceberg runtime

# Rust Environment
- Our iceberg-rust implementation
- Test data generators
```

### Directory Structure
```
tests/
  java_compat/
    data/                    # Test data
      java_created/          # Tables created by Java
      rust_created/          # Tables created by Rust
    scripts/
      java/                  # Java test scripts
        CreateTestTables.java
        ValidateRustTables.java
      rust/                  # Rust test programs
        read_java_tables.rs
        create_test_tables.rs
    results/                 # Test results
```

## Implementation Phases

### Phase 1: Setup (Week 17, Days 1-2)
- [ ] Create Java test harness
- [ ] Set up Spark with Iceberg
- [ ] Create test data directory structure
- [ ] Write helper utilities

### Phase 2: Java → Rust Tests (Week 17, Days 3-5)
- [ ] Generate test tables with Java
- [ ] Read and validate with Rust
- [ ] Test all data types
- [ ] Test delete files
- [ ] Test snapshot operations

### Phase 3: Rust → Java Tests (Week 18, Days 1-3)
- [ ] Create test tables with Rust
- [ ] Read and validate with Java/Spark
- [ ] Test DELETE operation results
- [ ] Test OVERWRITE operation results
- [ ] Test EXPIRE SNAPSHOTS results

### Phase 4: Edge Cases (Week 18, Days 4-5)
- [ ] Large tables (millions of rows)
- [ ] Many snapshots
- [ ] Complex partitioning
- [ ] Schema evolution scenarios
- [ ] Concurrent modifications

## Success Criteria

### ✅ Must Pass
1. All Java-created tables readable by Rust
2. All Rust-created tables readable by Java/Spark
3. DELETE operations produce identical results
4. OVERWRITE operations produce identical results
5. EXPIRE SNAPSHOTS behaves identically
6. Metadata files are byte-for-byte compatible (where order doesn't matter)
7. Data files use compatible Parquet encoding

### 📊 Nice to Have
1. Performance benchmarks vs Java
2. Memory usage comparisons
3. Compatibility with older Iceberg versions

## Test Scenarios

### Scenario 1: Basic Table Lifecycle
```
Java: Create table → Insert data → Create snapshot
Rust: Read table → Validate data → Validate metadata
```

### Scenario 2: DELETE Operation
```
Java: Create table with data
Rust: Perform DELETE
Java: Read and validate results match Java DELETE
```

### Scenario 3: OVERWRITE Operation
```
Java: Create partitioned table
Rust: Perform OVERWRITE
Java: Validate partition replacement
```

### Scenario 4: Snapshot Management
```
Rust: Create table with multiple snapshots
Rust: EXPIRE old snapshots
Java: Validate snapshot history
```

### Scenario 5: Full Round Trip
```
Java: Create table
Rust: INSERT data
Java: DELETE rows
Rust: OVERWRITE partition
Java: EXPIRE snapshots
Rust: Validate final state
```

## Validation Metrics

### Data Correctness
- Row count matches
- Column values match
- Null handling identical
- Partition pruning works

### Metadata Correctness
- Snapshot IDs match expected
- Manifest lists identical
- File statistics match
- Schema compatibility

### Operation Correctness
- DELETE removes same rows
- OVERWRITE replaces same files
- EXPIRE removes same snapshots

## Risk Mitigation

### Known Challenges
1. **Timestamp precision**: Java vs Rust time handling
2. **Floating point precision**: Double comparisons
3. **UUID generation**: Different random number generators
4. **File ordering**: Non-deterministic manifest ordering

### Mitigation Strategies
- Use deterministic test data
- Allow for acceptable floating point tolerance
- Focus on logical equivalence, not byte-for-byte matching
- Sort manifests for comparison

## Reporting

### Test Report Format
```markdown
## Test: {Test Name}
**Status**: ✅ PASS / ❌ FAIL / ⚠️ WARNING

### Setup
- Java version: {version}
- Iceberg version: {version}
- Rust version: {version}

### Results
- Rows expected: {count}
- Rows actual: {count}
- Files expected: {count}
- Files actual: {count}
- Metadata matches: Yes/No

### Issues
- {Any discrepancies found}

### Conclusion
{Summary}
```

## Next Steps After Testing

### If All Tests Pass ✅
- Document compatibility guarantees
- Add CI integration for ongoing compatibility
- Proceed to Phase 2: Data File Compaction

### If Issues Found ❌
- Categorize by severity
- Fix critical compatibility issues
- Re-run test suite
- Update documentation with known limitations

## Resources

### Documentation
- [Iceberg Spec](https://iceberg.apache.org/spec/)
- [Iceberg Java Docs](https://iceberg.apache.org/javadoc/)
- [Parquet Format](https://parquet.apache.org/docs/)

### Tools
- Iceberg Java CLI
- Parquet Tools
- Spark SQL
- Our Rust implementation

---

**Estimated Timeline**: 2 weeks (10 working days)
**Risk Level**: Medium (dependent on Java environment setup)
**Value**: High (validates all Phase 1 work)
