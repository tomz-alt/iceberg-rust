# Java Compatibility Testing

This directory contains infrastructure for testing compatibility between the Rust Iceberg implementation and Apache Iceberg's Java reference implementation.

## Directory Structure

```
java_compat/
├── data/
│   ├── java_created/     # Tables created by Java for Rust to read
│   └── rust_created/     # Tables created by Rust for Java to read
├── scripts/
│   ├── java/            # Java test programs
│   │   ├── src/
│   │   ├── build.gradle
│   │   └── settings.gradle
│   └── rust/            # Rust test programs
└── results/             # Test results and reports
```

## Prerequisites

### Java Environment
- **Java 17+** (required by Apache Iceberg 1.7.x)
- **Gradle 8+** (for building Java tests)

Verify installation:
```bash
java -version    # Should show 17+
gradle -version  # Should show 8+
```

### Rust Environment
- **Rust 1.75+** (already installed for main project)
- All dependencies from main iceberg-rust project

## Quick Start

### 1. Build Java Test Harness

```bash
cd crates/integration_tests/tests/java_compat/scripts/java
gradle build
```

### 2. Create Test Tables with Java

```bash
cd crates/integration_tests/tests/java_compat/scripts/java
gradle run
```

This creates 8 test tables in `data/java_created/`:
1. `test1_basic_table` - Simple unpartitioned table
2. `test2_partitioned_table` - Multi-partition table
3. `test3_position_deletes` - Table with position delete files
4. `test4_multiple_snapshots` - Table with snapshot history
5. `test5_after_delete` - Table after DELETE operation
6. `test6_after_overwrite` - Table after OVERWRITE operation
7. `test7_schema_evolution` - Table with evolved schema
8. `test8_all_data_types` - Table testing all data types

### 3. Run Rust Validation Tests

```bash
cd ../../../../../..  # Back to project root
cargo test --test java_compat_test
```

### 4. Create Tables with Rust

```bash
cargo run --bin create_rust_test_tables
```

### 5. Validate with Java

```bash
cd crates/integration_tests/tests/java_compat/scripts/java
gradle validateRust
```

## Test Scenarios

### Scenario 1: Java → Rust (Read Compatibility)
**Goal**: Verify Rust can read Java-created tables

1. Java creates various test tables
2. Rust reads and validates:
   - Table metadata
   - Data files
   - Delete files
   - Snapshot history
   - Schema evolution

**Success Criteria**:
- ✅ All tables load without errors
- ✅ Row counts match expected
- ✅ Data values match expected
- ✅ Metadata is correctly parsed

### Scenario 2: Rust → Java (Write Compatibility)
**Goal**: Verify Java can read Rust-created tables

1. Rust creates test tables
2. Java/Spark reads and validates:
   - Table structure
   - Data correctness
   - Operation results (DELETE, OVERWRITE, EXPIRE)

**Success Criteria**:
- ✅ Spark can query tables
- ✅ Results match expected
- ✅ Metadata is valid

### Scenario 3: Operation Compatibility
**Goal**: Verify operations produce equivalent results

**DELETE Operation**:
- Create identical tables in Java and Rust
- Perform same DELETE in both
- Compare resulting snapshots, manifests, and delete files

**OVERWRITE Operation**:
- Create identical partitioned tables
- Perform same OVERWRITE in both
- Verify partition replacement is identical

**EXPIRE SNAPSHOTS Operation**:
- Create tables with identical snapshot history
- Expire same snapshots in both
- Verify cleanup is identical

## Running Individual Tests

### Test Specific Table Type

```bash
# Java: Create only partitioned table test
gradle run --args="test2_partitioned_table"

# Rust: Validate specific test
cargo test java_compat_test::test_partitioned_table
```

### Verbose Output

```bash
# Java
gradle run --info

# Rust
cargo test java_compat_test -- --nocapture
```

## Troubleshooting

### Java Build Issues

**Problem**: Gradle can't download dependencies
```bash
# Check network/proxy settings
gradle --info build

# Try using Gradle wrapper
./gradlew build
```

**Problem**: Java version mismatch
```bash
# Set JAVA_HOME explicitly
export JAVA_HOME=/path/to/java17
gradle build
```

### Rust Test Issues

**Problem**: Can't find Java-created tables
```bash
# Verify data directory exists
ls -la crates/integration_tests/tests/java_compat/data/java_created/

# Re-run Java table creation
cd scripts/java && gradle run
```

**Problem**: Metadata parsing errors
- Check Iceberg version compatibility
- Ensure Java used latest stable Iceberg (1.7.x)
- Compare `table-metadata.json` format

### Common Issues

**File permissions**:
```bash
chmod -R 755 crates/integration_tests/tests/java_compat/data/
```

**Stale test data**:
```bash
# Clean all test data
rm -rf crates/integration_tests/tests/java_compat/data/*

# Recreate
cd scripts/java && gradle run
```

## Validation Checklist

### For Each Test Table

- [ ] Table metadata loads successfully
- [ ] Schema matches expected
- [ ] Partition spec is correct
- [ ] Snapshot count matches
- [ ] Data file count matches
- [ ] Delete file count matches (if applicable)
- [ ] Row count matches expected
- [ ] Data values are correct
- [ ] Column statistics are present
- [ ] Manifest lists are valid
- [ ] Manifests are valid

### For Operations

- [ ] DELETE: Same rows deleted
- [ ] DELETE: Position delete files compatible
- [ ] OVERWRITE: Same files replaced
- [ ] OVERWRITE: Same partitions affected
- [ ] EXPIRE: Same snapshots removed
- [ ] EXPIRE: Same files cleaned up

## Test Results

Results are written to `results/` directory:

```
results/
├── java_to_rust_report.md
├── rust_to_java_report.md
└── operation_compatibility_report.md
```

Each report includes:
- Test name and timestamp
- Environment details (versions)
- Pass/fail status
- Detailed findings
- Performance metrics (optional)

## Next Steps

After compatibility testing:

1. **If all tests pass** ✅
   - Document compatibility guarantees
   - Add to CI/CD pipeline
   - Proceed to Phase 2: Optimization

2. **If issues found** ❌
   - Categorize by severity
   - File GitHub issues
   - Fix critical issues
   - Re-run test suite

## References

- [Apache Iceberg Specification](https://iceberg.apache.org/spec/)
- [Java API Documentation](https://iceberg.apache.org/javadoc/)
- [Parquet Format Spec](https://parquet.apache.org/docs/)
- [Rust Implementation Plan](../../../../IMPLEMENTATION_AND_TEST_PLAN.md)
