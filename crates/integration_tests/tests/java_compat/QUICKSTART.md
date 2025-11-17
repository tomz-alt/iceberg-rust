# Java Compatibility Testing - Quick Start Guide

Get up and running with Java compatibility testing in 5 minutes!

## Prerequisites

```bash
# Check you have the required tools
python3 --version  # Need 3.8+
java --version     # Need 11+
cargo --version    # Already installed
```

## Step 1: Create Java Test Tables (PySpark)

This will create 8 test tables using Java Iceberg implementation via PySpark:

```bash
cd crates/integration_tests/tests/java_compat/scripts
./setup_and_run.sh
```

**What this does:**
- Sets up Python virtual environment
- Installs PySpark with Iceberg support
- Creates 8 test tables covering:
  - Basic unpartitioned tables
  - Partitioned tables
  - Position deletes
  - Multiple snapshots
  - DELETE operations
  - OVERWRITE operations
  - Schema evolution
  - All data types

**Expected output:**
```
✓ Created table with 100 rows
✓ Snapshot ID: 123456789
...
✅ Test Table Creation Complete!
```

**Time:** ~2-3 minutes (includes downloading Spark/Iceberg JARs on first run)

## Step 2: Validate with Rust

Now test that our Rust implementation can read all Java-created tables:

```bash
# From project root
cd ../../../../../

# Run compatibility tests (they're marked as #[ignore] by default)
cargo test --test java_compat_read_test -- --ignored --nocapture
```

**Expected output:**
```
=== Test 1: Read basic unpartitioned table ===
  ✓ Schema validated (3 columns)
  ✓ Snapshot ID: 123456789
  ✓ Row count: 100

=== Test 2: Read partitioned table ===
  ✓ Schema validated (4 columns)
  ✓ Partition spec validated (2 fields)
  ✓ Row count: 180

... 8 tests total ...

test result: ok. 8 passed; 0 failed; 0 ignored
```

**Time:** ~30 seconds

## Step 3: Verify Specific Test Cases

Test individual scenarios:

```bash
# Test only basic table reading
cargo test --test java_compat_read_test test_read_basic_table -- --ignored --nocapture

# Test only partitioned tables
cargo test --test java_compat_read_test test_read_partitioned_table -- --ignored --nocapture

# Test position deletes
cargo test --test java_compat_read_test test_read_position_deletes -- --ignored --nocapture

# List all available test tables
cargo test --test java_compat_read_test list_java_test_tables -- --ignored --nocapture
```

## Quick Health Check

If anything goes wrong, run this diagnostic:

```bash
# Check if Java tables were created
ls -la crates/integration_tests/tests/java_compat/data/java_created/

# Should see 8 directories:
# test1_basic_table
# test2_partitioned_table
# test3_position_deletes
# test4_multiple_snapshots
# test5_after_delete
# test6_after_overwrite
# test7_schema_evolution
# test8_all_data_types

# Check a table's structure
ls -la crates/integration_tests/tests/java_compat/data/java_created/test1_basic_table/

# Should see:
# data/       - Parquet data files
# metadata/   - Table metadata JSON files
```

## Troubleshooting

### "Table path does not exist"
**Problem:** Java test tables haven't been created yet

**Solution:**
```bash
cd crates/integration_tests/tests/java_compat/scripts
./setup_and_run.sh
```

### "Failed to download Iceberg JAR"
**Problem:** Network/firewall blocking Maven Central

**Solution:**
```bash
# Check internet connection
ping repo1.maven.org

# If behind proxy, set:
export MAVEN_OPTS="-Dhttp.proxyHost=proxy.example.com -Dhttp.proxyPort=8080"

# Retry
./setup_and_run.sh
```

### Python environment issues
**Problem:** PySpark or dependencies won't install

**Solution:**
```bash
# Clean start
cd crates/integration_tests/tests/java_compat/scripts/python
rm -rf venv
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install pyspark==3.5.4
```

### Tests fail with "No current snapshot"
**Problem:** Table was created but has no data

**Solution:**
```bash
# Check table metadata
cat crates/integration_tests/tests/java_compat/data/java_created/test1_basic_table/metadata/*.metadata.json | jq '.snapshots'

# Should show at least one snapshot. If empty, recreate tables:
rm -rf crates/integration_tests/tests/java_compat/data/java_created
./scripts/setup_and_run.sh
```

## What's Next?

After validating Java → Rust compatibility, the next steps are:

### Phase 2: Rust → Java Testing
Create tables with Rust, validate with Java/Spark

### Phase 3: Operation Equivalence
Verify DELETE, OVERWRITE, EXPIRE produce identical results

See [JAVA_COMPAT_TESTING.md](../JAVA_COMPAT_TESTING.md) for the full testing plan.

## Success Criteria ✅

You'll know everything is working when:

- [x] All 8 Java test tables created successfully
- [x] All 8 Rust tests pass
- [x] Table metadata parses correctly
- [x] Row counts match expected values
- [x] Schema validation passes

## Performance Baseline

On a typical developer machine:

| Step | Time |
|------|------|
| Setup PySpark (first run) | ~2 min |
| Create 8 test tables | ~1 min |
| Run all Rust tests | ~30 sec |
| **Total (first run)** | **~3.5 min** |
| **Total (subsequent)** | **~1.5 min** |

## Quick Reference

```bash
# Complete workflow
cd crates/integration_tests/tests/java_compat/scripts
./setup_and_run.sh                          # Create Java tables
cd ../../../../../
cargo test --test java_compat_read_test -- --ignored --nocapture  # Test with Rust

# Clean and restart
rm -rf crates/integration_tests/tests/java_compat/data/java_created
cd crates/integration_tests/tests/java_compat/scripts
./setup_and_run.sh

# Individual test
cargo test --test java_compat_read_test test_read_basic_table -- --ignored --nocapture
```

---

**Questions or Issues?** See the [main testing docs](../JAVA_COMPAT_TESTING.md) or [README](README.md).
