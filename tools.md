# Tool Usage Reference

**Last Updated**: 2025-11-17

## Build & Test

```bash
# Build library
cargo build --package iceberg --lib

# Run specific test
cargo test --package iceberg --lib transaction::delete::tests::test_delete_requires_filter

# Run all DELETE tests
cargo test --package iceberg --lib transaction::delete::tests -- --test-threads=1

# Check for errors only
cargo build --package iceberg --lib 2>&1 | grep -E "error|warning"
```

## Git Operations

```bash
# Fetch and pull latest
git fetch origin
git pull origin claude/compare-with-iceberg-01JPFMWKjBdf71usxVQtYeeD

# Commit with message
git add -A
git commit -m "feat: ..."
git push -u origin claude/compare-with-iceberg-01JPFMWKjBdf71usxVQtYeeD

# View recent commits
git log --oneline -10
```

## File Searches

```bash
# Find TODOs
grep -n "TODO\|FIXME" /home/user/iceberg-rust/crates/iceberg/src/transaction/compact.rs

# Find implementations
grep -n "impl.*TransactionAction\|pub async fn commit" crates/iceberg/src/transaction/*.rs

# Search for patterns
grep -rn "PositionDeleteFileWriter" crates/iceberg/src/
```

## Key File Paths

- DELETE: `crates/iceberg/src/transaction/delete.rs`
- Compaction: `crates/iceberg/src/transaction/compact.rs`
- Progress: `PROGRESS.md`, `PHASE2_PROGRESS_UPDATE.md`
- Tests: `crates/integration_tests/tests/java_compat/`

## Branch

- Active: `claude/compare-with-iceberg-01JPFMWKjBdf71usxVQtYeeD`
- Must start with `claude/` and end with session ID for push to work
