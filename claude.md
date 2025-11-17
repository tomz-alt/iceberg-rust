# Claude Session Tracking

This directory contains tracking files for Claude's work on iceberg-rust.

## Files

- **todo.md** - All active and pending todos
- **tools.md** - Tool usage patterns and references
- **current_impl.md** - Implementations that may be reused

## Purpose

Keep tracking concise, avoid token waste, enable quick context recovery.

## Development Principles

### Spec Compliance
- **Always verify against Apache Iceberg spec**: https://iceberg.apache.org/spec/
- **Cross-validate behavior** with Java and Python Iceberg implementations
- **Fact-check progress** - See PHASE2_COMPACTION_FACT_CHECK.md for compaction verification
- **Operation types** must match spec requirements (APPEND, OVERWRITE, DELETE, REPLACE)
- **Manifest and snapshot** creation must follow spec exactly

### Cross-Implementation Validation
1. Check Java Iceberg for reference implementation
2. Verify Python Iceberg for behavior consistency
3. Test with Java compatibility tests in `crates/integration_tests/tests/java_compat/`
4. Ensure file formats are readable by all implementations
