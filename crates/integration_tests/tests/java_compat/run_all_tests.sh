#!/bin/bash
# Run complete Java compatibility test suite

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../../../.." && pwd)"

echo "==================================================="
echo "Java Compatibility Test Suite"
echo "==================================================="
echo

# Step 1: Create Java test tables
echo "Step 1/3: Creating Java test tables with PySpark..."
echo "---------------------------------------------------"
cd "$SCRIPT_DIR/scripts"
./setup_and_run.sh

echo
echo "Step 2/3: Running Rust validation tests..."
echo "---------------------------------------------------"
cd "$PROJECT_ROOT"
cargo test --test java_compat_read_test -- --ignored --nocapture

echo
echo "==================================================="
echo "✅ All Tests Complete!"
echo "==================================================="
echo
echo "Summary:"
echo "  ✓ Created 8 Java test tables using PySpark/Iceberg"
echo "  ✓ Validated all tables readable by Rust implementation"
echo "  ✓ Verified schema compatibility"
echo "  ✓ Verified partition spec compatibility"
echo "  ✓ Verified snapshot compatibility"
echo
echo "Test results saved to:"
echo "  $SCRIPT_DIR/results/"
echo
