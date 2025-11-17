#!/bin/bash
# Setup and run Java compatibility tests

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../../../.." && pwd)"
DATA_DIR="$SCRIPT_DIR/../data"

echo "==================================================="
echo "Java Compatibility Testing Setup"
echo "==================================================="
echo

# Check prerequisites
echo "Checking prerequisites..."

# Check Python
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 not found. Please install Python 3.8+"
    exit 1
fi
echo "✓ Python 3: $(python3 --version)"

# Check Java
if ! command -v java &> /dev/null; then
    echo "❌ Java not found. Please install Java 11+"
    exit 1
fi
JAVA_VERSION=$(java -version 2>&1 | head -n 1)
echo "✓ Java: $JAVA_VERSION"

echo

# Setup Python environment
echo "Setting up Python environment..."
cd "$SCRIPT_DIR/python"

if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

echo "Activating virtual environment..."
source venv/bin/activate

echo "Installing dependencies..."
pip install -q --upgrade pip
pip install -q -r requirements.txt

echo "✓ Python environment ready"
echo

# Run table creation
echo "==================================================="
echo "Creating Test Tables with PySpark/Iceberg"
echo "==================================================="
echo

chmod +x create_test_tables.py
python3 create_test_tables.py "$DATA_DIR"

echo
echo "==================================================="
echo "✅ Java Test Tables Created Successfully!"
echo "==================================================="
echo
echo "Next steps:"
echo "  1. Run Rust validation tests:"
echo "     cd $PROJECT_ROOT"
echo "     cargo test --test java_compat_read_test"
echo
echo "  2. Create Rust test tables:"
echo "     cargo run --bin create_rust_test_tables"
echo
echo "  3. Validate Rust tables with Spark:"
echo "     $SCRIPT_DIR/python/validate_rust_tables.py"
echo

deactivate
