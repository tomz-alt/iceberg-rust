#!/usr/bin/env python3
"""
Create test tables using PySpark with Iceberg for Rust compatibility testing.

This uses the Java Iceberg implementation via PySpark to create various test scenarios.
"""

import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import date, datetime


def get_spark_session(warehouse_path):
    """Create Spark session with Iceberg support."""
    # Set warehouse to java_created directory so tables are created there
    java_warehouse = os.path.join(warehouse_path, "java_created")
    os.makedirs(java_warehouse, exist_ok=True)

    return (SparkSession.builder
        .appName("IcebergRustCompatTests")
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", java_warehouse)
        .config("spark.sql.defaultCatalog", "local")
        .getOrCreate())


def test1_basic_table(spark, base_path):
    """Test 1: Basic unpartitioned table"""
    print("\n=== Test 1: Basic unpartitioned table ===")

    table_name = "test1_basic_table"

    # Create table (warehouse path already points to java_created)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS local.default.{table_name} (
            id BIGINT,
            name STRING,
            value DOUBLE
        ) USING iceberg
    """)

    # Insert data
    data = [(i, f"name_{i}", i * 1.5) for i in range(1, 101)]
    df = spark.createDataFrame(data, ["id", "name", "value"])
    df.writeTo(f"local.default.{table_name}").append()

    # Verify
    count = spark.sql(f"SELECT COUNT(*) FROM local.default.{table_name}").collect()[0][0]
    print(f"  ✓ Created table with {count} rows")

    # Show snapshot info
    snapshots = spark.sql(f"SELECT snapshot_id FROM local.default.{table_name}.snapshots").collect()
    print(f"  ✓ Snapshot ID: {snapshots[-1][0]}")


def test2_partitioned_table(spark, base_path):
    """Test 2: Partitioned table"""
    print("\n=== Test 2: Partitioned table ===")

    table_name = "test2_partitioned_table"

    # Create partitioned table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS local.default.{table_name} (
            id BIGINT,
            date DATE,
            category STRING,
            value DOUBLE
        ) USING iceberg
        PARTITIONED BY (days(date), category)
    """)

    # Insert data for multiple partitions
    data = []
    categories = ["A", "B", "C"]
    base_date = date(2022, 1, 1)

    for cat in categories:
        for day_offset in range(3):
            curr_date = date(2022, 1, 1 + day_offset)
            for i in range(20):
                record_id = ord(cat) * 1000 + day_offset * 100 + i
                data.append((record_id, curr_date, cat, i * 2.5))

    df = spark.createDataFrame(data, ["id", "date", "category", "value"])
    df.writeTo(f"local.default.{table_name}").append()

    # Verify
    count = spark.sql(f"SELECT COUNT(*) FROM local.default.{table_name}").collect()[0][0]
    partitions = spark.sql(f"SELECT COUNT(DISTINCT category, date) FROM local.default.{table_name}").collect()[0][0]
    print(f"  ✓ Created partitioned table with {partitions} partitions")
    print(f"  ✓ Total records: {count}")


def test3_position_deletes(spark, base_path):
    """Test 3: Table with position deletes"""
    print("\n=== Test 3: Table with position deletes ===")

    table_name = "test3_position_deletes"

    # Create table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS local.default.{table_name} (
            id BIGINT,
            data STRING
        ) USING iceberg
    """)

    # Insert initial data
    data = [(i, f"data_{i}") for i in range(1, 51)]
    df = spark.createDataFrame(data, ["id", "data"])
    df.writeTo(f"local.default.{table_name}").append()

    # Delete some rows (ids 10-14)
    spark.sql(f"DELETE FROM local.default.{table_name} WHERE id >= 10 AND id <= 14")

    # Verify
    count = spark.sql(f"SELECT COUNT(*) FROM local.default.{table_name}").collect()[0][0]
    print(f"  ✓ Created table with 50 rows")
    print(f"  ✓ Deleted 5 rows (ids 10-14)")
    print(f"  ✓ Effective row count: {count}")


def test4_multiple_snapshots(spark, base_path):
    """Test 4: Table with multiple snapshots"""
    print("\n=== Test 4: Table with multiple snapshots ===")

    table_name = "test4_multiple_snapshots"

    # Create table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS local.default.{table_name} (
            id BIGINT,
            batch INT
        ) USING iceberg
    """)

    # Create 5 snapshots
    for batch in range(1, 6):
        data = [(batch * 20 + i, batch) for i in range(1, 21)]
        df = spark.createDataFrame(data, ["id", "batch"])
        df.writeTo(f"local.default.{table_name}").append()

    # Verify
    count = spark.sql(f"SELECT COUNT(*) FROM local.default.{table_name}").collect()[0][0]
    snapshots = spark.sql(f"SELECT COUNT(*) FROM local.default.{table_name}.snapshots").collect()[0][0]
    print(f"  ✓ Created {snapshots} snapshots")
    print(f"  ✓ Total records: {count}")


def test5_after_delete(spark, base_path):
    """Test 5: Table after DELETE operation"""
    print("\n=== Test 5: Table after DELETE operation ===")

    table_name = "test5_after_delete"

    # Create table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS local.default.{table_name} (
            id BIGINT,
            category STRING,
            value INT
        ) USING iceberg
    """)

    # Insert data
    data = [(i, "even" if i % 2 == 0 else "odd", i) for i in range(1, 101)]
    df = spark.createDataFrame(data, ["id", "category", "value"])
    df.writeTo(f"local.default.{table_name}").append()

    # Delete even rows
    spark.sql(f"DELETE FROM local.default.{table_name} WHERE category = 'even'")

    # Verify
    count = spark.sql(f"SELECT COUNT(*) FROM local.default.{table_name}").collect()[0][0]
    print(f"  ✓ Created table with 100 rows")
    print(f"  ✓ Deleted 50 even-numbered rows")
    print(f"  ✓ Effective row count: {count}")


def test6_after_overwrite(spark, base_path):
    """Test 6: Table after OVERWRITE operation"""
    print("\n=== Test 6: Table after OVERWRITE operation ===")

    table_name = "test6_after_overwrite"

    # Create partitioned table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS local.default.{table_name} (
            id BIGINT,
            partition_col STRING,
            value INT
        ) USING iceberg
        PARTITIONED BY (partition_col)
    """)

    # Insert data for partitions A, B, C
    data = []
    for part in ["A", "B", "C"]:
        for i in range(1, 31):
            data.append((ord(part) * 100 + i, part, i))

    df = spark.createDataFrame(data, ["id", "partition_col", "value"])
    df.writeTo(f"local.default.{table_name}").append()

    # Overwrite partition B
    new_data = [(9000 + i, "B", i * 10) for i in range(1, 21)]
    df_new = spark.createDataFrame(new_data, ["id", "partition_col", "value"])
    df_new.writeTo(f"local.default.{table_name}").overwritePartitions()

    # Verify
    total = spark.sql(f"SELECT COUNT(*) FROM local.default.{table_name}").collect()[0][0]
    part_b = spark.sql(f"SELECT COUNT(*) FROM local.default.{table_name} WHERE partition_col = 'B'").collect()[0][0]
    print(f"  ✓ Created partitioned table with 3 partitions (90 rows total)")
    print(f"  ✓ Overwrote partition B (30 rows → {part_b} rows)")
    print(f"  ✓ Final row count: {total}")


def test7_schema_evolution(spark, base_path):
    """Test 7: Table with schema evolution"""
    print("\n=== Test 7: Table with schema evolution ===")

    table_name = "test7_schema_evolution"

    # Create table with initial schema
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS local.default.{table_name} (
            id BIGINT,
            name STRING
        ) USING iceberg
    """)

    # Insert data with schema v1
    data = [(i, f"name_{i}") for i in range(1, 31)]
    df = spark.createDataFrame(data, ["id", "name"])
    df.writeTo(f"local.default.{table_name}").append()

    # Evolve schema: add age column
    spark.sql(f"ALTER TABLE local.default.{table_name} ADD COLUMN age INT")

    # Insert data with schema v2
    schema = StructType([
        StructField("id", LongType(), False),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)
    ])
    data2 = [(i, f"name_{i}", 20 + i % 50) for i in range(31, 61)]
    df2 = spark.createDataFrame(data2, schema)
    df2.writeTo(f"local.default.{table_name}").append()

    # Verify
    count = spark.sql(f"SELECT COUNT(*) FROM local.default.{table_name}").collect()[0][0]
    print(f"  ✓ Created table with schema v1 (id, name)")
    print(f"  ✓ Wrote 30 rows")
    print(f"  ✓ Evolved schema to v2 (added 'age' column)")
    print(f"  ✓ Wrote 30 more rows")
    print(f"  ✓ Total rows: {count}")


def test8_all_data_types(spark, base_path):
    """Test 8: Table with all data types"""
    print("\n=== Test 8: Table with all data types ===")

    table_name = "test8_all_data_types"

    # Create table with various types
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS local.default.{table_name} (
            id BIGINT,
            bool_col BOOLEAN,
            int_col INT,
            long_col BIGINT,
            float_col FLOAT,
            double_col DOUBLE,
            string_col STRING,
            date_col DATE,
            timestamp_col TIMESTAMP,
            binary_col BINARY
        ) USING iceberg
    """)

    # Create diverse data
    schema = StructType([
        StructField("id", LongType(), False),
        StructField("bool_col", BooleanType(), True),
        StructField("int_col", IntegerType(), True),
        StructField("long_col", LongType(), True),
        StructField("float_col", FloatType(), True),
        StructField("double_col", DoubleType(), True),
        StructField("string_col", StringType(), True),
        StructField("date_col", DateType(), True),
        StructField("timestamp_col", TimestampType(), True),
        StructField("binary_col", BinaryType(), True),
    ])

    data = []
    for i in range(1, 51):
        data.append((
            i,
            i % 2 == 0,
            i * 100,
            i * 1000000,
            float(i * 1.5),
            i * 2.718281828,
            f"test_{i}",
            date(2022, 1, 1 + i % 28),
            datetime(2022, 1, 1, 12, 0, i % 60),
            f"binary_{i}".encode()
        ))

    df = spark.createDataFrame(data, schema)
    df.writeTo(f"local.default.{table_name}").append()

    # Verify
    count = spark.sql(f"SELECT COUNT(*) FROM local.default.{table_name}").collect()[0][0]
    print(f"  ✓ Created table with all common data types")
    print(f"  ✓ Rows: {count}")


def main():
    """Main execution"""
    # Get base data path
    base_path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
        os.path.dirname(__file__), "..", "..", "data"
    )
    base_path = os.path.abspath(base_path)

    # Create directories
    os.makedirs(os.path.join(base_path, "java_created"), exist_ok=True)

    print("=" * 60)
    print("Creating Java Iceberg Test Tables (via PySpark)")
    print("=" * 60)
    print(f"Output directory: {base_path}/java_created")
    print()

    # Initialize Spark
    print("Initializing Spark with Iceberg support...")
    spark = get_spark_session(base_path)

    try:
        # Create all test tables
        test1_basic_table(spark, base_path)
        test2_partitioned_table(spark, base_path)
        test3_position_deletes(spark, base_path)
        test4_multiple_snapshots(spark, base_path)
        test5_after_delete(spark, base_path)
        test6_after_overwrite(spark, base_path)
        test7_schema_evolution(spark, base_path)
        test8_all_data_types(spark, base_path)

        print("\n" + "=" * 60)
        print("✅ Test Table Creation Complete!")
        print("=" * 60)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
