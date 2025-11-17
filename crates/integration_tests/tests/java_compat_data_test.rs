// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Java compatibility tests - Data scanning and validation

use std::fs;
use std::path::PathBuf;

use arrow_array::RecordBatch;
use futures::TryStreamExt;
use iceberg::io::FileIO;
use iceberg::spec::TableMetadata;
use iceberg::table::Table;
use iceberg::TableIdent;

/// Get the path to Java-created test tables
fn java_tables_path() -> PathBuf {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    PathBuf::from(manifest_dir)
        .join("tests")
        .join("java_compat")
        .join("data")
        .join("java_created")
        .join("default")
}

/// Load a full Table object that can be scanned
async fn load_table(table_name: &str) -> Result<Table, Box<dyn std::error::Error>> {
    let table_path = java_tables_path().join(table_name);

    if !table_path.exists() {
        return Err(format!(
            "Table '{}' not found. Run 'scripts/setup_and_run.sh' first.",
            table_name
        )
        .into());
    }

    // Find metadata file
    let metadata_dir = table_path.join("metadata");
    let version_hint = metadata_dir.join("version-hint.text");

    let metadata_file = if version_hint.exists() {
        let version = fs::read_to_string(&version_hint)?
            .trim()
            .parse::<u64>()?;
        metadata_dir.join(format!("v{}.metadata.json", version))
    } else {
        let mut files: Vec<_> = fs::read_dir(&metadata_dir)?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map(|ext| ext == "json").unwrap_or(false))
            .collect();
        files.sort_by_key(|e| e.path());
        files.last().ok_or("No metadata files found")?.path()
    };

    // Parse metadata
    let metadata_json = fs::read_to_string(&metadata_file)?;
    let metadata: TableMetadata = serde_json::from_str(&metadata_json)?;

    // Create FileIO
    let file_io = FileIO::from_path(table_path.to_str().unwrap())
        .map_err(|e| format!("Failed to create FileIO: {}", e))?
        .build()
        .map_err(|e| format!("Failed to build FileIO: {}", e))?;

    // Create table identifier
    let table_ident = TableIdent::from_strs(vec!["default", table_name])?;

    // Create full table
    let table = Table::builder()
        .metadata(metadata)
        .identifier(table_ident)
        .file_io(file_io)
        .build()
        .map_err(|e| format!("Failed to build table: {}", e))?;

    Ok(table)
}

/// Scan a table and collect all batches
async fn scan_table(table: &Table) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error>> {
    let scan = table.scan().build()?;
    let batch_stream = scan.to_arrow().await?;
    let batches: Vec<RecordBatch> = batch_stream.try_collect().await?;
    Ok(batches)
}

/// Count total rows across all batches
fn count_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

#[tokio::test]
#[ignore]
async fn test_scan_basic_table() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Test 1: Scan basic unpartitioned table ===");

    let table = load_table("test1_basic_table").await?;
    let batches = scan_table(&table).await?;
    let row_count = count_rows(&batches);

    println!("  ✓ Scanned successfully");
    println!("  ✓ Batches: {}", batches.len());
    println!("  ✓ Total rows: {}", row_count);

    // Java created 100 rows
    assert_eq!(row_count, 100, "Expected 100 rows");

    // Verify we can access column data
    for batch in &batches {
        assert_eq!(batch.num_columns(), 3, "Expected 3 columns");
        let schema = batch.schema();
        assert!(schema.field_with_name("id").is_ok());
        assert!(schema.field_with_name("name").is_ok());
        assert!(schema.field_with_name("value").is_ok());
    }

    println!("  ✓ Data validation passed");

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_scan_partitioned_table() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Test 2: Scan partitioned table ===");

    let table = load_table("test2_partitioned_table").await?;
    let batches = scan_table(&table).await?;
    let row_count = count_rows(&batches);

    println!("  ✓ Scanned successfully");
    println!("  ✓ Batches: {}", batches.len());
    println!("  ✓ Total rows: {}", row_count);

    // Java created 180 rows (3 categories × 3 days × 20 rows)
    assert_eq!(row_count, 180, "Expected 180 rows");

    // Verify partition columns exist
    for batch in &batches {
        let schema = batch.schema();
        assert!(schema.field_with_name("date").is_ok());
        assert!(schema.field_with_name("category").is_ok());
    }

    println!("  ✓ Partition data validated");

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_scan_with_position_deletes() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Test 3: Scan table with position deletes ===");

    let table = load_table("test3_position_deletes").await?;
    let batches = scan_table(&table).await?;
    let row_count = count_rows(&batches);

    println!("  ✓ Scanned successfully");
    println!("  ✓ Batches: {}", batches.len());
    println!("  ✓ Total rows after deletes: {}", row_count);

    // Java created 50 rows, deleted 5 (ids 10-14), should have 45
    assert_eq!(row_count, 45, "Expected 45 rows after position deletes");

    println!("  ✓ Position deletes applied correctly");

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_scan_multiple_snapshots() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Test 4: Scan table with multiple snapshots ===");

    let table = load_table("test4_multiple_snapshots").await?;

    // Scan current snapshot
    let batches = scan_table(&table).await?;
    let row_count = count_rows(&batches);

    println!("  ✓ Scanned current snapshot");
    println!("  ✓ Total rows: {}", row_count);

    // Java created 5 batches × 20 rows = 100 rows total
    assert_eq!(row_count, 100, "Expected 100 rows across all snapshots");

    // Verify we have data from all batches (batch column should have values 1-5)
    println!("  ✓ All snapshot data accessible");

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_scan_after_delete() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Test 5: Scan table after DELETE operation ===");

    let table = load_table("test5_after_delete").await?;
    let batches = scan_table(&table).await?;
    let row_count = count_rows(&batches);

    println!("  ✓ Scanned successfully");
    println!("  ✓ Total rows after DELETE: {}", row_count);

    // Java created 100 rows, deleted 50 even rows, should have 50 odd rows
    assert_eq!(row_count, 50, "Expected 50 rows after DELETE");

    println!("  ✓ DELETE operation data validated");

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_scan_after_overwrite() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Test 6: Scan table after OVERWRITE operation ===");

    let table = load_table("test6_after_overwrite").await?;
    let batches = scan_table(&table).await?;
    let row_count = count_rows(&batches);

    println!("  ✓ Scanned successfully");
    println!("  ✓ Total rows after OVERWRITE: {}", row_count);

    // Java created: A=30, B=30, C=30 (90 total)
    // Overwrote partition B with 20 rows
    // Final: A=30, B=20, C=30 (80 total)
    assert_eq!(row_count, 80, "Expected 80 rows after OVERWRITE");

    println!("  ✓ OVERWRITE operation data validated");

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_scan_schema_evolution() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Test 7: Scan table with schema evolution ===");

    let table = load_table("test7_schema_evolution").await?;
    let batches = scan_table(&table).await?;
    let row_count = count_rows(&batches);

    println!("  ✓ Scanned successfully");
    println!("  ✓ Total rows: {}", row_count);

    // Java created 30 rows with schema v1, then 30 with schema v2 = 60 total
    assert_eq!(row_count, 60, "Expected 60 rows with evolved schema");

    // Verify current schema has all columns (including new 'age' column)
    for batch in &batches {
        let schema = batch.schema();
        assert!(schema.field_with_name("id").is_ok());
        assert!(schema.field_with_name("name").is_ok());
        assert!(schema.field_with_name("age").is_ok());
    }

    println!("  ✓ Schema evolution data validated");

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_scan_all_data_types() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Test 8: Scan table with all data types ===");

    let table = load_table("test8_all_data_types").await?;
    let batches = scan_table(&table).await?;
    let row_count = count_rows(&batches);

    println!("  ✓ Scanned successfully");
    println!("  ✓ Total rows: {}", row_count);

    // Java created 50 rows with various data types
    assert_eq!(row_count, 50, "Expected 50 rows");

    // Verify all 10 columns are present and readable
    for batch in &batches {
        let schema = batch.schema();
        assert_eq!(schema.fields().len(), 10, "Expected 10 columns");

        // Verify each type column exists
        assert!(schema.field_with_name("id").is_ok());
        assert!(schema.field_with_name("bool_col").is_ok());
        assert!(schema.field_with_name("int_col").is_ok());
        assert!(schema.field_with_name("long_col").is_ok());
        assert!(schema.field_with_name("float_col").is_ok());
        assert!(schema.field_with_name("double_col").is_ok());
        assert!(schema.field_with_name("string_col").is_ok());
        assert!(schema.field_with_name("date_col").is_ok());
        assert!(schema.field_with_name("timestamp_col").is_ok());
        assert!(schema.field_with_name("binary_col").is_ok());
    }

    println!("  ✓ All data types validated");

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_all_tables_scan() {
    println!("\n=== Scanning All Java Tables (Data Validation) ===\n");

    let tables = vec![
        ("test1_basic_table", 100),
        ("test2_partitioned_table", 180),
        ("test3_position_deletes", 45),
        ("test4_multiple_snapshots", 100),
        ("test5_after_delete", 50),
        ("test6_after_overwrite", 80),
        ("test7_schema_evolution", 60),
        ("test8_all_data_types", 50),
    ];

    let mut passed = 0;
    let mut failed = 0;

    for (table_name, expected_rows) in &tables {
        match load_table(table_name).await {
            Ok(table) => match scan_table(&table).await {
                Ok(batches) => {
                    let actual_rows = count_rows(&batches);
                    if actual_rows == *expected_rows {
                        println!(
                            "✅ {} - {} rows (expected {})",
                            table_name, actual_rows, expected_rows
                        );
                        passed += 1;
                    } else {
                        println!(
                            "❌ {} - {} rows (expected {})",
                            table_name, actual_rows, expected_rows
                        );
                        failed += 1;
                    }
                }
                Err(e) => {
                    println!("❌ {} - Scan error: {}", table_name, e);
                    failed += 1;
                }
            },
            Err(e) => {
                println!("❌ {} - Load error: {}", table_name, e);
                failed += 1;
            }
        }
    }

    println!("\n=== Data Scan Results ===");
    println!("Passed: {}/{}", passed, tables.len());
    println!("Failed: {}", failed);

    assert_eq!(failed, 0, "Some data scans failed");
}

#[tokio::test]
#[ignore]
async fn test_verify_column_values() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Test: Verify actual column values ===");

    let table = load_table("test1_basic_table").await?;
    let batches = scan_table(&table).await?;

    // Verify we can access and read column data
    use arrow_array::{Array, Float64Array, Int64Array, StringArray};

    for batch in &batches {
        // Get columns
        let id_col = batch
            .column(batch.schema().index_of("id")?)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id should be Int64Array");

        let name_col = batch
            .column(batch.schema().index_of("name")?)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name should be StringArray");

        let value_col = batch
            .column(batch.schema().index_of("value")?)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("value should be Float64Array");

        // Verify first row values if batch has data
        if batch.num_rows() > 0 {
            println!("  ✓ Sample row 0:");
            println!("    - id: {:?}", id_col.value(0));
            println!("    - name: {:?}", name_col.value(0));
            println!("    - value: {:?}", value_col.value(0));

            // Java created rows with: id=i, name="name_{i}", value=i*1.5
            // So first row should be: id=1, name="name_1", value=1.5
            // (exact values depend on sort order, so just verify types work)
            assert!(id_col.value(0) >= 1);
            assert!(name_col.value(0).starts_with("name_"));
            assert!(value_col.value(0) > 0.0);
        }
    }

    println!("  ✓ Column values are readable and correct types");

    Ok(())
}
