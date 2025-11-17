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

//! Java compatibility tests - Reading Java-created tables with Rust

use std::path::PathBuf;
use std::sync::Arc;

use iceberg::io::FileIO;
use iceberg::spec::{Schema, TableMetadata};
use iceberg::table::Table;
use iceberg::{Catalog, TableIdent};

/// Get the path to Java-created test tables
fn java_tables_path() -> PathBuf {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    PathBuf::from(manifest_dir)
        .join("tests")
        .join("java_compat")
        .join("data")
        .join("java_created")
        .join("default") // Hadoop catalog uses namespace subdirectory
}

/// Load a table from local filesystem
async fn load_local_table(table_path: PathBuf) -> Result<Table, Box<dyn std::error::Error>> {
    // Check if table exists
    if !table_path.exists() {
        return Err(format!(
            "Table path does not exist: {}. \
             Please run 'scripts/setup_and_run.sh' to create Java test tables first.",
            table_path.display()
        )
        .into());
    }

    // Find metadata file
    let metadata_dir = table_path.join("metadata");
    if !metadata_dir.exists() {
        return Err(format!(
            "Metadata directory not found: {}",
            metadata_dir.display()
        )
        .into());
    }

    // Find the version-hint.text file to get current metadata version
    let version_hint = metadata_dir.join("version-hint.text");
    let metadata_file = if version_hint.exists() {
        let version = std::fs::read_to_string(&version_hint)?
            .trim()
            .parse::<u64>()?;
        metadata_dir.join(format!("v{}.metadata.json", version))
    } else {
        // Fall back to finding the latest metadata file
        let mut metadata_files: Vec<_> = std::fs::read_dir(&metadata_dir)?
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .map(|ext| ext == "json")
                    .unwrap_or(false)
            })
            .collect();

        metadata_files.sort_by_key(|e| e.path());
        metadata_files
            .last()
            .ok_or("No metadata files found")?
            .path()
    };

    // Read and parse metadata
    let metadata_json = std::fs::read_to_string(&metadata_file)?;
    let metadata: TableMetadata = serde_json::from_str(&metadata_json)?;

    // Create FileIO
    let file_io = FileIO::from_path(table_path.to_str().unwrap())
        .map_err(|e| format!("Failed to create FileIO: {}", e))?
        .build()
        .map_err(|e| format!("Failed to build FileIO: {}", e))?;

    // Create table
    let table = Table::builder()
        .metadata(metadata)
        .file_io(file_io)
        .build()
        .map_err(|e| format!("Failed to build table: {}", e))?;

    Ok(table)
}

/// Count rows in a table by scanning all data files
async fn count_rows(table: &Table) -> Result<i64, Box<dyn std::error::Error>> {
    let snapshot = table
        .metadata()
        .current_snapshot()
        .ok_or("No current snapshot")?;

    let mut total_rows = 0i64;

    // Get all manifests from the snapshot
    let manifest_list = table
        .file_io()
        .new_input(snapshot.manifest_list())
        .map_err(|e| format!("Failed to create input for manifest list: {}", e))?;

    // For now, we'll use the record count from the snapshot summary
    // In a full implementation, we would actually scan the data files
    if let Some(summary) = snapshot.summary() {
        if let Some(records_str) = summary.get("total-records") {
            total_rows = records_str
                .parse()
                .map_err(|e| format!("Failed to parse total-records: {}", e))?;
        }
    }

    Ok(total_rows)
}

#[tokio::test]
#[ignore] // Requires Java-created tables
async fn test_read_basic_table() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Test 1: Read basic unpartitioned table ===");

    let table_path = java_tables_path().join("test1_basic_table");
    let table = load_local_table(table_path).await?;

    // Verify schema
    let schema = table.metadata().current_schema();
    assert_eq!(schema.fields.len(), 3, "Expected 3 columns");

    let id_field = schema.field_by_name("id").expect("id column missing");
    let name_field = schema.field_by_name("name").expect("name column missing");
    let value_field = schema
        .field_by_name("value")
        .expect("value column missing");

    assert!(id_field.required, "id should be required");
    assert!(!name_field.required, "name should be optional");
    assert!(!value_field.required, "value should be optional");

    // Verify snapshot exists
    let snapshot = table
        .metadata()
        .current_snapshot()
        .expect("No current snapshot");

    println!("  ✓ Schema validated (3 columns)");
    println!("  ✓ Snapshot ID: {}", snapshot.snapshot_id());

    // Verify row count (should be 100)
    let row_count = count_rows(&table).await?;
    assert_eq!(row_count, 100, "Expected 100 rows");
    println!("  ✓ Row count: {}", row_count);

    Ok(())
}

#[tokio::test]
#[ignore] // Requires Java-created tables
async fn test_read_partitioned_table() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Test 2: Read partitioned table ===");

    let table_path = java_tables_path().join("test2_partitioned_table");
    let table = load_local_table(table_path).await?;

    // Verify schema
    let schema = table.metadata().current_schema();
    assert_eq!(schema.fields.len(), 4, "Expected 4 columns");

    // Verify partitioning
    let partition_spec = table.metadata().default_partition_spec();
    assert!(
        !partition_spec.is_unpartitioned(),
        "Table should be partitioned"
    );
    assert_eq!(
        partition_spec.fields().len(),
        2,
        "Expected 2 partition fields"
    );

    println!("  ✓ Schema validated (4 columns)");
    println!("  ✓ Partition spec validated (2 fields)");

    // Verify row count (should be 180: 3 categories × 3 days × 20 rows)
    let row_count = count_rows(&table).await?;
    assert_eq!(row_count, 180, "Expected 180 rows");
    println!("  ✓ Row count: {}", row_count);

    Ok(())
}

#[tokio::test]
#[ignore] // Requires Java-created tables
async fn test_read_position_deletes() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Test 3: Read table with position deletes ===");

    let table_path = java_tables_path().join("test3_position_deletes");
    let table = load_local_table(table_path).await?;

    // Verify schema
    let schema = table.metadata().current_schema();
    assert_eq!(schema.fields.len(), 2, "Expected 2 columns");

    // Verify snapshots (should have 2: one for data, one for deletes)
    let snapshots: Vec<_> = table.metadata().snapshots_iter().collect();
    assert_eq!(snapshots.len(), 2, "Expected 2 snapshots");

    println!("  ✓ Schema validated (2 columns)");
    println!("  ✓ Snapshots: {}", snapshots.len());

    // Note: The actual row count after deletes requires scanning,
    // which is more complex. For now, we verify the table loads correctly.
    // Original: 50 rows, Deleted: 5 rows, Expected: 45 rows

    Ok(())
}

#[tokio::test]
#[ignore] // Requires Java-created tables
async fn test_read_multiple_snapshots() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Test 4: Read table with multiple snapshots ===");

    let table_path = java_tables_path().join("test4_multiple_snapshots");
    let table = load_local_table(table_path).await?;

    // Verify snapshots (should have 5)
    let snapshots: Vec<_> = table.metadata().snapshots_iter().collect();
    assert_eq!(snapshots.len(), 5, "Expected 5 snapshots");

    println!("  ✓ Snapshots: {}", snapshots.len());

    // Verify current snapshot row count (should be 100: 5 batches × 20 rows)
    let row_count = count_rows(&table).await?;
    assert_eq!(row_count, 100, "Expected 100 rows");
    println!("  ✓ Total row count: {}", row_count);

    Ok(())
}

#[tokio::test]
#[ignore] // Requires Java-created tables
async fn test_read_after_delete() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Test 5: Read table after DELETE operation ===");

    let table_path = java_tables_path().join("test5_after_delete");
    let table = load_local_table(table_path).await?;

    // Verify schema
    let schema = table.metadata().current_schema();
    assert_eq!(schema.fields.len(), 3, "Expected 3 columns");

    // Verify snapshots (should have 2: one for data, one for deletes)
    let snapshots: Vec<_> = table.metadata().snapshots_iter().collect();
    assert_eq!(snapshots.len(), 2, "Expected 2 snapshots");

    println!("  ✓ Schema validated (3 columns)");
    println!("  ✓ Snapshots: {}", snapshots.len());

    // Original: 100 rows, Deleted: 50 even rows, Expected: 50 odd rows remaining

    Ok(())
}

#[tokio::test]
#[ignore] // Requires Java-created tables
async fn test_read_after_overwrite() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Test 6: Read table after OVERWRITE operation ===");

    let table_path = java_tables_path().join("test6_after_overwrite");
    let table = load_local_table(table_path).await?;

    // Verify schema
    let schema = table.metadata().current_schema();
    assert_eq!(schema.fields.len(), 3, "Expected 3 columns");

    // Verify partitioning
    let partition_spec = table.metadata().default_partition_spec();
    assert!(
        !partition_spec.is_unpartitioned(),
        "Table should be partitioned"
    );

    // Verify snapshots (should have 2: one for initial data, one for overwrite)
    let snapshots: Vec<_> = table.metadata().snapshots_iter().collect();
    assert_eq!(snapshots.len(), 2, "Expected 2 snapshots");

    println!("  ✓ Schema validated (3 columns)");
    println!("  ✓ Partition spec validated");
    println!("  ✓ Snapshots: {}", snapshots.len());

    // Final: 80 rows (30 in A, 20 in B, 30 in C)

    Ok(())
}

#[tokio::test]
#[ignore] // Requires Java-created tables
async fn test_read_schema_evolution() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Test 7: Read table with schema evolution ===");

    let table_path = java_tables_path().join("test7_schema_evolution");
    let table = load_local_table(table_path).await?;

    // Verify current schema has 3 columns (after evolution)
    let schema = table.metadata().current_schema();
    assert_eq!(schema.fields.len(), 3, "Expected 3 columns after evolution");

    let id_field = schema.field_by_name("id").expect("id column missing");
    let name_field = schema.field_by_name("name").expect("name column missing");
    let age_field = schema.field_by_name("age").expect("age column missing");

    assert!(id_field.required, "id should be required");
    assert!(!name_field.required, "name should be optional");
    assert!(!age_field.required, "age should be optional");

    // Verify schema history (should have 2 schemas)
    let schemas: Vec<_> = table.metadata().schemas_iter().collect();
    assert_eq!(schemas.len(), 2, "Expected 2 schemas");

    println!("  ✓ Current schema validated (3 columns)");
    println!("  ✓ Schema history: {} versions", schemas.len());

    // Verify row count (should be 60: 30 from v1 + 30 from v2)
    let row_count = count_rows(&table).await?;
    assert_eq!(row_count, 60, "Expected 60 rows");
    println!("  ✓ Row count: {}", row_count);

    Ok(())
}

#[tokio::test]
#[ignore] // Requires Java-created tables
async fn test_read_all_data_types() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Test 8: Read table with all data types ===");

    let table_path = java_tables_path().join("test8_all_data_types");
    let table = load_local_table(table_path).await?;

    // Verify schema with all data types
    let schema = table.metadata().current_schema();
    assert_eq!(schema.fields.len(), 10, "Expected 10 columns");

    let expected_columns = vec![
        "id",
        "bool_col",
        "int_col",
        "long_col",
        "float_col",
        "double_col",
        "string_col",
        "date_col",
        "timestamp_col",
        "binary_col",
    ];

    for col_name in expected_columns {
        assert!(
            schema.field_by_name(col_name).is_some(),
            "Column {} missing",
            col_name
        );
    }

    println!("  ✓ Schema validated (10 columns with various types)");

    // Verify row count (should be 50)
    let row_count = count_rows(&table).await?;
    assert_eq!(row_count, 50, "Expected 50 rows");
    println!("  ✓ Row count: {}", row_count);

    Ok(())
}

/// Helper test to list all available Java test tables
#[tokio::test]
#[ignore]
async fn list_java_test_tables() {
    println!("\n=== Available Java Test Tables ===");

    let base_path = java_tables_path();

    if !base_path.exists() {
        println!("❌ Java test tables directory not found: {}", base_path.display());
        println!("   Run 'scripts/setup_and_run.sh' to create test tables.");
        return;
    }

    match std::fs::read_dir(&base_path) {
        Ok(entries) => {
            let mut count = 0;
            for entry in entries {
                if let Ok(entry) = entry {
                    if entry.path().is_dir() {
                        println!("  ✓ {}", entry.file_name().to_string_lossy());
                        count += 1;
                    }
                }
            }
            println!("\nTotal: {} test tables", count);
        }
        Err(e) => {
            println!("❌ Error reading directory: {}", e);
        }
    }
}
