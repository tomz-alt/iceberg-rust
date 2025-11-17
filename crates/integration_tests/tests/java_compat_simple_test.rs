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

//! Simple Java compatibility tests - Validates we can read Java-created tables

use std::fs;
use std::path::PathBuf;

use iceberg::spec::TableMetadata;
use serde_json;

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

/// Load table metadata from a table directory
fn load_table_metadata(table_name: &str) -> Result<TableMetadata, Box<dyn std::error::Error>> {
    let table_path = java_tables_path().join(table_name);

    if !table_path.exists() {
        return Err(format!(
            "Table '{}' not found. Please run 'scripts/setup_and_run.sh' first.",
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
        // Fall back to latest file
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

    Ok(metadata)
}

#[test]
#[ignore] // Requires Java-created tables
fn test_can_list_java_tables() {
    let base_path = java_tables_path();

    if !base_path.exists() {
        println!("❌ Java test tables not found at: {}", base_path.display());
        println!("   Run: crates/integration_tests/tests/java_compat/scripts/setup_and_run.sh");
        panic!("Java test tables not created");
    }

    let tables: Vec<_> = fs::read_dir(&base_path)
        .expect("Failed to read java_created directory")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();

    println!("\n✅ Found {} Java test tables:", tables.len());
    for table in &tables {
        println!("  - {}", table);
    }

    assert!(tables.len() >= 8, "Expected at least 8 test tables");
}

#[test]
#[ignore]
fn test_load_basic_table() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Test 1: Load basic unpartitioned table ===");

    let metadata = load_table_metadata("test1_basic_table")?;

    println!("  ✓ Metadata loaded");
    println!("  ✓ Schema ID: {}", metadata.current_schema_id());
    println!("  ✓ Fields: {}", metadata.current_schema().as_struct().fields().len());

    assert_eq!(metadata.current_schema().as_struct().fields().len(), 3);
    assert!(metadata.current_snapshot().is_some());

    Ok(())
}

#[test]
#[ignore]
fn test_load_partitioned_table() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Test 2: Load partitioned table ===");

    let metadata = load_table_metadata("test2_partitioned_table")?;

    println!("  ✓ Metadata loaded");
    println!("  ✓ Fields: {}", metadata.current_schema().as_struct().fields().len());
    println!("  ✓ Partitioned: {}", !metadata.default_partition_spec().is_unpartitioned());

    assert_eq!(metadata.current_schema().as_struct().fields().len(), 4);
    assert!(!metadata.default_partition_spec().is_unpartitioned());

    Ok(())
}

#[test]
#[ignore]
fn test_load_position_deletes() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Test 3: Load table with position deletes ===");

    let metadata = load_table_metadata("test3_position_deletes")?;

    let snapshot_count = metadata.snapshots().count();
    println!("  ✓ Metadata loaded");
    println!("  ✓ Snapshots: {}", snapshot_count);

    assert_eq!(snapshot_count, 2, "Expected 2 snapshots (data + deletes)");

    Ok(())
}

#[test]
#[ignore]
fn test_load_multiple_snapshots() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Test 4: Load table with multiple snapshots ===");

    let metadata = load_table_metadata("test4_multiple_snapshots")?;

    let snapshot_count = metadata.snapshots().count();
    println!("  ✓ Metadata loaded");
    println!("  ✓ Snapshots: {}", snapshot_count);

    assert_eq!(snapshot_count, 5);

    Ok(())
}

#[test]
#[ignore]
fn test_load_after_delete() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Test 5: Load table after DELETE operation ===");

    let metadata = load_table_metadata("test5_after_delete")?;

    let snapshot_count = metadata.snapshots().count();
    println!("  ✓ Metadata loaded");
    println!("  ✓ Snapshots: {}", snapshot_count);

    assert_eq!(snapshot_count, 2);

    Ok(())
}

#[test]
#[ignore]
fn test_load_after_overwrite() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Test 6: Load table after OVERWRITE operation ===");

    let metadata = load_table_metadata("test6_after_overwrite")?;

    let snapshot_count = metadata.snapshots().count();
    println!("  ✓ Metadata loaded");
    println!("  ✓ Partitioned: {}", !metadata.default_partition_spec().is_unpartitioned());
    println!("  ✓ Snapshots: {}", snapshot_count);

    assert_eq!(snapshot_count, 2);
    assert!(!metadata.default_partition_spec().is_unpartitioned());

    Ok(())
}

#[test]
#[ignore]
fn test_load_schema_evolution() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Test 7: Load table with schema evolution ===");

    let metadata = load_table_metadata("test7_schema_evolution")?;

    let schema_count = metadata.schemas_iter().count();
    let field_count = metadata.current_schema().as_struct().fields().len();

    println!("  ✓ Metadata loaded");
    println!("  ✓ Schema versions: {}", schema_count);
    println!("  ✓ Current schema fields: {}", field_count);

    assert_eq!(schema_count, 2, "Expected 2 schema versions");
    assert_eq!(field_count, 3, "Expected 3 fields after evolution");

    Ok(())
}

#[test]
#[ignore]
fn test_load_all_data_types() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Test 8: Load table with all data types ===");

    let metadata = load_table_metadata("test8_all_data_types")?;

    let field_count = metadata.current_schema().as_struct().fields().len();
    println!("  ✓ Metadata loaded");
    println!("  ✓ Fields: {}", field_count);

    assert_eq!(field_count, 10, "Expected 10 fields");

    // Verify specific columns exist
    let schema = metadata.current_schema();
    assert!(schema.field_by_name("id").is_some());
    assert!(schema.field_by_name("bool_col").is_some());
    assert!(schema.field_by_name("timestamp_col").is_some());

    Ok(())
}

#[test]
#[ignore]
fn test_all_tables_load() {
    println!("\n=== Loading All Java Tables ===\n");

    let tables = vec![
        "test1_basic_table",
        "test2_partitioned_table",
        "test3_position_deletes",
        "test4_multiple_snapshots",
        "test5_after_delete",
        "test6_after_overwrite",
        "test7_schema_evolution",
        "test8_all_data_types",
    ];

    let mut passed = 0;
    let mut failed = 0;

    for table in &tables {
        match load_table_metadata(table) {
            Ok(metadata) => {
                println!("✅ {} - {} fields, {} snapshots",
                    table,
                    metadata.current_schema().as_struct().fields().len(),
                    metadata.snapshots().count()
                );
                passed += 1;
            }
            Err(e) => {
                println!("❌ {} - Error: {}", table, e);
                failed += 1;
            }
        }
    }

    println!("\n=== Results ===");
    println!("Passed: {}/{}", passed, tables.len());
    println!("Failed: {}", failed);

    assert_eq!(failed, 0, "Some tables failed to load");
}
