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

//! This module provides `PositionDeleteFileWriter`.
//!
//! Position delete files mark specific rows in data files for deletion by
//! storing the file path and row position. This is the foundation for
//! implementing DELETE, UPDATE, and MERGE operations in Iceberg.
//!
//! # Schema
//!
//! Position delete files must have at least two columns:
//! - `file_path` (STRING, field id 2147483546): Path to the data file
//! - `pos` (LONG, field id 2147483545): Row position in the data file
//!
//! Optionally, the schema can include additional columns from the deleted row
//! for debugging and auditing purposes.
//!
//! # Example
//!
//! ```rust,no_run
//! use iceberg::spec::{DataFile, PartitionKey};
//! use iceberg::writer::base_writer::position_delete_writer::PositionDeleteFileWriterBuilder;
//! use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
//! # use iceberg::Result;
//!
//! # async fn example() -> Result<()> {
//! // Create writer (requires rolling file writer, location generator, etc.)
//! # let builder = todo!("Create PositionDeleteFileWriterBuilder");
//! let mut writer = builder.build(None).await?;
//!
//! // Write deletes for a specific file
//! let file_path = "/path/to/data/file.parquet";
//! let positions = vec![10, 25, 42]; // Row positions to delete
//!
//! writer.write_deletes(file_path, positions.into_iter()).await?;
//!
//! // Close and get delete files
//! let delete_files: Vec<DataFile> = writer.close().await?;
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::spec::{DataContentType, DataFile, PartitionKey};
use crate::writer::file_writer::FileWriterBuilder;
use crate::writer::file_writer::location_generator::{FileNameGenerator, LocationGenerator};
use crate::writer::file_writer::rolling_writer::{RollingFileWriter, RollingFileWriterBuilder};
use crate::writer::{CurrentFileStatus, IcebergWriter, IcebergWriterBuilder};
use crate::{Error, ErrorKind, Result};

/// Field ID for the file_path column in position delete files.
/// This is a reserved field ID as per the Iceberg spec.
pub const FILE_PATH_FIELD_ID: i32 = 2147483546;

/// Field ID for the pos (position) column in position delete files.
/// This is a reserved field ID as per the Iceberg spec.
pub const POS_FIELD_ID: i32 = 2147483545;

/// Field name for the file path column.
pub const FILE_PATH_COLUMN_NAME: &str = "file_path";

/// Field name for the position column.
pub const POS_COLUMN_NAME: &str = "pos";

/// Builder for `PositionDeleteFileWriter`.
///
/// This builder configures and creates position delete file writers
/// that write delete files marking specific rows for deletion.
#[derive(Clone, Debug)]
pub struct PositionDeleteFileWriterBuilder<
    B: FileWriterBuilder,
    L: LocationGenerator,
    F: FileNameGenerator,
> {
    inner: RollingFileWriterBuilder<B, L, F>,
}

impl<B, L, F> PositionDeleteFileWriterBuilder<B, L, F>
where
    B: FileWriterBuilder,
    L: LocationGenerator,
    F: FileNameGenerator,
{
    /// Create a new `PositionDeleteFileWriterBuilder` using a `RollingFileWriterBuilder`.
    ///
    /// The rolling file writer should be configured with the position delete schema,
    /// which can be created using `position_delete_schema()` or
    /// `position_delete_schema_with_row_schema()`.
    pub fn new(inner: RollingFileWriterBuilder<B, L, F>) -> Self {
        Self { inner }
    }
}

#[async_trait::async_trait]
impl<B, L, F> IcebergWriterBuilder for PositionDeleteFileWriterBuilder<B, L, F>
where
    B: FileWriterBuilder,
    L: LocationGenerator,
    F: FileNameGenerator,
{
    type R = PositionDeleteFileWriter<B, L, F>;

    async fn build(self, partition_key: Option<PartitionKey>) -> Result<Self::R> {
        Ok(PositionDeleteFileWriter {
            inner: Some(self.inner.clone().build()),
            partition_key,
            delete_cache: HashMap::new(),
        })
    }
}

/// Writer for position delete files.
///
/// This writer accumulates position deletes and writes them to Parquet files.
/// Position deletes specify which rows to delete by recording the file path
/// and row position.
///
/// # Thread Safety
///
/// This writer is NOT thread-safe. Use separate writer instances for
/// concurrent writes.
#[derive(Debug)]
pub struct PositionDeleteFileWriter<
    B: FileWriterBuilder,
    L: LocationGenerator,
    F: FileNameGenerator,
> {
    inner: Option<RollingFileWriter<B, L, F>>,
    partition_key: Option<PartitionKey>,
    /// Cache of deletes grouped by file path for efficient batch writing
    delete_cache: HashMap<String, Vec<i64>>,
}

impl<B, L, F> PositionDeleteFileWriter<B, L, F>
where
    B: FileWriterBuilder,
    L: LocationGenerator,
    F: FileNameGenerator,
{
    /// Write position deletes for a specific data file.
    ///
    /// # Arguments
    ///
    /// * `file_path` - Path to the data file containing rows to delete
    /// * `positions` - Iterator of row positions (0-based) to mark for deletion
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use iceberg::Result;
    /// # async fn example(mut writer: iceberg::writer::base_writer::position_delete_writer::PositionDeleteFileWriter<impl iceberg::writer::file_writer::FileWriterBuilder, impl iceberg::writer::file_writer::location_generator::LocationGenerator, impl iceberg::writer::file_writer::location_generator::FileNameGenerator>) -> Result<()> {
    /// let file_path = "/warehouse/db/table/data/file1.parquet";
    /// let positions_to_delete = vec![0, 5, 10, 100];
    ///
    /// writer.write_deletes(file_path, positions_to_delete.into_iter()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn write_deletes(
        &mut self,
        file_path: &str,
        positions: impl Iterator<Item = i64>,
    ) -> Result<()> {
        // Cache deletes grouped by file path
        let entry = self.delete_cache.entry(file_path.to_string()).or_default();
        entry.extend(positions);

        // Flush if cache gets too large (prevent OOM)
        if self.delete_cache.values().map(|v| v.len()).sum::<usize>() > 100_000 {
            self.flush_cache().await?;
        }

        Ok(())
    }

    /// Flush cached deletes to the underlying writer.
    async fn flush_cache(&mut self) -> Result<()> {
        if self.delete_cache.is_empty() {
            return Ok(());
        }

        let writer = self.inner.as_mut().ok_or_else(|| {
            Error::new(ErrorKind::Unexpected, "Writer is not initialized!")
        })?;

        // Collect all file paths and positions
        let mut all_file_paths = Vec::new();
        let mut all_positions = Vec::new();

        for (file_path, positions) in self.delete_cache.drain() {
            let count = positions.len();
            all_file_paths.extend(vec![file_path; count]);
            all_positions.extend(positions);
        }

        if all_file_paths.is_empty() {
            return Ok(());
        }

        // Create RecordBatch with file_path and pos columns
        let file_path_array = StringArray::from(all_file_paths);
        let pos_array = Int64Array::from(all_positions);

        let schema = position_delete_schema();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(file_path_array) as ArrayRef,
                Arc::new(pos_array) as ArrayRef,
            ],
        )
        .map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Failed to create position delete RecordBatch: {e}"),
            )
        })?;

        writer.write(&self.partition_key, &batch).await
    }
}

#[async_trait::async_trait]
impl<B, L, F> IcebergWriter for PositionDeleteFileWriter<B, L, F>
where
    B: FileWriterBuilder,
    L: LocationGenerator,
    F: FileNameGenerator,
{
    /// Write a pre-constructed RecordBatch of position deletes.
    ///
    /// The batch must conform to the position delete schema with
    /// `file_path` and `pos` columns.
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        let writer = self.inner.as_mut().ok_or_else(|| {
            Error::new(ErrorKind::Unexpected, "Writer is not initialized!")
        })?;

        writer.write(&self.partition_key, &batch).await
    }

    async fn close(&mut self) -> Result<Vec<DataFile>> {
        // Flush any remaining cached deletes
        self.flush_cache().await?;

        if let Some(writer) = self.inner.take() {
            writer
                .close()
                .await?
                .into_iter()
                .map(|mut res| {
                    // Set content type to PositionDeletes
                    res.content(DataContentType::PositionDeletes);

                    // Set partition information if available
                    if let Some(pk) = self.partition_key.as_ref() {
                        res.partition(pk.data().clone());
                        res.partition_spec_id(pk.spec().spec_id());
                    }

                    res.build().map_err(|e| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            format!("Failed to build position delete file: {e}"),
                        )
                    })
                })
                .collect()
        } else {
            Err(Error::new(
                ErrorKind::Unexpected,
                "Position delete file writer has been closed.",
            ))
        }
    }
}

impl<B, L, F> CurrentFileStatus for PositionDeleteFileWriter<B, L, F>
where
    B: FileWriterBuilder,
    L: LocationGenerator,
    F: FileNameGenerator,
{
    fn current_file_path(&self) -> String {
        self.inner.as_ref().unwrap().current_file_path()
    }

    fn current_row_num(&self) -> usize {
        self.inner.as_ref().unwrap().current_row_num()
    }

    fn current_written_size(&self) -> usize {
        self.inner.as_ref().unwrap().current_written_size()
    }
}

/// Create the standard position delete schema with file_path and pos columns.
///
/// This schema follows the Iceberg specification for position delete files:
/// - `file_path` (STRING, field id 2147483546): Path to the data file
/// - `pos` (LONG, field id 2147483545): Row position in the data file
///
/// # Returns
///
/// An Arrow schema suitable for writing position delete files.
pub fn position_delete_schema() -> ArrowSchemaRef {
    let mut file_path_metadata = HashMap::new();
    file_path_metadata.insert(
        PARQUET_FIELD_ID_META_KEY.to_string(),
        FILE_PATH_FIELD_ID.to_string(),
    );

    let mut pos_metadata = HashMap::new();
    pos_metadata.insert(
        PARQUET_FIELD_ID_META_KEY.to_string(),
        POS_FIELD_ID.to_string(),
    );

    Arc::new(ArrowSchema::new(vec![
        Field::new(FILE_PATH_COLUMN_NAME, DataType::Utf8, false).with_metadata(file_path_metadata),
        Field::new(POS_COLUMN_NAME, DataType::Int64, false).with_metadata(pos_metadata),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    use crate::arrow::arrow_schema_to_schema;
    use crate::io::FileIOBuilder;
    use crate::spec::{DataContentType, DataFileFormat};
    use crate::writer::file_writer::ParquetWriterBuilder;
    use crate::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };
    use crate::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
    use crate::writer::{IcebergWriter, IcebergWriterBuilder};

    #[test]
    fn test_position_delete_schema() {
        let schema = position_delete_schema();

        assert_eq!(schema.fields().len(), 2);

        // Check file_path field
        let file_path_field = &schema.fields()[0];
        assert_eq!(file_path_field.name(), FILE_PATH_COLUMN_NAME);
        assert_eq!(file_path_field.data_type(), &DataType::Utf8);
        assert!(!file_path_field.is_nullable());
        assert_eq!(
            file_path_field
                .metadata()
                .get(PARQUET_FIELD_ID_META_KEY)
                .unwrap(),
            &FILE_PATH_FIELD_ID.to_string()
        );

        // Check pos field
        let pos_field = &schema.fields()[1];
        assert_eq!(pos_field.name(), POS_COLUMN_NAME);
        assert_eq!(pos_field.data_type(), &DataType::Int64);
        assert!(!pos_field.is_nullable());
        assert_eq!(
            pos_field
                .metadata()
                .get(PARQUET_FIELD_ID_META_KEY)
                .unwrap(),
            &POS_FIELD_ID.to_string()
        );
    }

    #[test]
    fn test_field_ids_are_reserved() {
        // These field IDs are specified in the Iceberg spec as reserved
        // for position delete files
        assert_eq!(FILE_PATH_FIELD_ID, 2147483546);
        assert_eq!(POS_FIELD_ID, 2147483545);
    }

    #[tokio::test]
    async fn test_position_delete_writer_basic() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        let arrow_schema = position_delete_schema();
        let schema = Arc::new(arrow_schema_to_schema(&arrow_schema).unwrap());

        let pw = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            schema.clone(),
        );

        let rolling_file_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            pw,
            file_io.clone(),
            location_gen,
            file_name_gen,
        );

        let mut delete_writer = PositionDeleteFileWriterBuilder::new(rolling_file_writer_builder)
            .build(None)
            .await?;

        // Write deletes for a specific file
        let file_path = "/warehouse/db/table/data/file1.parquet";
        let positions = vec![0_i64, 5, 10, 100];

        delete_writer
            .write_deletes(file_path, positions.clone().into_iter())
            .await?;

        let delete_files = delete_writer.close().await?;
        assert_eq!(delete_files.len(), 1);

        let delete_file = &delete_files[0];
        assert_eq!(delete_file.file_format, DataFileFormat::Parquet);
        assert_eq!(delete_file.content, DataContentType::PositionDeletes);
        assert_eq!(delete_file.record_count, 4); // 4 deletes written

        // Verify the written Parquet file
        let input_file = file_io.new_input(delete_file.file_path.clone())?;
        let input_content = input_file.read().await?;

        let parquet_reader =
            ParquetRecordBatchReaderBuilder::try_new(input_content)
                .expect("Failed to create Parquet reader");

        let mut batches = Vec::new();
        for batch in parquet_reader.build()? {
            batches.push(batch?);
        }

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 4);
        assert_eq!(batch.num_columns(), 2);

        // Verify file_path column
        let file_path_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(file_path_col.value(0), file_path);
        assert_eq!(file_path_col.value(1), file_path);
        assert_eq!(file_path_col.value(2), file_path);
        assert_eq!(file_path_col.value(3), file_path);

        // Verify pos column
        let pos_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(pos_col.value(0), 0);
        assert_eq!(pos_col.value(1), 5);
        assert_eq!(pos_col.value(2), 10);
        assert_eq!(pos_col.value(3), 100);

        Ok(())
    }

    #[tokio::test]
    async fn test_position_delete_writer_multiple_files() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        let arrow_schema = position_delete_schema();
        let schema = Arc::new(arrow_schema_to_schema(&arrow_schema).unwrap());

        let pw = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            schema.clone(),
        );

        let rolling_file_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            pw,
            file_io.clone(),
            location_gen,
            file_name_gen,
        );

        let mut delete_writer = PositionDeleteFileWriterBuilder::new(rolling_file_writer_builder)
            .build(None)
            .await?;

        // Write deletes for multiple data files
        delete_writer
            .write_deletes(
                "/warehouse/db/table/data/file1.parquet",
                vec![0_i64, 1, 2].into_iter(),
            )
            .await?;

        delete_writer
            .write_deletes(
                "/warehouse/db/table/data/file2.parquet",
                vec![5_i64, 10, 15].into_iter(),
            )
            .await?;

        delete_writer
            .write_deletes(
                "/warehouse/db/table/data/file3.parquet",
                vec![100_i64].into_iter(),
            )
            .await?;

        let delete_files = delete_writer.close().await?;
        assert_eq!(delete_files.len(), 1);

        let delete_file = &delete_files[0];
        assert_eq!(delete_file.record_count, 7); // 3 + 3 + 1 = 7 total deletes

        // Verify the content
        let input_file = file_io.new_input(delete_file.file_path.clone())?;
        let input_content = input_file.read().await?;

        let parquet_reader =
            ParquetRecordBatchReaderBuilder::try_new(input_content)
                .expect("Failed to create Parquet reader");

        let batches: Vec<RecordBatch> = parquet_reader
            .build()?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 7);

        Ok(())
    }

    #[tokio::test]
    async fn test_position_delete_writer_empty() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        let arrow_schema = position_delete_schema();
        let schema = Arc::new(arrow_schema_to_schema(&arrow_schema).unwrap());

        let pw = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            schema.clone(),
        );

        let rolling_file_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            pw,
            file_io.clone(),
            location_gen,
            file_name_gen,
        );

        let mut delete_writer = PositionDeleteFileWriterBuilder::new(rolling_file_writer_builder)
            .build(None)
            .await?;

        // Close without writing any deletes
        let delete_files = delete_writer.close().await?;

        // Should create no files if no deletes were written
        assert_eq!(delete_files.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_position_delete_writer_large_batch() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        let arrow_schema = position_delete_schema();
        let schema = Arc::new(arrow_schema_to_schema(&arrow_schema).unwrap());

        let pw = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            schema.clone(),
        );

        let rolling_file_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            pw,
            file_io.clone(),
            location_gen,
            file_name_gen,
        );

        let mut delete_writer = PositionDeleteFileWriterBuilder::new(rolling_file_writer_builder)
            .build(None)
            .await?;

        // Write a large number of deletes
        let file_path = "/warehouse/db/table/data/large_file.parquet";
        let positions: Vec<i64> = (0..10000).collect();

        delete_writer
            .write_deletes(file_path, positions.into_iter())
            .await?;

        let delete_files = delete_writer.close().await?;
        assert_eq!(delete_files.len(), 1);

        let delete_file = &delete_files[0];
        assert_eq!(delete_file.record_count, 10000);

        Ok(())
    }

    #[tokio::test]
    async fn test_position_delete_schema_field_ids() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        let arrow_schema = position_delete_schema();
        let schema = Arc::new(arrow_schema_to_schema(&arrow_schema).unwrap());

        let pw = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            schema.clone(),
        );

        let rolling_file_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            pw,
            file_io.clone(),
            location_gen,
            file_name_gen,
        );

        let mut delete_writer = PositionDeleteFileWriterBuilder::new(rolling_file_writer_builder)
            .build(None)
            .await?;

        delete_writer
            .write_deletes("/test/file.parquet", vec![0_i64].into_iter())
            .await?;

        let delete_files = delete_writer.close().await?;
        assert_eq!(delete_files.len(), 1);

        // Verify field IDs in written Parquet file
        let input_file = file_io.new_input(delete_files[0].file_path.clone())?;
        let input_content = input_file.read().await?;

        let parquet_metadata =
            ArrowReaderMetadata::load(&input_content, ArrowReaderOptions::default())
                .expect("Failed to load Parquet metadata");

        let field_ids: Vec<i32> = parquet_metadata
            .parquet_schema()
            .columns()
            .iter()
            .map(|col| col.self_type().get_basic_info().id())
            .collect();

        // Verify the reserved field IDs are used
        assert_eq!(field_ids, vec![FILE_PATH_FIELD_ID, POS_FIELD_ID]);

        Ok(())
    }
}
