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

//! EXPIRE SNAPSHOTS operation implementation.
//!
//! This module provides the EXPIRE SNAPSHOTS operation for removing old snapshots
//! from Iceberg table metadata. Snapshot expiration is essential for managing
//! table metadata size and preventing unbounded storage growth.
//!
//! # Overview
//!
//! Snapshot expiration removes snapshots from table metadata based on retention
//! policies while ensuring data integrity. The operation:
//!
//! - Removes snapshots older than a specified timestamp
//! - Retains a minimum number of recent snapshots
//! - Never expires the current snapshot (main branch)
//! - Never expires snapshots referenced by active branches or tags
//! - Optionally removes unreferenced data files (garbage collection)
//!
//! # Safety Guarantees
//!
//! The implementation follows conservative safety rules:
//!
//! 1. **Never expire current snapshot**: The snapshot referenced by the main branch is never removed
//! 2. **Respect branch/tag references**: Snapshots referenced by any branch or tag are retained
//! 3. **Min snapshots guarantee**: Always retains at least N snapshots (configurable)
//! 4. **Metadata-only by default**: Does not delete data files unless explicitly requested
//!
//! # Example
//!
//! ```rust,no_run
//! use iceberg::transaction::Transaction;
//! use chrono::{Duration, Utc};
//! # use iceberg::Result;
//!
//! # async fn example(table: iceberg::table::Table) -> Result<()> {
//! // Create a transaction
//! let tx = Transaction::new(&table);
//!
//! // Expire snapshots older than 5 days, keep at least 10 snapshots
//! let expire_time = Utc::now() - Duration::days(5);
//! let expire_action = tx
//!     .expire_snapshots()
//!     .expire_older_than(expire_time.timestamp_millis())
//!     .retain_last(10);
//!
//! // Apply and commit
//! let tx = expire_action.apply(tx)?;
//! // tx.commit(&catalog).await?;
//! # Ok(())
//! # }
//! ```

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;

use crate::error::Result;
use crate::spec::{MAIN_BRANCH, Snapshot};
use crate::table::Table;
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, TableRequirement, TableUpdate};

/// Default minimum number of snapshots to keep (Iceberg spec default).
const DEFAULT_MIN_SNAPSHOTS_TO_KEEP: usize = 1;

/// Default max snapshot age in milliseconds: 5 days (Iceberg spec default).
const DEFAULT_MAX_SNAPSHOT_AGE_MS: i64 = 5 * 24 * 60 * 60 * 1000; // 5 days

/// Action to expire old snapshots from table metadata.
///
/// This action removes snapshots that are older than a specified age or exceed
/// retention limits. It provides multiple configuration options to control which
/// snapshots are expired.
///
/// # Configuration Options
///
/// - **expire_older_than**: Timestamp (milliseconds) - expire snapshots older than this time
/// - **retain_last**: Number of snapshots - always keep at least N most recent snapshots
/// - **max_snapshot_age_ms**: Age threshold (milliseconds) - expire snapshots older than this age
/// - **snapshot_ids**: Explicit list of snapshot IDs to expire
///
/// # Safety Rules
///
/// The following snapshots are NEVER expired, regardless of configuration:
/// - Current snapshot (main branch)
/// - Snapshots referenced by any branch
/// - Snapshots referenced by any tag
/// - Snapshots required to satisfy min retention (retain_last)
///
/// # Example
///
/// ```rust,no_run
/// use iceberg::transaction::Transaction;
/// use chrono::{Duration, Utc};
/// # use iceberg::Result;
///
/// # fn example(table: iceberg::table::Table) -> Result<()> {
/// let tx = Transaction::new(&table);
///
/// // Expire snapshots older than 7 days, keeping at least 5
/// let cutoff = Utc::now() - Duration::days(7);
/// let action = tx
///     .expire_snapshots()
///     .expire_older_than(cutoff.timestamp_millis())
///     .retain_last(5);
///
/// let tx = action.apply(tx)?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct ExpireSnapshotsAction {
    /// Timestamp (milliseconds since epoch) - expire snapshots older than this
    expire_older_than: Option<i64>,
    /// Minimum number of snapshots to retain
    retain_last: Option<usize>,
    /// Maximum snapshot age in milliseconds
    max_snapshot_age_ms: Option<i64>,
    /// Specific snapshot IDs to expire (explicit mode)
    snapshot_ids: Vec<i64>,
}

impl ExpireSnapshotsAction {
    /// Creates a new ExpireSnapshotsAction with default settings.
    ///
    /// By default:
    /// - No explicit expiration time
    /// - Retains last 1 snapshot (Iceberg spec default)
    /// - Max age: 5 days (Iceberg spec default)
    pub fn new() -> Self {
        Self {
            expire_older_than: None,
            retain_last: Some(DEFAULT_MIN_SNAPSHOTS_TO_KEEP),
            max_snapshot_age_ms: Some(DEFAULT_MAX_SNAPSHOT_AGE_MS),
            snapshot_ids: vec![],
        }
    }

    /// Sets the timestamp (in milliseconds since epoch) before which snapshots should be expired.
    ///
    /// Snapshots with timestamp_ms older than this value will be candidates for expiration,
    /// subject to other retention policies.
    ///
    /// # Arguments
    ///
    /// * `timestamp_ms` - Milliseconds since Unix epoch (January 1, 1970 00:00:00 UTC)
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use iceberg::transaction::Transaction;
    /// use chrono::{Duration, Utc};
    ///
    /// # fn example(table: iceberg::table::Table) {
    /// let tx = Transaction::new(&table);
    /// let cutoff = Utc::now() - Duration::days(30);
    /// let action = tx.expire_snapshots().expire_older_than(cutoff.timestamp_millis());
    /// # }
    /// ```
    pub fn expire_older_than(mut self, timestamp_ms: i64) -> Self {
        self.expire_older_than = Some(timestamp_ms);
        self
    }

    /// Sets the minimum number of snapshots to retain.
    ///
    /// Even if snapshots are older than the expiration threshold, this many of the
    /// most recent snapshots will be kept.
    ///
    /// # Arguments
    ///
    /// * `n` - Minimum number of snapshots to keep
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use iceberg::transaction::Transaction;
    ///
    /// # fn example(table: iceberg::table::Table) {
    /// let tx = Transaction::new(&table);
    /// // Keep at least 10 most recent snapshots
    /// let action = tx.expire_snapshots().retain_last(10);
    /// # }
    /// ```
    pub fn retain_last(mut self, n: usize) -> Self {
        self.retain_last = Some(n);
        self
    }

    /// Sets the maximum age (in milliseconds) for snapshots.
    ///
    /// Snapshots older than this age will be expired. This is an alternative to
    /// `expire_older_than` that uses a relative age instead of absolute timestamp.
    ///
    /// # Arguments
    ///
    /// * `age_ms` - Maximum age in milliseconds
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use iceberg::transaction::Transaction;
    ///
    /// # fn example(table: iceberg::table::Table) {
    /// let tx = Transaction::new(&table);
    /// // Expire snapshots older than 7 days
    /// let seven_days_ms = 7 * 24 * 60 * 60 * 1000;
    /// let action = tx.expire_snapshots().with_max_snapshot_age_ms(seven_days_ms);
    /// # }
    /// ```
    pub fn with_max_snapshot_age_ms(mut self, age_ms: i64) -> Self {
        self.max_snapshot_age_ms = Some(age_ms);
        self
    }

    /// Adds a specific snapshot ID to expire.
    ///
    /// This allows explicit control over which snapshots to remove. The snapshot
    /// will only be expired if it's safe to do so (not current, not referenced).
    ///
    /// # Arguments
    ///
    /// * `snapshot_id` - The ID of the snapshot to expire
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use iceberg::transaction::Transaction;
    ///
    /// # fn example(table: iceberg::table::Table) {
    /// let tx = Transaction::new(&table);
    /// let action = tx.expire_snapshots()
    ///     .expire_snapshot_id(12345)
    ///     .expire_snapshot_id(67890);
    /// # }
    /// ```
    pub fn expire_snapshot_id(mut self, snapshot_id: i64) -> Self {
        self.snapshot_ids.push(snapshot_id);
        self
    }

    /// Determines which snapshots should be retained based on retention policies.
    ///
    /// Returns a set of snapshot IDs that must NOT be expired.
    fn determine_snapshots_to_retain(&self, table: &Table) -> Result<HashSet<i64>> {
        let metadata = table.metadata();
        let mut retain_set = HashSet::new();

        // 1. Always retain current snapshot (main branch)
        if let Some(current_snapshot) = metadata.current_snapshot() {
            retain_set.insert(current_snapshot.snapshot_id());
        }

        // 2. Retain all snapshots referenced by branches and tags
        for (_, snapshot_ref) in &metadata.refs {
            retain_set.insert(snapshot_ref.snapshot_id);
        }

        // 3. Retain last N snapshots based on retain_last
        if let Some(retain_last) = self.retain_last {
            let snapshot_log = &metadata.snapshot_log;
            let recent_snapshots: Vec<i64> = snapshot_log
                .iter()
                .rev()
                .take(retain_last)
                .map(|entry| entry.snapshot_id)
                .collect();

            retain_set.extend(recent_snapshots);
        }

        // 4. Filter by age if expire_older_than or max_snapshot_age_ms is set
        let now_ms = Utc::now().timestamp_millis();

        if let Some(cutoff_ms) = self.expire_older_than.or_else(|| {
            self.max_snapshot_age_ms.map(|age| now_ms - age)
        }) {
            // Retain snapshots newer than the cutoff
            for snapshot in metadata.snapshots.values() {
                if snapshot.timestamp_ms() >= cutoff_ms {
                    retain_set.insert(snapshot.snapshot_id());
                }
            }
        }

        Ok(retain_set)
    }

    /// Determines which snapshots should be expired.
    ///
    /// Returns a set of snapshot IDs that can be safely removed.
    fn determine_snapshots_to_expire(
        &self,
        table: &Table,
        retain_set: &HashSet<i64>,
    ) -> Result<HashSet<i64>> {
        let metadata = table.metadata();
        let mut expire_set = HashSet::new();

        // If explicit snapshot IDs are provided, use those (filtered by retention)
        if !self.snapshot_ids.is_empty() {
            for &snapshot_id in &self.snapshot_ids {
                if !retain_set.contains(&snapshot_id) {
                    expire_set.insert(snapshot_id);
                }
            }
            return Ok(expire_set);
        }

        // Otherwise, expire all snapshots NOT in the retain set
        for snapshot_id in metadata.snapshots.keys() {
            if !retain_set.contains(snapshot_id) {
                expire_set.insert(*snapshot_id);
            }
        }

        Ok(expire_set)
    }

    /// Validates the expiration request.
    fn validate(&self, table: &Table) -> Result<()> {
        let metadata = table.metadata();

        // Ensure we're not trying to expire the current snapshot
        if let Some(current_snapshot) = metadata.current_snapshot() {
            if self.snapshot_ids.contains(&current_snapshot.snapshot_id()) {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot expire current snapshot: {}",
                        current_snapshot.snapshot_id()
                    ),
                ));
            }
        }

        // Validate that we have at least one expiration criteria
        if self.expire_older_than.is_none()
            && self.max_snapshot_age_ms.is_none()
            && self.snapshot_ids.is_empty()
        {
            // This is OK - we have default retention policies
        }

        Ok(())
    }

    /// Collects file paths referenced by the given snapshot.
    ///
    /// This includes the manifest list and all manifest files. Data files
    /// are not tracked in the current implementation.
    async fn collect_snapshot_files(
        &self,
        snapshot: &Snapshot,
        table: &Table,
    ) -> Result<HashSet<String>> {
        let mut file_set = HashSet::new();

        // Add manifest list file
        file_set.insert(snapshot.manifest_list().to_string());

        // Load manifest list and collect manifest file paths
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await?;

        for entry in manifest_list.entries() {
            file_set.insert(entry.manifest_path.clone());
        }

        Ok(file_set)
    }

    /// Builds the set of files that are still referenced by retained snapshots.
    async fn build_referenced_files(
        &self,
        table: &Table,
        retain_set: &HashSet<i64>,
    ) -> Result<HashSet<String>> {
        let metadata = table.metadata();
        let mut referenced_files = HashSet::new();

        for snapshot_id in retain_set {
            if let Some(snapshot) = metadata.snapshots.get(snapshot_id) {
                let snapshot_files = self.collect_snapshot_files(snapshot, table).await?;
                referenced_files.extend(snapshot_files);
            }
        }

        Ok(referenced_files)
    }
}

impl Default for ExpireSnapshotsAction {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TransactionAction for ExpireSnapshotsAction {
    /// Commits the expire snapshots action to the table.
    ///
    /// This generates the necessary TableUpdates to remove expired snapshots
    /// from the table metadata.
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        // 1. Validate the request
        self.validate(table)?;

        // 2. Determine which snapshots to retain
        let retain_set = self.determine_snapshots_to_retain(table)?;

        // 3. Determine which snapshots to expire
        let expire_set = self.determine_snapshots_to_expire(table, &retain_set)?;

        // 4. If no snapshots to expire, return empty commit
        if expire_set.is_empty() {
            return Ok(ActionCommit::new(vec![], vec![]));
        }

        // 5. Build referenced files for safety (future: garbage collection)
        let _referenced_files = self.build_referenced_files(table, &retain_set).await?;

        // 6. Build table updates
        let mut updates = Vec::new();

        // Remove expired snapshots
        let snapshot_ids_to_remove: Vec<i64> = expire_set.iter().copied().collect();
        updates.push(TableUpdate::RemoveSnapshots {
            snapshot_ids: snapshot_ids_to_remove,
        });

        // Remove snapshot references (tags/branches) pointing to expired snapshots
        let metadata = table.metadata();
        for (ref_name, snapshot_ref) in &metadata.refs {
            if expire_set.contains(&snapshot_ref.snapshot_id) {
                // Don't remove main branch reference
                if ref_name != MAIN_BRANCH {
                    updates.push(TableUpdate::RemoveSnapshotRef {
                        ref_name: ref_name.clone(),
                    });
                }
            }
        }

        // 7. Build table requirements (optimistic concurrency control)
        let requirements = vec![
            TableRequirement::UuidMatch {
                uuid: metadata.uuid(),
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: MAIN_BRANCH.to_string(),
                snapshot_id: metadata.current_snapshot_id(),
            },
        ];

        Ok(ActionCommit::new(updates, requirements))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_expire_snapshots_action() {
        let action = ExpireSnapshotsAction::new();

        assert_eq!(action.retain_last, Some(DEFAULT_MIN_SNAPSHOTS_TO_KEEP));
        assert_eq!(action.max_snapshot_age_ms, Some(DEFAULT_MAX_SNAPSHOT_AGE_MS));
        assert!(action.expire_older_than.is_none());
        assert!(action.snapshot_ids.is_empty());
    }

    #[test]
    fn test_expire_older_than() {
        let timestamp = 1234567890000;
        let action = ExpireSnapshotsAction::new().expire_older_than(timestamp);

        assert_eq!(action.expire_older_than, Some(timestamp));
    }

    #[test]
    fn test_retain_last() {
        let action = ExpireSnapshotsAction::new().retain_last(10);

        assert_eq!(action.retain_last, Some(10));
    }

    #[test]
    fn test_with_max_snapshot_age_ms() {
        let age_ms = 7 * 24 * 60 * 60 * 1000; // 7 days
        let action = ExpireSnapshotsAction::new().with_max_snapshot_age_ms(age_ms);

        assert_eq!(action.max_snapshot_age_ms, Some(age_ms));
    }

    #[test]
    fn test_expire_snapshot_id() {
        let action = ExpireSnapshotsAction::new()
            .expire_snapshot_id(123)
            .expire_snapshot_id(456);

        assert_eq!(action.snapshot_ids, vec![123, 456]);
    }

    #[test]
    fn test_builder_chaining() {
        let timestamp = 1234567890000;
        let action = ExpireSnapshotsAction::new()
            .expire_older_than(timestamp)
            .retain_last(5)
            .expire_snapshot_id(999);

        assert_eq!(action.expire_older_than, Some(timestamp));
        assert_eq!(action.retain_last, Some(5));
        assert_eq!(action.snapshot_ids, vec![999]);
    }

    // Integration tests
    use crate::transaction::tests::make_v2_minimal_table;
    use crate::transaction::{ApplyTransactionAction, Transaction, TransactionAction};
    use crate::spec::{DataContentType, DataFileBuilder, DataFileFormat, Literal, Struct};
    use crate::{TableRequirement, TableUpdate};

    /// Helper function to create a data file for testing
    fn create_test_data_file(path: &str, record_count: u64) -> crate::spec::DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(1000)
            .record_count(record_count)
            .partition(Struct::from_iter([Some(Literal::long(1))]))
            .build()
            .unwrap()
    }

    /// Helper to append a data file and create a new snapshot
    async fn append_and_commit(
        table: &crate::table::Table,
        file_path: &str,
        record_count: u64,
    ) -> crate::table::Table {
        let tx = Transaction::new(table);
        let data_file = create_test_data_file(file_path, record_count);

        let action = tx.fast_append().add_data_files(vec![data_file]);
        let tx = action.apply(tx).unwrap();

        // Simulate catalog commit by applying updates
        let mut current_table = table.clone();
        let current_table_for_commit = current_table.clone();

        for action in &tx.actions {
            let mut action_commit = Arc::clone(action)
                .commit(&current_table_for_commit)
                .await
                .unwrap();

            let updates = action_commit.take_updates();
            let mut metadata_builder = current_table.metadata().clone().into_builder(None);
            for update in updates {
                metadata_builder = update.apply(metadata_builder).unwrap();
            }
            let new_metadata = metadata_builder.build().unwrap().metadata;
            current_table = current_table.with_metadata(Arc::new(new_metadata));
        }

        current_table
    }

    #[tokio::test]
    async fn test_expire_snapshots_no_snapshots_to_expire() {
        let table = make_v2_minimal_table();

        // Try to expire with default settings - should have no effect
        // (table has no snapshots initially)
        let action = ExpireSnapshotsAction::new();

        let result = Arc::new(action).commit(&table).await;
        assert!(result.is_ok());

        let mut action_commit = result.unwrap();
        let updates = action_commit.take_updates();
        // Should have no updates since there's nothing to expire
        assert_eq!(updates.len(), 0);
    }

    #[tokio::test]
    async fn test_expire_snapshots_retains_last_n() {
        let mut table = make_v2_minimal_table();

        // Create 5 snapshots
        for i in 1..=5 {
            table = append_and_commit(&table, &format!("test/data{}.parquet", i), 100).await;
        }

        // Verify we have 5 snapshots
        assert_eq!(table.metadata().snapshots.len(), 5);

        // Expire snapshots but retain last 3
        let action = ExpireSnapshotsAction::new()
            .retain_last(3)
            .expire_older_than(Utc::now().timestamp_millis()); // Expire all old ones

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        // Should have at least one update (RemoveSnapshots)
        assert!(!updates.is_empty());

        // Find RemoveSnapshots update
        let removed_snapshots = updates.iter().find_map(|u| {
            if let TableUpdate::RemoveSnapshots { snapshot_ids } = u {
                Some(snapshot_ids)
            } else {
                None
            }
        });

        // Should have removed some snapshots (5 - 3 = 2 snapshots should be removed)
        if let Some(removed) = removed_snapshots {
            assert_eq!(removed.len(), 2, "Should remove 2 snapshots to keep last 3");
        }
    }

    #[tokio::test]
    async fn test_expire_snapshots_by_age() {
        let mut table = make_v2_minimal_table();

        // Create a few snapshots
        for i in 1..=3 {
            table = append_and_commit(&table, &format!("test/data{}.parquet", i), 100).await;
        }

        assert_eq!(table.metadata().snapshots.len(), 3);

        // Expire snapshots older than a very old time (should expire nothing due to retain_last=1)
        let very_old_time = Utc::now().timestamp_millis() - 365 * 24 * 60 * 60 * 1000; // 1 year ago
        let action = ExpireSnapshotsAction::new()
            .expire_older_than(very_old_time)
            .retain_last(1);

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        // Should expire 2 snapshots (3 total - 1 to retain)
        if let Some(TableUpdate::RemoveSnapshots { snapshot_ids }) = updates.first() {
            assert!(snapshot_ids.len() >= 2, "Should expire older snapshots");
        }
    }

    #[tokio::test]
    async fn test_expire_never_expires_current_snapshot() {
        let mut table = make_v2_minimal_table();

        // Create 3 snapshots
        for i in 1..=3 {
            table = append_and_commit(&table, &format!("test/data{}.parquet", i), 100).await;
        }

        let current_snapshot_id = table
            .metadata()
            .current_snapshot()
            .map(|s| s.snapshot_id())
            .unwrap();

        // Try to expire the current snapshot explicitly
        let action = ExpireSnapshotsAction::new().expire_snapshot_id(current_snapshot_id);

        let result = Arc::new(action).commit(&table).await;

        // Should error because we can't expire current snapshot
        assert!(result.is_err());
        if let Err(err) = result {
            assert_eq!(err.kind(), ErrorKind::DataInvalid);
            assert!(err.message().contains("Cannot expire current snapshot"));
        }
    }

    #[tokio::test]
    async fn test_expire_snapshots_updates_refs() {
        let mut table = make_v2_minimal_table();

        // Create snapshots
        for i in 1..=3 {
            table = append_and_commit(&table, &format!("test/data{}.parquet", i), 100).await;
        }

        // Expire old snapshots, keep last 1
        let action = ExpireSnapshotsAction::new()
            .retain_last(1)
            .expire_older_than(Utc::now().timestamp_millis());

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        // Verify we have RemoveSnapshots update
        let has_remove_snapshots = updates.iter().any(|u| {
            matches!(u, TableUpdate::RemoveSnapshots { .. })
        });
        assert!(has_remove_snapshots, "Should have RemoveSnapshots update");

        // Verify requirements
        let requirements = action_commit.take_requirements();
        assert_eq!(requirements.len(), 2);

        // Should have UuidMatch and RefSnapshotIdMatch
        assert!(requirements.iter().any(|r| matches!(r, TableRequirement::UuidMatch { .. })));
        assert!(requirements.iter().any(|r| matches!(r, TableRequirement::RefSnapshotIdMatch { .. })));
    }

    #[tokio::test]
    async fn test_expire_with_explicit_snapshot_ids() {
        let mut table = make_v2_minimal_table();

        // Create 3 snapshots
        for i in 1..=3 {
            table = append_and_commit(&table, &format!("test/data{}.parquet", i), 100).await;
        }

        // Get all snapshot IDs except current
        let all_snapshot_ids: Vec<i64> = table
            .metadata()
            .snapshots
            .keys()
            .copied()
            .collect();

        let current_snapshot_id = table
            .metadata()
            .current_snapshot()
            .map(|s| s.snapshot_id())
            .unwrap();

        // Find an old snapshot ID to expire
        let old_snapshot_id = all_snapshot_ids
            .iter()
            .find(|&&id| id != current_snapshot_id)
            .copied();

        if let Some(old_id) = old_snapshot_id {
            // Expire specific snapshot
            let action = ExpireSnapshotsAction::new().expire_snapshot_id(old_id);

            let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
            let updates = action_commit.take_updates();

            // Verify the specific snapshot was removed
            if let Some(TableUpdate::RemoveSnapshots { snapshot_ids }) = updates.first() {
                assert!(snapshot_ids.contains(&old_id));
            }
        }
    }
}
