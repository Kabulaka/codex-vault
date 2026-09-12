use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
};

use fs2::FileExt;
use rusqlite::{Connection, OptionalExtension, params};

use crate::domain::{
    Action, Cutoff, HistoryEntry, ItemResult, Language, OperationStore, PendingBatch, Preferences,
    VaultError,
};

pub struct SqliteStore {
    connection: Connection,
    lock_file: File,
}

pub enum StorageBackend {
    Ready(SqliteStore),
    ReadOnly(String),
}

impl StorageBackend {
    pub fn open_or_read_only(path: &Path) -> Self {
        match SqliteStore::open(path) {
            Ok(store) => Self::Ready(store),
            Err(error) => Self::ReadOnly(error.to_string()),
        }
    }
}

impl SqliteStore {
    pub fn open(path: &Path) -> Result<Self, VaultError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|error| VaultError::Storage(error.to_string()))?;
        }
        let lock_path = lock_path(path);
        let lock_file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)
            .map_err(|error| VaultError::Storage(error.to_string()))?;
        lock_file.try_lock_exclusive().map_err(|error| {
            VaultError::Storage(format!(
                "another Codex Vault process owns {}: {error}",
                lock_path.display()
            ))
        })?;

        let mut connection =
            Connection::open(path).map_err(|error| VaultError::Storage(error.to_string()))?;
        connection
            .pragma_update(None, "foreign_keys", "ON")
            .map_err(|error| VaultError::Storage(error.to_string()))?;
        migrate(&mut connection)?;
        Ok(Self {
            connection,
            lock_file,
        })
    }

    fn transaction(&mut self) -> Result<rusqlite::Transaction<'_>, VaultError> {
        self.connection
            .transaction()
            .map_err(|error| VaultError::Storage(error.to_string()))
    }
}

impl Drop for SqliteStore {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.lock_file);
    }
}

fn lock_path(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_owned();
    value.push(".lock");
    PathBuf::from(value)
}

fn migrate(connection: &mut Connection) -> Result<(), VaultError> {
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(|error| VaultError::Storage(error.to_string()))?;
    if !matches!(version, 0..=2) {
        return Err(VaultError::Storage(format!(
            "unsupported database schema version {version}"
        )));
    }

    let existing = existing_tables(connection)?;
    if !existing.is_empty() {
        validate_existing_schema(connection, version)?;
    }

    let transaction = connection
        .transaction()
        .map_err(|error| VaultError::Storage(error.to_string()))?;
    transaction
        .execute_batch(
            "
            CREATE TABLE IF NOT EXISTS preferences (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                language TEXT NOT NULL,
                cutoff_kind TEXT NOT NULL,
                cutoff_value TEXT NOT NULL,
                project TEXT,
                archived INTEGER,
                updated_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS operation_batches (
                id TEXT PRIMARY KEY,
                plan_key TEXT NOT NULL UNIQUE,
                action TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                planned_count INTEGER NOT NULL,
                status TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS operation_items (
                batch_id TEXT NOT NULL REFERENCES operation_batches(id) ON DELETE CASCADE,
                session_id TEXT NOT NULL,
                action TEXT NOT NULL,
                result TEXT,
                completed_at INTEGER,
                PRIMARY KEY (batch_id, session_id)
            );
            CREATE TABLE IF NOT EXISTS write_probe (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                touched_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_operation_batches_created
                ON operation_batches(created_at);
            ",
        )
        .map_err(|error| VaultError::Storage(error.to_string()))?;
    if version < 2 && existing.contains("operation_batches") {
        transaction
            .execute("ALTER TABLE operation_batches ADD COLUMN plan_key TEXT", [])
            .map_err(|error| VaultError::Storage(error.to_string()))?;
        transaction
            .execute(
                "UPDATE operation_batches SET plan_key = 'legacy:' || id WHERE plan_key IS NULL",
                [],
            )
            .map_err(|error| VaultError::Storage(error.to_string()))?;
        transaction
            .execute(
                "CREATE UNIQUE INDEX idx_operation_batches_plan_key ON operation_batches(plan_key)",
                [],
            )
            .map_err(|error| VaultError::Storage(error.to_string()))?;
    }
    transaction
        .pragma_update(None, "user_version", 2)
        .map_err(|error| VaultError::Storage(error.to_string()))?;
    transaction
        .commit()
        .map_err(|error| VaultError::Storage(error.to_string()))
}

fn existing_tables(connection: &Connection) -> Result<BTreeSet<String>, VaultError> {
    let mut statement = connection
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'")
        .map_err(|error| VaultError::Storage(error.to_string()))?;
    let rows = statement
        .query_map([], |row| row.get(0))
        .map_err(|error| VaultError::Storage(error.to_string()))?;
    rows.collect::<Result<BTreeSet<_>, _>>()
        .map_err(|error| VaultError::Storage(error.to_string()))
}

fn validate_existing_schema(connection: &Connection, version: i64) -> Result<(), VaultError> {
    let expected = [
        (
            "preferences",
            &[
                "singleton",
                "language",
                "cutoff_kind",
                "cutoff_value",
                "project",
                "archived",
                "updated_at",
            ][..],
        ),
        (
            "operation_batches",
            &["id", "action", "created_at", "planned_count", "status"][..],
        ),
        (
            "operation_items",
            &["batch_id", "session_id", "action", "result", "completed_at"][..],
        ),
    ];
    let tables = existing_tables(connection)?;
    for (table, required) in expected {
        if !tables.contains(table) {
            return Err(VaultError::Storage(format!(
                "existing database is missing table {table}"
            )));
        }
        let mut statement = connection
            .prepare(&format!("PRAGMA table_info({table})"))
            .map_err(|error| VaultError::Storage(error.to_string()))?;
        let rows = statement
            .query_map([], |row| row.get::<_, String>(1))
            .map_err(|error| VaultError::Storage(error.to_string()))?;
        let columns = rows
            .collect::<Result<BTreeSet<_>, _>>()
            .map_err(|error| VaultError::Storage(error.to_string()))?;
        if required.iter().any(|column| !columns.contains(*column))
            || (version == 2 && table == "operation_batches" && !columns.contains("plan_key"))
        {
            return Err(VaultError::Storage(format!(
                "existing table {table} has an incompatible schema"
            )));
        }
    }
    Ok(())
}

fn batch_node_ids(connection: &Connection, batch_id: &str) -> Result<BTreeSet<String>, VaultError> {
    let mut statement = connection
        .prepare("SELECT session_id FROM operation_items WHERE batch_id = ?1")
        .map_err(|error| VaultError::Storage(error.to_string()))?;
    let rows = statement
        .query_map([batch_id], |row| row.get(0))
        .map_err(|error| VaultError::Storage(error.to_string()))?;
    rows.collect::<Result<BTreeSet<_>, _>>()
        .map_err(|error| VaultError::Storage(error.to_string()))
}

fn new_batch_id() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("batch-{nanos:x}-{:x}", std::process::id())
}

impl OperationStore for SqliteStore {
    fn write_ready(&self) -> Result<(), VaultError> {
        self.connection
            .execute_batch("SAVEPOINT codex_vault_write_probe")
            .map_err(|error| VaultError::Storage(error.to_string()))?;
        let probe = self.connection.execute(
            "INSERT INTO write_probe(singleton, touched_at) VALUES (1, unixepoch())
             ON CONFLICT(singleton) DO UPDATE SET touched_at = excluded.touched_at",
            [],
        );
        let rollback = self
            .connection
            .execute_batch("ROLLBACK TO codex_vault_write_probe");
        let release = self
            .connection
            .execute_batch("RELEASE codex_vault_write_probe");
        let cleanup_error = match (rollback, release) {
            (Ok(()), Ok(())) => None,
            (Err(rollback), Ok(())) => Some(format!("rollback failed: {rollback}")),
            (Ok(()), Err(release)) => Some(format!("release failed: {release}")),
            (Err(rollback), Err(release)) => Some(format!(
                "rollback failed: {rollback}; release failed: {release}"
            )),
        };
        match (probe, cleanup_error) {
            (Ok(_), None) => Ok(()),
            (Err(error), None) => Err(VaultError::Storage(error.to_string())),
            (Ok(_), Some(cleanup)) => Err(VaultError::Storage(format!(
                "write probe cleanup failed: {cleanup}"
            ))),
            (Err(error), Some(cleanup)) => Err(VaultError::Storage(format!(
                "write probe failed: {error}; cleanup failed: {cleanup}"
            ))),
        }
    }

    fn load_preferences(&self) -> Result<Preferences, VaultError> {
        let value = self
            .connection
            .query_row(
                "SELECT language, cutoff_kind, cutoff_value, project, archived
                 FROM preferences WHERE singleton = 1",
                [],
                |row| {
                    let language: String = row.get(0)?;
                    let kind: String = row.get(1)?;
                    let raw: String = row.get(2)?;
                    let cutoff = if kind == "absolute" {
                        Cutoff::Absolute(raw.parse().unwrap_or(0))
                    } else {
                        Cutoff::RollingDays(raw.parse().unwrap_or(30))
                    };
                    let archived = row.get::<_, Option<i64>>(4)?.map(|value| value != 0);
                    Ok(Preferences {
                        language: Language::parse(&language),
                        cutoff,
                        project: row.get(3)?,
                        archived,
                    })
                },
            )
            .optional()
            .map_err(|error| VaultError::Storage(error.to_string()))?;
        Ok(value.unwrap_or_default())
    }

    fn save_preferences(&mut self, value: &Preferences) -> Result<(), VaultError> {
        let (kind, cutoff) = match value.cutoff {
            Cutoff::RollingDays(days) => ("rolling_days", days.to_string()),
            Cutoff::Absolute(epoch) => ("absolute", epoch.to_string()),
        };
        self.connection
            .execute(
                "INSERT INTO preferences
                    (singleton, language, cutoff_kind, cutoff_value, project, archived, updated_at)
                 VALUES (1, ?1, ?2, ?3, ?4, ?5, unixepoch())
                 ON CONFLICT(singleton) DO UPDATE SET
                    language=excluded.language,
                    cutoff_kind=excluded.cutoff_kind,
                    cutoff_value=excluded.cutoff_value,
                    project=excluded.project,
                    archived=excluded.archived,
                    updated_at=excluded.updated_at",
                params![
                    value.language.code(),
                    kind,
                    cutoff,
                    value.project,
                    value.archived.map(i64::from)
                ],
            )
            .map_err(|error| VaultError::Storage(error.to_string()))?;
        Ok(())
    }

    fn begin_batch(
        &mut self,
        plan_key: &str,
        action: Action,
        created_at: i64,
        node_ids: &[String],
    ) -> Result<String, VaultError> {
        let candidate_id = new_batch_id();
        let transaction = self.transaction()?;
        transaction
            .execute(
                "INSERT INTO operation_batches
                    (id, plan_key, action, created_at, planned_count, status)
                 VALUES (?1, ?2, ?3, ?4, ?5, 'executing')
                 ON CONFLICT(plan_key) DO NOTHING",
                params![
                    candidate_id,
                    plan_key,
                    action.as_str(),
                    created_at,
                    node_ids.len()
                ],
            )
            .map_err(|error| VaultError::Storage(error.to_string()))?;
        let batch_id: String = transaction
            .query_row(
                "SELECT id FROM operation_batches
                 WHERE plan_key = ?1 AND action = ?2 AND planned_count = ?3",
                params![plan_key, action.as_str(), node_ids.len()],
                |row| row.get(0),
            )
            .map_err(|error| VaultError::Storage(format!("incompatible existing plan: {error}")))?;
        transaction
            .execute(
                "UPDATE operation_batches SET status = 'executing' WHERE id = ?1",
                [&batch_id],
            )
            .map_err(|error| VaultError::Storage(error.to_string()))?;
        transaction
            .execute(
                "UPDATE operation_items SET result = NULL, completed_at = NULL
                 WHERE batch_id = ?1 AND result != 'success' AND result != 'skipped'",
                [&batch_id],
            )
            .map_err(|error| VaultError::Storage(error.to_string()))?;
        {
            let mut statement = transaction
                .prepare(
                    "INSERT OR IGNORE INTO operation_items
                        (batch_id, session_id, action, result, completed_at)
                     VALUES (?1, ?2, ?3, NULL, NULL)",
                )
                .map_err(|error| VaultError::Storage(error.to_string()))?;
            for node_id in node_ids {
                statement
                    .execute(params![&batch_id, node_id, action.as_str()])
                    .map_err(|error| VaultError::Storage(error.to_string()))?;
            }
        }
        let stored = batch_node_ids(&transaction, &batch_id)?;
        let expected = node_ids.iter().cloned().collect::<BTreeSet<_>>();
        if stored != expected || stored.len() != node_ids.len() {
            return Err(VaultError::Storage(
                "existing immutable plan has a different node set".into(),
            ));
        }
        transaction
            .commit()
            .map_err(|error| VaultError::Storage(error.to_string()))?;
        Ok(batch_id)
    }

    fn complete_batch(
        &mut self,
        batch_id: &str,
        results: &BTreeMap<String, ItemResult>,
        status: &str,
        completed_at: i64,
    ) -> Result<(), VaultError> {
        let transaction = self.transaction()?;
        let stored = batch_node_ids(&transaction, batch_id)?;
        let expected = results.keys().cloned().collect::<BTreeSet<_>>();
        if stored != expected {
            return Err(VaultError::Storage(format!(
                "result set does not match immutable plan {batch_id}"
            )));
        }
        for (node_id, result) in results {
            let value = result.as_storage_value();
            let changed = transaction
                .execute(
                    "UPDATE operation_items
                     SET result = ?3, completed_at = ?4
                     WHERE batch_id = ?1 AND session_id = ?2
                       AND (result IS NULL OR result = 'interrupted'
                            OR result LIKE 'failed:%' OR result = ?3)",
                    params![batch_id, node_id, value, completed_at],
                )
                .map_err(|error| VaultError::Storage(error.to_string()))?;
            if changed != 1 {
                return Err(VaultError::Storage(format!(
                    "operation item is missing or has a conflicting observed result: {batch_id}/{node_id}"
                )));
            }
        }
        let changed = transaction
            .execute(
                "UPDATE operation_batches SET status = ?2 WHERE id = ?1",
                params![batch_id, status],
            )
            .map_err(|error| VaultError::Storage(error.to_string()))?;
        if changed != 1 {
            return Err(VaultError::Storage(format!(
                "operation batch is missing: {batch_id}"
            )));
        }
        transaction
            .commit()
            .map_err(|error| VaultError::Storage(error.to_string()))?;
        Ok(())
    }

    fn pending_batches(&self) -> Result<Vec<PendingBatch>, VaultError> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT b.id, b.action, i.session_id
                 FROM operation_batches b
                 JOIN operation_items i ON i.batch_id = b.id
                 WHERE b.status IN ('executing', 'interrupted', 'needs_verification')
                 ORDER BY b.created_at, b.id, i.session_id",
            )
            .map_err(|error| VaultError::Storage(error.to_string()))?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(|error| VaultError::Storage(error.to_string()))?;
        let mut batches = BTreeMap::<String, (Action, Vec<String>)>::new();
        for row in rows {
            let (batch_id, action, node_id) =
                row.map_err(|error| VaultError::Storage(error.to_string()))?;
            let action = Action::parse(&action).ok_or_else(|| {
                VaultError::Storage(format!("unknown action in pending batch {batch_id}"))
            })?;
            let entry = batches
                .entry(batch_id)
                .or_insert_with(|| (action, Vec::new()));
            if entry.0 != action {
                return Err(VaultError::Storage(
                    "pending batch contains conflicting actions".into(),
                ));
            }
            entry.1.push(node_id);
        }
        Ok(batches
            .into_iter()
            .map(|(batch_id, (action, node_ids))| PendingBatch {
                batch_id,
                action,
                node_ids,
            })
            .collect())
    }

    fn prune(&mut self, now: i64, retention_days: u32) -> Result<usize, VaultError> {
        let cutoff = now.saturating_sub(i64::from(retention_days) * 86_400);
        self.connection
            .execute(
                "DELETE FROM operation_batches
                 WHERE created_at < ?1 AND status NOT IN ('executing')",
                [cutoff],
            )
            .map_err(|error| VaultError::Storage(error.to_string()))
    }

    fn history(&self, limit: usize) -> Result<Vec<HistoryEntry>, VaultError> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT id, created_at, action, planned_count, status
                 FROM operation_batches ORDER BY created_at DESC LIMIT ?1",
            )
            .map_err(|error| VaultError::Storage(error.to_string()))?;
        let rows = statement
            .query_map([limit], |row| {
                Ok(HistoryEntry {
                    batch_id: row.get(0)?,
                    created_at: row.get(1)?,
                    action: row.get(2)?,
                    planned_count: row.get::<_, i64>(3)?.try_into().unwrap_or(usize::MAX),
                    status: row.get(4)?,
                })
            })
            .map_err(|error| VaultError::Storage(error.to_string()))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|error| VaultError::Storage(error.to_string()))
    }
}

impl OperationStore for StorageBackend {
    fn write_ready(&self) -> Result<(), VaultError> {
        match self {
            Self::Ready(store) => store.write_ready(),
            Self::ReadOnly(reason) => Err(VaultError::Storage(reason.clone())),
        }
    }

    fn load_preferences(&self) -> Result<Preferences, VaultError> {
        match self {
            Self::Ready(store) => store.load_preferences(),
            Self::ReadOnly(_) => Ok(Preferences::default()),
        }
    }

    fn save_preferences(&mut self, value: &Preferences) -> Result<(), VaultError> {
        match self {
            Self::Ready(store) => store.save_preferences(value),
            Self::ReadOnly(reason) => Err(VaultError::Storage(reason.clone())),
        }
    }

    fn begin_batch(
        &mut self,
        plan_key: &str,
        action: Action,
        created_at: i64,
        node_ids: &[String],
    ) -> Result<String, VaultError> {
        match self {
            Self::Ready(store) => store.begin_batch(plan_key, action, created_at, node_ids),
            Self::ReadOnly(reason) => Err(VaultError::Storage(reason.clone())),
        }
    }

    fn complete_batch(
        &mut self,
        batch_id: &str,
        results: &BTreeMap<String, ItemResult>,
        status: &str,
        completed_at: i64,
    ) -> Result<(), VaultError> {
        match self {
            Self::Ready(store) => store.complete_batch(batch_id, results, status, completed_at),
            Self::ReadOnly(reason) => Err(VaultError::Storage(reason.clone())),
        }
    }

    fn pending_batches(&self) -> Result<Vec<PendingBatch>, VaultError> {
        match self {
            Self::Ready(store) => store.pending_batches(),
            Self::ReadOnly(reason) => Err(VaultError::Storage(reason.clone())),
        }
    }

    fn prune(&mut self, now: i64, retention_days: u32) -> Result<usize, VaultError> {
        match self {
            Self::Ready(store) => store.prune(now, retention_days),
            Self::ReadOnly(_) => Ok(0),
        }
    }

    fn history(&self, limit: usize) -> Result<Vec<HistoryEntry>, VaultError> {
        match self {
            Self::Ready(store) => store.history(limit),
            Self::ReadOnly(_) => Ok(Vec::new()),
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn batch_plan_and_items_are_atomic_and_recoverable() {
        let directory = tempdir().unwrap();
        let mut store = SqliteStore::open(&directory.path().join("vault.db")).unwrap();
        let batch_id = store
            .begin_batch("b1", Action::Archive, 100, &["a".into(), "b".into()])
            .unwrap();
        let pending = store.pending_batches().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].batch_id, batch_id);
        assert_eq!(pending[0].node_ids, vec!["a", "b"]);

        let incomplete = BTreeMap::from([("a".into(), ItemResult::Success)]);
        assert!(
            store
                .complete_batch(&batch_id, &incomplete, "completed", 101)
                .is_err()
        );
        let unchanged: i64 = store
            .connection
            .query_row(
                "SELECT count(*) FROM operation_items WHERE result IS NULL",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(unchanged, 2);

        let complete = BTreeMap::from([
            ("a".into(), ItemResult::Success),
            ("b".into(), ItemResult::Interrupted),
        ]);
        store
            .complete_batch(&batch_id, &complete, "interrupted", 102)
            .unwrap();
        let history = store.history(10).unwrap();
        assert_eq!(history[0].status, "interrupted");
        assert_eq!(history[0].planned_count, 2);
    }

    #[test]
    fn plan_key_is_idempotent_but_immutable() {
        let directory = tempdir().unwrap();
        let mut store = SqliteStore::open(&directory.path().join("vault.db")).unwrap();
        let first = store
            .begin_batch("same", Action::Archive, 100, &["a".into(), "b".into()])
            .unwrap();
        let second = store
            .begin_batch("same", Action::Archive, 101, &["a".into(), "b".into()])
            .unwrap();
        assert_eq!(first, second);
        assert!(
            store
                .begin_batch("same", Action::Archive, 102, &["a".into()])
                .is_err()
        );
        assert_eq!(store.pending_batches().unwrap()[0].node_ids, vec!["a", "b"]);
    }

    #[test]
    fn write_probe_detects_read_only_connections() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("vault.db");
        drop(SqliteStore::open(&path).unwrap());
        let connection =
            Connection::open_with_flags(&path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        let lock_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(lock_path(&path))
            .unwrap();
        let store = SqliteStore {
            connection,
            lock_file,
        };
        assert!(store.write_ready().is_err());
        assert!(store.connection.is_autocommit());
    }

    #[test]
    fn failed_write_probe_releases_savepoint_and_allows_later_batches() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("vault.db");
        let mut store = SqliteStore::open(&path).unwrap();
        store
            .connection
            .execute("DROP TABLE write_probe", [])
            .unwrap();
        assert!(store.write_ready().is_err());
        assert!(store.connection.is_autocommit());
        store
            .connection
            .execute_batch(
                "CREATE TABLE write_probe (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    touched_at INTEGER NOT NULL
                );",
            )
            .unwrap();
        assert!(store.write_ready().is_ok());
        assert!(
            store
                .begin_batch("after-probe", Action::Archive, 100, &["root".into()])
                .is_ok()
        );
    }

    #[test]
    fn only_one_process_instance_can_own_a_database() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("vault.db");
        let first = SqliteStore::open(&path).unwrap();
        assert!(SqliteStore::open(&path).is_err());
        drop(first);
        assert!(SqliteStore::open(&path).is_ok());
    }

    #[test]
    fn incompatible_schema_is_rejected_without_modifying_database() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("vault.db");
        let connection = Connection::open(&path).unwrap();
        connection
            .execute("CREATE TABLE foreign_data (value TEXT)", [])
            .unwrap();
        connection.pragma_update(None, "user_version", 1).unwrap();
        drop(connection);
        let before = std::fs::read(&path).unwrap();
        assert!(SqliteStore::open(&path).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), before);
    }

    #[test]
    fn failed_v1_migration_rolls_back_every_schema_change() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("vault.db");
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE preferences (
                    singleton INTEGER PRIMARY KEY,
                    language TEXT NOT NULL,
                    cutoff_kind TEXT NOT NULL,
                    cutoff_value TEXT NOT NULL,
                    project TEXT,
                    archived INTEGER,
                    updated_at INTEGER NOT NULL
                );
                CREATE TABLE operation_batches (
                    id TEXT PRIMARY KEY,
                    action TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    planned_count INTEGER NOT NULL,
                    status TEXT NOT NULL
                );
                CREATE TABLE operation_items (
                    batch_id TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    action TEXT NOT NULL,
                    result TEXT,
                    completed_at INTEGER,
                    PRIMARY KEY (batch_id, session_id)
                );
                CREATE INDEX idx_operation_batches_plan_key ON preferences(language);
                PRAGMA user_version = 1;",
            )
            .unwrap();
        drop(connection);

        assert!(SqliteStore::open(&path).is_err());
        let connection = Connection::open(&path).unwrap();
        let version: i64 = connection
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 1);
        let plan_key_columns: i64 = connection
            .query_row(
                "SELECT count(*) FROM pragma_table_info('operation_batches') WHERE name = 'plan_key'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(plan_key_columns, 0);
        let write_probe_tables: i64 = connection
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'write_probe'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(write_probe_tables, 0);
    }

    #[test]
    fn corrupt_database_is_rejected_without_rewriting_it() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("vault.db");
        let before = b"not a sqlite database".to_vec();
        std::fs::write(&path, &before).unwrap();
        assert!(SqliteStore::open(&path).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), before);
    }

    #[test]
    fn preferences_round_trip_custom_cutoff() {
        let directory = tempdir().unwrap();
        let mut store = SqliteStore::open(&directory.path().join("vault.db")).unwrap();
        let value = Preferences {
            language: Language::En,
            cutoff: Cutoff::Absolute(123),
            project: Some("/work".into()),
            archived: Some(true),
        };
        store.save_preferences(&value).unwrap();
        assert_eq!(store.load_preferences().unwrap(), value);
    }
}
