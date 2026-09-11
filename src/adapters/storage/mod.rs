use std::path::Path;

use rusqlite::{Connection, OptionalExtension, params};

use crate::domain::{
    Action, Cutoff, HistoryEntry, ItemResult, Language, OperationStore, Preferences, VaultError,
};

pub struct SqliteStore {
    connection: Connection,
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
        let connection =
            Connection::open(path).map_err(|error| VaultError::Storage(error.to_string()))?;
        connection
            .execute_batch(
                "
                PRAGMA foreign_keys = ON;
                PRAGMA journal_mode = WAL;
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
                CREATE INDEX IF NOT EXISTS idx_operation_batches_created
                    ON operation_batches(created_at);
                ",
            )
            .map_err(|error| VaultError::Storage(error.to_string()))?;
        Ok(Self { connection })
    }

    fn transaction(&mut self) -> Result<rusqlite::Transaction<'_>, VaultError> {
        self.connection
            .transaction()
            .map_err(|error| VaultError::Storage(error.to_string()))
    }
}

impl OperationStore for SqliteStore {
    fn write_ready(&self) -> Result<(), VaultError> {
        self.connection
            .query_row("SELECT 1", [], |_| Ok(()))
            .map_err(|error| VaultError::Storage(error.to_string()))
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
        batch_id: &str,
        action: Action,
        created_at: i64,
        node_ids: &[String],
    ) -> Result<(), VaultError> {
        let transaction = self.transaction()?;
        transaction
            .execute(
                "INSERT INTO operation_batches(id, action, created_at, planned_count, status)
                 VALUES (?1, ?2, ?3, ?4, 'executing')",
                params![batch_id, action.as_str(), created_at, node_ids.len()],
            )
            .map_err(|error| VaultError::Storage(error.to_string()))?;
        {
            let mut statement = transaction
                .prepare(
                    "INSERT INTO operation_items(batch_id, session_id, action, result, completed_at)
                     VALUES (?1, ?2, ?3, NULL, NULL)",
                )
                .map_err(|error| VaultError::Storage(error.to_string()))?;
            for node_id in node_ids {
                statement
                    .execute(params![batch_id, node_id, action.as_str()])
                    .map_err(|error| VaultError::Storage(error.to_string()))?;
            }
        }
        transaction
            .commit()
            .map_err(|error| VaultError::Storage(error.to_string()))
    }

    fn record_result(
        &mut self,
        batch_id: &str,
        node_id: &str,
        result: &ItemResult,
        completed_at: i64,
    ) -> Result<(), VaultError> {
        let changed = self
            .connection
            .execute(
                "UPDATE operation_items
                 SET result = ?3, completed_at = ?4
                 WHERE batch_id = ?1 AND session_id = ?2
                   AND (result IS NULL OR result = 'interrupted')",
                params![batch_id, node_id, result.as_storage_value(), completed_at],
            )
            .map_err(|error| VaultError::Storage(error.to_string()))?;
        if changed != 1 {
            return Err(VaultError::Storage(format!(
                "operation item is missing or immutable: {batch_id}/{node_id}"
            )));
        }
        Ok(())
    }

    fn finish_batch(&mut self, batch_id: &str, status: &str) -> Result<(), VaultError> {
        self.connection
            .execute(
                "UPDATE operation_batches SET status = ?2 WHERE id = ?1",
                params![batch_id, status],
            )
            .map_err(|error| VaultError::Storage(error.to_string()))?;
        Ok(())
    }

    fn recover_interrupted(&mut self) -> Result<usize, VaultError> {
        let transaction = self.transaction()?;
        let batches = transaction
            .execute(
                "UPDATE operation_batches SET status = 'interrupted'
                 WHERE status = 'executing'",
                [],
            )
            .map_err(|error| VaultError::Storage(error.to_string()))?;
        transaction
            .execute(
                "UPDATE operation_items SET result = 'interrupted'
                 WHERE result IS NULL AND batch_id IN
                   (SELECT id FROM operation_batches WHERE status = 'interrupted')",
                [],
            )
            .map_err(|error| VaultError::Storage(error.to_string()))?;
        transaction
            .commit()
            .map_err(|error| VaultError::Storage(error.to_string()))?;
        Ok(batches)
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
        batch_id: &str,
        action: Action,
        created_at: i64,
        node_ids: &[String],
    ) -> Result<(), VaultError> {
        match self {
            Self::Ready(store) => store.begin_batch(batch_id, action, created_at, node_ids),
            Self::ReadOnly(reason) => Err(VaultError::Storage(reason.clone())),
        }
    }

    fn record_result(
        &mut self,
        batch_id: &str,
        node_id: &str,
        result: &ItemResult,
        completed_at: i64,
    ) -> Result<(), VaultError> {
        match self {
            Self::Ready(store) => store.record_result(batch_id, node_id, result, completed_at),
            Self::ReadOnly(reason) => Err(VaultError::Storage(reason.clone())),
        }
    }

    fn finish_batch(&mut self, batch_id: &str, status: &str) -> Result<(), VaultError> {
        match self {
            Self::Ready(store) => store.finish_batch(batch_id, status),
            Self::ReadOnly(reason) => Err(VaultError::Storage(reason.clone())),
        }
    }

    fn recover_interrupted(&mut self) -> Result<usize, VaultError> {
        match self {
            Self::Ready(store) => store.recover_interrupted(),
            Self::ReadOnly(_) => Ok(0),
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
        store
            .begin_batch("b1", Action::Archive, 100, &["a".into(), "b".into()])
            .unwrap();
        assert_eq!(store.recover_interrupted().unwrap(), 1);
        let history = store.history(10).unwrap();
        assert_eq!(history[0].status, "interrupted");
        assert_eq!(history[0].planned_count, 2);
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
