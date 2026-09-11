use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use rusqlite::{Connection, OpenFlags, types::ValueRef};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines},
    process::{Child, ChildStdin, ChildStdout, Command},
    time::timeout,
};

use crate::domain::{
    Action, MutationAck, PortFuture, RuntimeStatus, ScanSnapshot, SessionGateway, SessionNode,
    SessionSource, VaultError,
};

const ALL_SOURCE_KINDS: &[&str] = &[
    "cli",
    "vscode",
    "exec",
    "appServer",
    "subAgent",
    "subAgentReview",
    "subAgentCompact",
    "subAgentThreadSpawn",
    "subAgentOther",
    "unknown",
];

pub struct AppServerClient {
    child: Child,
    stdin: ChildStdin,
    stdout: Lines<BufReader<ChildStdout>>,
    next_id: u64,
    notifications: Vec<Notification>,
    codex_state_dir: Result<PathBuf, String>,
}

impl AppServerClient {
    pub async fn spawn(codex: &Path) -> Result<Self, VaultError> {
        let mut child = Command::new(codex)
            .arg("app-server")
            .arg("--stdio")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .map_err(|error| VaultError::Unavailable(error.to_string()))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| VaultError::Unavailable("app-server stdin unavailable".into()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| VaultError::Unavailable("app-server stdout unavailable".into()))?;
        let mut client = Self {
            child,
            stdin,
            stdout: BufReader::new(stdout).lines(),
            next_id: 1,
            notifications: Vec::new(),
            codex_state_dir: default_codex_state_dir(),
        };
        client.initialize().await?;
        Ok(client)
    }

    async fn initialize(&mut self) -> Result<(), VaultError> {
        self.request(
            "initialize",
            json!({
                "clientInfo": {
                    "name": "codex_vault",
                    "title": "Codex Vault",
                    "version": env!("CARGO_PKG_VERSION")
                },
                "capabilities": {
                    "experimentalApi": true
                }
            }),
        )
        .await?;
        self.send(json!({"method": "initialized", "params": {}}))
            .await
    }

    async fn send(&mut self, value: Value) -> Result<(), VaultError> {
        let mut encoded =
            serde_json::to_vec(&value).map_err(|error| VaultError::Protocol(error.to_string()))?;
        encoded.push(b'\n');
        self.stdin
            .write_all(encoded.as_slice())
            .await
            .map_err(|error| VaultError::Unavailable(error.to_string()))?;
        self.stdin
            .flush()
            .await
            .map_err(|error| VaultError::Unavailable(error.to_string()))
    }

    async fn request(&mut self, method: &str, params: Value) -> Result<Value, VaultError> {
        let id = self.next_id;
        self.next_id += 1;
        self.send(json!({"method": method, "id": id, "params": params}))
            .await?;
        loop {
            let line = self
                .stdout
                .next_line()
                .await
                .map_err(|error| VaultError::Unavailable(error.to_string()))?
                .ok_or_else(|| {
                    VaultError::Unavailable(format!("app-server exited while waiting for {method}"))
                })?;
            let value: Value = serde_json::from_str(&line)
                .map_err(|error| VaultError::Protocol(error.to_string()))?;
            if value.get("method").is_some() {
                self.capture_notification(&value);
                continue;
            }
            if value.get("id").and_then(Value::as_u64) != Some(id) {
                continue;
            }
            if let Some(error) = value.get("error") {
                return Err(VaultError::Protocol(format!(
                    "{method}: {}",
                    error
                        .get("message")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown app-server error")
                )));
            }
            return value
                .get("result")
                .cloned()
                .ok_or_else(|| VaultError::Protocol(format!("{method}: missing result")));
        }
    }

    fn capture_notification(&mut self, value: &Value) {
        let Some(method) = value.get("method").and_then(Value::as_str) else {
            return;
        };
        let thread_id = value
            .pointer("/params/threadId")
            .and_then(Value::as_str)
            .map(str::to_owned);
        self.notifications.push(Notification {
            method: method.to_owned(),
            thread_id,
        });
    }

    async fn drain_notifications(&mut self) -> Result<(), VaultError> {
        while let Ok(result) = timeout(Duration::from_millis(40), self.stdout.next_line()).await {
            let Some(line) = result.map_err(|error| VaultError::Unavailable(error.to_string()))?
            else {
                break;
            };
            let value: Value = serde_json::from_str(&line)
                .map_err(|error| VaultError::Protocol(error.to_string()))?;
            self.capture_notification(&value);
        }
        Ok(())
    }

    async fn list_page(
        &mut self,
        archived: bool,
        ancestor: Option<&str>,
        cursor: Option<&str>,
        pinned: Option<bool>,
        all_sources: bool,
        use_state_db_only: bool,
    ) -> Result<ListResponse, VaultError> {
        let source_kinds = if ancestor.is_some() || all_sources {
            ALL_SOURCE_KINDS
        } else {
            &["cli", "vscode"]
        };
        let result = self
            .request(
                "thread/list",
                json!({
                    "archived": archived,
                    "cursor": cursor,
                    "limit": 200,
                    "sortKey": "recency_at",
                    "sortDirection": "desc",
                    "sourceKinds": source_kinds,
                    "ancestorThreadId": ancestor,
                    "isPinned": pinned,
                    "useStateDbOnly": use_state_db_only
                }),
            )
            .await?;
        serde_json::from_value(result).map_err(|error| VaultError::Protocol(error.to_string()))
    }

    async fn list_all(
        &mut self,
        archived: bool,
        ancestor: Option<&str>,
        pinned: Option<bool>,
        all_sources: bool,
        use_state_db_only: bool,
    ) -> Result<Vec<ThreadDto>, VaultError> {
        let mut cursor = None;
        let mut values = Vec::new();
        let mut seen = BTreeSet::new();
        loop {
            let page = self
                .list_page(
                    archived,
                    ancestor,
                    cursor.as_deref(),
                    pinned,
                    all_sources,
                    use_state_db_only,
                )
                .await?;
            values.extend(page.data);
            let Some(next) = page.next_cursor else {
                break;
            };
            if !seen.insert(next.clone()) {
                return Err(VaultError::Protocol(
                    "thread/list returned a repeated cursor".into(),
                ));
            }
            cursor = Some(next);
        }
        Ok(values)
    }

    async fn scan_inner(&mut self) -> Result<ScanSnapshot, VaultError> {
        let mut diagnostics = Vec::new();
        let mut raw = BTreeMap::<String, (ThreadDto, bool)>::new();
        let mut relation_complete = true;
        for archived in [false, true] {
            for dto in self.list_all(archived, None, None, true, false).await? {
                let id = dto.id.clone();
                if raw.insert(id.clone(), (dto, archived)).is_some() {
                    relation_complete = false;
                    diagnostics.push(format!(
                        "thread {id} appeared in active and archived listings"
                    ));
                }
            }
        }

        let root_ids = raw
            .values()
            .filter(|(dto, _)| {
                dto.parent_thread_id().is_none() && parse_source(&dto.source).is_interactive_root()
            })
            .map(|(dto, _)| dto.id.clone())
            .collect::<BTreeSet<_>>();

        let mut included_ids = BTreeSet::new();
        for start in raw.keys() {
            let mut current = start.as_str();
            let mut seen = BTreeSet::new();
            loop {
                if !seen.insert(current) {
                    relation_complete = false;
                    diagnostics.push(format!("cycle detected at thread {start}"));
                    break;
                }
                let Some((dto, _)) = raw.get(current) else {
                    relation_complete = false;
                    diagnostics.push(format!("thread {start} has missing ancestor {current}"));
                    break;
                };
                if let Some(parent) = dto.parent_thread_id() {
                    current = parent;
                } else {
                    if root_ids.contains(current) {
                        included_ids.insert(start.clone());
                    }
                    break;
                }
            }
        }

        if let Some(probe_root) = root_ids.first() {
            let expected = included_ids
                .iter()
                .filter(|id| *id != probe_root)
                .filter(|id| raw_belongs_to_root(id, probe_root, &raw))
                .cloned()
                .collect::<BTreeSet<_>>();
            let mut observed = BTreeSet::new();
            for archived in [false, true] {
                match self
                    .list_all(archived, Some(probe_root), None, true, true)
                    .await
                {
                    Ok(values) => observed.extend(values.into_iter().map(|value| value.id)),
                    Err(error) => {
                        relation_complete = false;
                        diagnostics.push(format!(
                            "ancestorThreadId unavailable for {probe_root}: {error}"
                        ));
                    }
                }
            }
            if observed != expected {
                relation_complete = false;
                diagnostics.push(format!(
                    "ancestorThreadId result for {probe_root} does not match the complete parent closure"
                ));
            }
        }

        let mut nodes = BTreeMap::<String, SessionNode>::new();
        let mut write_capable = relation_complete;
        for id in &included_ids {
            let Some((dto, archived)) = raw.remove(id) else {
                continue;
            };
            let source = if root_ids.contains(id) {
                parse_source(&dto.source)
            } else {
                SessionSource::Descendant
            };
            match map_thread(dto, archived, source) {
                Ok(node) => {
                    nodes.insert(id.clone(), node);
                }
                Err(error) => {
                    write_capable = false;
                    diagnostics.push(error.to_string());
                }
            }
        }

        if !pin_states_complete(&nodes) {
            let mut pinned_ids = BTreeSet::new();
            let mut unpinned_ids = BTreeSet::new();
            let mut pin_filter_reliable = true;
            let mut pin_diagnostics = Vec::new();
            for archived in [false, true] {
                match self.list_all(archived, None, Some(true), true, true).await {
                    Ok(values) => pinned_ids.extend(values.into_iter().map(|value| value.id)),
                    Err(error) => {
                        pin_filter_reliable = false;
                        pin_diagnostics.push(format!("isPinned=true filter unavailable: {error}"));
                    }
                }
                match self.list_all(archived, None, Some(false), true, true).await {
                    Ok(values) => unpinned_ids.extend(values.into_iter().map(|value| value.id)),
                    Err(error) => {
                        pin_filter_reliable = false;
                        pin_diagnostics.push(format!("isPinned=false filter unavailable: {error}"));
                    }
                }
            }
            if !apply_pin_partition(&mut nodes, &pinned_ids, &unpinned_ids) {
                pin_filter_reliable = false;
                pin_diagnostics.push(
                    "app-server pin filters overlap or do not cover every managed thread".into(),
                );
            }
            if !pin_filter_reliable {
                let fallback = self
                    .codex_state_dir
                    .as_ref()
                    .map_err(Clone::clone)
                    .and_then(|directory| apply_pin_state_fallback(&mut nodes, directory));
                if let Err(error) = fallback {
                    write_capable = false;
                    diagnostics.extend(pin_diagnostics);
                    diagnostics.push(format!(
                        "read-only Codex pin-state fallback unavailable: {error}"
                    ));
                    diagnostics.push(
                        "pin state cannot be proven; all state-changing operations are disabled"
                            .into(),
                    );
                }
            }
        }

        if !relation_complete {
            write_capable = false;
        }
        Ok(ScanSnapshot {
            nodes: nodes.into_values().collect(),
            write_capable,
            relation_complete,
            diagnostics,
        })
    }
}

fn raw_belongs_to_root(start: &str, root: &str, raw: &BTreeMap<String, (ThreadDto, bool)>) -> bool {
    let mut current = start;
    let mut seen = BTreeSet::new();
    loop {
        if current == root {
            return true;
        }
        if !seen.insert(current) {
            return false;
        }
        let Some((dto, _)) = raw.get(current) else {
            return false;
        };
        let Some(parent) = dto.parent_thread_id() else {
            return false;
        };
        current = parent;
    }
}

fn apply_pin_partition(
    nodes: &mut BTreeMap<String, SessionNode>,
    pinned_ids: &BTreeSet<String>,
    unpinned_ids: &BTreeSet<String>,
) -> bool {
    if pinned_ids.iter().any(|id| unpinned_ids.contains(id)) {
        return false;
    }
    let mut resolved = BTreeMap::new();
    for node in nodes.values() {
        let value = if pinned_ids.contains(&node.id) {
            true
        } else if unpinned_ids.contains(&node.id) {
            false
        } else {
            return false;
        };
        if node.pinned.is_some_and(|observed| observed != value) {
            return false;
        }
        resolved.insert(node.id.clone(), value);
    }
    for node in nodes.values_mut() {
        node.pinned = resolved.get(&node.id).copied();
    }
    true
}

fn pin_states_complete(nodes: &BTreeMap<String, SessionNode>) -> bool {
    nodes.values().all(|node| node.pinned.is_some())
}

fn default_codex_state_dir() -> Result<PathBuf, String> {
    if let Some(value) = env::var_os("CODEX_HOME") {
        let path = PathBuf::from(value);
        if path.as_os_str().is_empty() {
            return Err("CODEX_HOME is empty".into());
        }
        return Ok(path);
    }
    env::var_os("HOME")
        .map(PathBuf::from)
        .filter(|path| !path.as_os_str().is_empty())
        .map(|path| path.join(".codex"))
        .ok_or_else(|| "HOME is unavailable and CODEX_HOME is not set".into())
}

fn find_state_database(directory: &Path) -> Result<PathBuf, String> {
    let entries = fs::read_dir(directory)
        .map_err(|error| format!("cannot read {}: {error}", directory.display()))?;
    let mut candidates = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| format!("cannot inspect state directory: {error}"))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(version) = name
            .strip_prefix("state_")
            .and_then(|value| value.strip_suffix(".sqlite"))
            .and_then(|value| value.parse::<u32>().ok())
        else {
            continue;
        };
        let file_type = entry
            .file_type()
            .map_err(|error| format!("cannot inspect {name}: {error}"))?;
        if file_type.is_file() {
            candidates.push((version, entry.path()));
        }
    }
    candidates
        .into_iter()
        .max_by_key(|(version, _)| *version)
        .map(|(_, path)| path)
        .ok_or_else(|| {
            format!(
                "no versioned state_*.sqlite file exists in {}",
                directory.display()
            )
        })
}

fn read_pin_states(
    database: &Path,
    expected_ids: &BTreeSet<String>,
) -> Result<BTreeMap<String, bool>, String> {
    let connection = Connection::open_with_flags(
        database,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|error| format!("cannot open {} read-only: {error}", database.display()))?;
    let mut statement = connection
        .prepare("SELECT id, is_pinned FROM threads")
        .map_err(|error| format!("threads(id, is_pinned) is unavailable: {error}"))?;
    let mut rows = statement
        .query([])
        .map_err(|error| format!("cannot query threads(id, is_pinned): {error}"))?;
    let mut values = BTreeMap::new();
    while let Some(row) = rows
        .next()
        .map_err(|error| format!("cannot read pin-state row: {error}"))?
    {
        let id: String = row
            .get(0)
            .map_err(|error| format!("thread id is not text: {error}"))?;
        if !expected_ids.contains(&id) {
            continue;
        }
        let pinned = match row
            .get_ref(1)
            .map_err(|error| format!("is_pinned cannot be read for {id}: {error}"))?
        {
            ValueRef::Integer(0) => false,
            ValueRef::Integer(1) => true,
            _ => return Err(format!("is_pinned for {id} is not 0 or 1")),
        };
        if values.insert(id.clone(), pinned).is_some() {
            return Err(format!("duplicate pin-state row for {id}"));
        }
    }
    let missing = expected_ids
        .iter()
        .filter(|id| !values.contains_key(*id))
        .take(3)
        .cloned()
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        return Err(format!(
            "pin-state rows do not cover every scanned thread (missing: {})",
            missing.join(", ")
        ));
    }
    Ok(values)
}

fn apply_pin_state_fallback(
    nodes: &mut BTreeMap<String, SessionNode>,
    directory: &Path,
) -> Result<(), String> {
    let database = find_state_database(directory)?;
    let expected_ids = nodes.keys().cloned().collect::<BTreeSet<_>>();
    let values = read_pin_states(&database, &expected_ids)?;
    for node in nodes.values_mut() {
        if node.pinned.is_none() {
            node.pinned = values.get(&node.id).copied();
        }
    }
    if pin_states_complete(nodes) {
        Ok(())
    } else {
        Err("pin-state fallback left unknown values".into())
    }
}

impl SessionGateway for AppServerClient {
    fn scan(&mut self) -> PortFuture<'_, ScanSnapshot> {
        Box::pin(self.scan_inner())
    }

    fn mutate<'a>(&'a mut self, action: Action, thread_id: &'a str) -> PortFuture<'a, MutationAck> {
        Box::pin(async move {
            let method = format!("thread/{}", action.as_str());
            self.notifications.clear();
            self.request(method.as_str(), json!({"threadId": thread_id}))
                .await?;
            self.drain_notifications().await?;
            let expected = match action {
                Action::Archive => "thread/archived",
                Action::Restore => "thread/unarchived",
                Action::Delete => "thread/deleted",
            };
            Ok(MutationAck {
                response_received: true,
                notification_seen: self.notifications.iter().any(|notification| {
                    notification.method == expected
                        && notification.thread_id.as_deref() == Some(thread_id)
                }),
            })
        })
    }
}

impl Drop for AppServerClient {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

#[derive(Debug)]
struct Notification {
    method: String,
    thread_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListResponse {
    data: Vec<ThreadDto>,
    #[serde(default)]
    next_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ThreadDto {
    id: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    cwd: Option<String>,
    #[serde(default)]
    project_id: Option<String>,
    #[serde(default)]
    recency_at: Option<i64>,
    #[serde(default)]
    updated_at: Option<i64>,
    #[serde(default)]
    is_pinned: Option<bool>,
    #[serde(default)]
    parent_thread_id: Option<String>,
    #[serde(default)]
    source: Value,
    #[serde(default)]
    status: Option<StatusDto>,
}

impl ThreadDto {
    fn parent_thread_id(&self) -> Option<&str> {
        self.parent_thread_id.as_deref().or_else(|| {
            self.source
                .pointer("/subAgent/thread_spawn/parent_thread_id")
                .and_then(Value::as_str)
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StatusDto {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    active_flags: Vec<String>,
}

fn parse_source(value: &Value) -> SessionSource {
    match value.as_str() {
        Some("cli") => SessionSource::Cli,
        Some("vscode") => SessionSource::Vscode,
        Some("exec") => SessionSource::Exec,
        Some(other) => SessionSource::Other(other.to_owned()),
        None if value.get("subAgent").is_some() => SessionSource::Descendant,
        None if value.is_null() => SessionSource::Unknown,
        None => SessionSource::Other(value.to_string()),
    }
}

fn map_thread(
    dto: ThreadDto,
    archived: bool,
    source: SessionSource,
) -> Result<SessionNode, VaultError> {
    if dto.id.trim().is_empty() {
        return Err(VaultError::Protocol("thread id is empty".into()));
    }
    let parent_id = dto.parent_thread_id().map(str::to_owned);
    let cwd = dto
        .cwd
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| VaultError::Protocol(format!("thread {} has no cwd", dto.id)))?;
    let title = dto
        .name
        .or(dto.title)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| dto.id.clone());
    let status = match dto.status {
        Some(status) => match status.kind.as_str() {
            "notLoaded" => RuntimeStatus::NotLoaded,
            "idle" => RuntimeStatus::Idle,
            "systemError" => RuntimeStatus::SystemError,
            "active" => RuntimeStatus::Active(status.active_flags),
            other => RuntimeStatus::Unknown(other.to_owned()),
        },
        None => RuntimeStatus::Unknown("missing".into()),
    };
    Ok(SessionNode {
        id: dto.id,
        title,
        project: dto.project_id,
        cwd,
        last_activity: dto.recency_at.or(dto.updated_at),
        archived,
        pinned: dto.is_pinned,
        status,
        parent_id,
        source,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session_node(id: &str, pinned: Option<bool>) -> SessionNode {
        SessionNode {
            id: id.into(),
            title: id.into(),
            project: None,
            cwd: "/tmp".into(),
            last_activity: Some(1),
            archived: false,
            pinned,
            status: RuntimeStatus::Idle,
            parent_id: None,
            source: SessionSource::Cli,
        }
    }

    fn create_state_database(path: &Path, rows: &[(&str, i64)]) {
        let connection = Connection::open(path).unwrap();
        connection
            .execute(
                "CREATE TABLE threads (id TEXT PRIMARY KEY, is_pinned INTEGER NOT NULL)",
                [],
            )
            .unwrap();
        for (id, pinned) in rows {
            connection
                .execute(
                    "INSERT INTO threads (id, is_pinned) VALUES (?1, ?2)",
                    (id, pinned),
                )
                .unwrap();
        }
    }

    #[test]
    fn recognizes_interactive_and_exec_sources() {
        assert_eq!(parse_source(&json!("cli")), SessionSource::Cli);
        assert_eq!(parse_source(&json!("vscode")), SessionSource::Vscode);
        assert_eq!(parse_source(&json!("exec")), SessionSource::Exec);
        assert_eq!(
            parse_source(&json!({"subAgent": "review"})),
            SessionSource::Descendant
        );
    }

    #[test]
    fn maps_parent_from_subagent_source() {
        let dto: ThreadDto = serde_json::from_value(json!({
            "id": "child",
            "cwd": "/tmp",
            "recencyAt": 1,
            "isPinned": false,
            "source": {
                "subAgent": {
                    "thread_spawn": {
                        "parent_thread_id": "root",
                        "depth": 1
                    }
                }
            },
            "status": {"type": "idle"}
        }))
        .unwrap();
        let node = map_thread(dto, false, SessionSource::Descendant).unwrap();
        assert_eq!(node.parent_id.as_deref(), Some("root"));
    }

    #[test]
    fn rejects_overlapping_pin_filter_results() {
        let dto: ThreadDto = serde_json::from_value(json!({
            "id": "root",
            "name": "root",
            "cwd": "/tmp",
            "recencyAt": 1,
            "source": "cli",
            "status": {"type": "idle"}
        }))
        .unwrap();
        let node = map_thread(dto, false, SessionSource::Cli).unwrap();
        let mut nodes = BTreeMap::from([("root".into(), node)]);
        let pinned = BTreeSet::from(["root".into()]);
        let unpinned = BTreeSet::from(["root".into()]);
        assert!(!apply_pin_partition(&mut nodes, &pinned, &unpinned));
        assert_eq!(nodes["root"].pinned, None);
    }

    #[test]
    fn complete_app_server_pin_fields_need_no_fallback() {
        let nodes = BTreeMap::from([
            ("one".into(), session_node("one", Some(false))),
            ("two".into(), session_node("two", Some(true))),
        ]);
        assert!(pin_states_complete(&nodes));
    }

    #[test]
    fn ignored_pin_filters_use_latest_read_only_state_database() {
        let directory = tempfile::tempdir().unwrap();
        create_state_database(
            &directory.path().join("state_4.sqlite"),
            &[("root", 1), ("child", 1)],
        );
        let database = directory.path().join("state_5.sqlite");
        create_state_database(&database, &[("root", 0), ("child", 1)]);
        let before = fs::read(&database).unwrap();
        let mut nodes = BTreeMap::from([
            ("root".into(), session_node("root", None)),
            ("child".into(), session_node("child", None)),
        ]);
        let overlapping = BTreeSet::from(["root".into(), "child".into()]);

        assert!(!apply_pin_partition(&mut nodes, &overlapping, &overlapping));
        apply_pin_state_fallback(&mut nodes, directory.path()).unwrap();

        assert_eq!(nodes["root"].pinned, Some(false));
        assert_eq!(nodes["child"].pinned, Some(true));
        assert_eq!(fs::read(&database).unwrap(), before);
    }

    #[test]
    fn pin_state_fallback_fails_closed_for_missing_or_incompatible_data() {
        let empty = tempfile::tempdir().unwrap();
        let mut nodes = BTreeMap::from([("root".into(), session_node("root", None))]);
        assert!(apply_pin_state_fallback(&mut nodes, empty.path()).is_err());
        assert_eq!(nodes["root"].pinned, None);

        let missing_column = tempfile::tempdir().unwrap();
        let connection = Connection::open(missing_column.path().join("state_1.sqlite")).unwrap();
        connection
            .execute("CREATE TABLE threads (id TEXT PRIMARY KEY)", [])
            .unwrap();
        drop(connection);
        assert!(apply_pin_state_fallback(&mut nodes, missing_column.path()).is_err());
        assert_eq!(nodes["root"].pinned, None);

        let invalid = tempfile::tempdir().unwrap();
        create_state_database(&invalid.path().join("state_2.sqlite"), &[("root", 2)]);
        assert!(apply_pin_state_fallback(&mut nodes, invalid.path()).is_err());
        assert_eq!(nodes["root"].pinned, None);

        let unreadable = tempfile::tempdir().unwrap();
        fs::write(
            unreadable.path().join("state_3.sqlite"),
            b"not a sqlite database",
        )
        .unwrap();
        assert!(apply_pin_state_fallback(&mut nodes, unreadable.path()).is_err());
        assert_eq!(nodes["root"].pinned, None);

        let incomplete = tempfile::tempdir().unwrap();
        create_state_database(&incomplete.path().join("state_3.sqlite"), &[("other", 0)]);
        assert!(apply_pin_state_fallback(&mut nodes, incomplete.path()).is_err());
        assert_eq!(nodes["root"].pinned, None);
    }
}
