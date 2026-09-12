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
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const NOTIFICATION_DRAIN_BUDGET: Duration = Duration::from_millis(250);
const NOTIFICATION_IDLE: Duration = Duration::from_millis(40);

pub struct AppServerClient {
    child: Child,
    stdin: ChildStdin,
    stdout: Lines<BufReader<ChildStdout>>,
    next_id: u64,
    notifications: Vec<Notification>,
    codex_state_dir: Result<PathBuf, String>,
    codex_path: PathBuf,
    usable: bool,
    capabilities_complete: bool,
    capability_diagnostics: Vec<String>,
    request_timeout: Duration,
    notification_drain_budget: Duration,
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
            codex_path: codex.to_owned(),
            usable: true,
            capabilities_complete: true,
            capability_diagnostics: Vec::new(),
            request_timeout: REQUEST_TIMEOUT,
            notification_drain_budget: NOTIFICATION_DRAIN_BUDGET,
        };
        client.initialize().await?;
        client.probe_required_methods().await?;
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
        match self.request_outcome(method, params).await? {
            RpcOutcome::Success(value) => Ok(value),
            RpcOutcome::Error { message, .. } => {
                Err(VaultError::Protocol(format!("{method}: {message}")))
            }
        }
    }

    async fn request_outcome(
        &mut self,
        method: &str,
        params: Value,
    ) -> Result<RpcOutcome, VaultError> {
        if !self.usable {
            return Err(VaultError::Unavailable(
                "app-server connection is no longer usable".into(),
            ));
        }
        let id = self.next_id;
        self.next_id += 1;
        let request_timeout = self.request_timeout;
        let result = timeout(request_timeout, async {
            self.send(json!({"method": method, "id": id, "params": params}))
                .await?;
            loop {
                let line = self
                    .stdout
                    .next_line()
                    .await
                    .map_err(|error| VaultError::Unavailable(error.to_string()))?
                    .ok_or_else(|| {
                        VaultError::Unavailable(format!(
                            "app-server exited while waiting for {method}"
                        ))
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
                    return Ok(RpcOutcome::Error {
                        code: error.get("code").and_then(Value::as_i64),
                        message: error
                            .get("message")
                            .and_then(Value::as_str)
                            .unwrap_or("unknown app-server error")
                            .to_owned(),
                    });
                }
                return value
                    .get("result")
                    .cloned()
                    .map(RpcOutcome::Success)
                    .ok_or_else(|| VaultError::Protocol(format!("{method}: missing result")));
            }
        })
        .await;
        match result {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(error)) => {
                self.invalidate();
                Err(error)
            }
            Err(_) => {
                self.invalidate();
                Err(VaultError::Unavailable(format!(
                    "app-server timed out while waiting for {method}"
                )))
            }
        }
    }

    fn invalidate(&mut self) {
        self.usable = false;
        let _ = self.child.start_kill();
    }

    async fn probe_required_methods(&mut self) -> Result<(), VaultError> {
        let probes = [
            (
                "thread/read",
                json!({"threadId": "", "includeTurns": false}),
            ),
            ("thread/archive", json!({"threadId": ""})),
            ("thread/unarchive", json!({"threadId": ""})),
            ("thread/delete", json!({"threadId": ""})),
        ];
        for (method, params) in probes {
            let outcome = self.request_outcome(method, params).await?;
            let supported = probe_establishes_support(&outcome);
            if !supported {
                self.capabilities_complete = false;
                self.capability_diagnostics.push(format!(
                    "required app-server method is unavailable: {method}"
                ));
            }
        }
        Ok(())
    }

    async fn restart_if_needed(&mut self) -> Result<(), VaultError> {
        if self.usable {
            return Ok(());
        }
        let replacement = Self::spawn(&self.codex_path).await?;
        *self = replacement;
        Ok(())
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
        let drain_budget = self.notification_drain_budget;
        let result = timeout(drain_budget, async {
            while let Ok(result) = timeout(NOTIFICATION_IDLE, self.stdout.next_line()).await {
                let Some(line) =
                    result.map_err(|error| VaultError::Unavailable(error.to_string()))?
                else {
                    break;
                };
                let value: Value = serde_json::from_str(&line)
                    .map_err(|error| VaultError::Protocol(error.to_string()))?;
                self.capture_notification(&value);
            }
            Ok(())
        })
        .await;
        match result {
            Ok(Ok(())) | Err(_) => Ok(()),
            Ok(Err(error)) => {
                self.invalidate();
                Err(error)
            }
        }
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
        let mut diagnostics = self.capability_diagnostics.clone();
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

        let all_ids = raw.keys().cloned().collect::<BTreeSet<_>>();
        let explicit_pin_states = raw
            .iter()
            .filter_map(|(id, (dto, _))| dto.is_pinned.map(|value| (id.clone(), value)))
            .collect::<BTreeMap<_, _>>();
        for (id, (dto, _)) in &raw {
            let source = parse_source(&dto.source);
            if !source.is_known()
                || (source == SessionSource::Descendant && dto.parent_thread_id().is_none())
            {
                relation_complete = false;
                diagnostics.push(format!(
                    "thread {id} has an unknown or unverifiable source: {}",
                    dto.source
                ));
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
        let mut write_capable = relation_complete && self.capabilities_complete;
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
                Ok((node, warnings)) => {
                    diagnostics.extend(warnings);
                    nodes.insert(id.clone(), node);
                }
                Err(error) => {
                    write_capable = false;
                    diagnostics.push(error.to_string());
                }
            }
        }

        let mut pinned_ids = BTreeSet::new();
        let mut unpinned_ids = BTreeSet::new();
        let mut pin_diagnostics = Vec::new();
        let mut filters_available = true;
        for archived in [false, true] {
            match self.list_all(archived, None, Some(true), true, true).await {
                Ok(values) => pinned_ids.extend(values.into_iter().map(|value| value.id)),
                Err(error) => {
                    filters_available = false;
                    pin_diagnostics.push(format!("isPinned=true filter unavailable: {error}"));
                }
            }
            match self.list_all(archived, None, Some(false), true, true).await {
                Ok(values) => unpinned_ids.extend(values.into_iter().map(|value| value.id)),
                Err(error) => {
                    filters_available = false;
                    pin_diagnostics.push(format!("isPinned=false filter unavailable: {error}"));
                }
            }
        }
        if !self.usable {
            return Err(VaultError::Unavailable(
                "app-server connection failed while validating pin filters".into(),
            ));
        }
        let filtered = if filters_available {
            resolve_pin_partition(&all_ids, &explicit_pin_states, &pinned_ids, &unpinned_ids)
        } else {
            Err("app-server pin filters are unavailable".into())
        };
        let resolved = match filtered {
            Ok(values) => Ok(values),
            Err(error) => {
                pin_diagnostics.push(error);
                self.codex_state_dir
                    .as_ref()
                    .map_err(Clone::clone)
                    .and_then(|directory| read_pin_state_fallback(directory, &all_ids))
                    .and_then(|values| {
                        validate_explicit_pin_states(&values, &explicit_pin_states)?;
                        Ok(values)
                    })
            }
        };
        match resolved {
            Ok(values) => {
                for node in nodes.values_mut() {
                    node.pinned = values.get(&node.id).copied();
                }
            }
            Err(error) => {
                write_capable = false;
                diagnostics.extend(pin_diagnostics);
                diagnostics.push(format!(
                    "read-only Codex pin-state fallback unavailable: {error}"
                ));
                diagnostics.push(
                    "pin state cannot be proven; all state-changing operations are disabled".into(),
                );
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

fn probe_establishes_support(outcome: &RpcOutcome) -> bool {
    matches!(
        outcome,
        RpcOutcome::Error {
            code: Some(-32602),
            ..
        }
    )
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

fn resolve_pin_partition(
    expected_ids: &BTreeSet<String>,
    explicit: &BTreeMap<String, bool>,
    pinned_ids: &BTreeSet<String>,
    unpinned_ids: &BTreeSet<String>,
) -> Result<BTreeMap<String, bool>, String> {
    if pinned_ids.iter().any(|id| unpinned_ids.contains(id)) {
        return Err("app-server pin filters overlap".into());
    }
    let mut resolved = BTreeMap::new();
    for id in expected_ids {
        let value = if pinned_ids.contains(id) {
            true
        } else if unpinned_ids.contains(id) {
            false
        } else {
            return Err(format!("app-server pin filters do not cover thread {id}"));
        };
        resolved.insert(id.clone(), value);
    }
    if pinned_ids
        .union(unpinned_ids)
        .any(|id| !expected_ids.contains(id))
    {
        return Err("app-server pin filters returned an unexpected thread".into());
    }
    validate_explicit_pin_states(&resolved, explicit)?;
    Ok(resolved)
}

fn validate_explicit_pin_states(
    resolved: &BTreeMap<String, bool>,
    explicit: &BTreeMap<String, bool>,
) -> Result<(), String> {
    for (id, observed) in explicit {
        if resolved.get(id) != Some(observed) {
            return Err(format!(
                "explicit pin state conflicts with the verified source for {id}"
            ));
        }
    }
    Ok(())
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
    let mut values = BTreeMap::new();
    for chunk in expected_ids.iter().collect::<Vec<_>>().chunks(500) {
        let placeholders = (1..=chunk.len())
            .map(|index| format!("?{index}"))
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!("SELECT id, is_pinned FROM threads WHERE id IN ({placeholders})");
        let mut statement = connection
            .prepare(&sql)
            .map_err(|error| format!("threads(id, is_pinned) is unavailable: {error}"))?;
        let mut rows = statement
            .query(rusqlite::params_from_iter(
                chunk.iter().map(|id| id.as_str()),
            ))
            .map_err(|error| format!("cannot query threads(id, is_pinned): {error}"))?;
        while let Some(row) = rows
            .next()
            .map_err(|error| format!("cannot read pin-state row: {error}"))?
        {
            let id: String = row
                .get(0)
                .map_err(|error| format!("thread id is not text: {error}"))?;
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

fn read_pin_state_fallback(
    directory: &Path,
    expected_ids: &BTreeSet<String>,
) -> Result<BTreeMap<String, bool>, String> {
    let database = find_state_database(directory)?;
    read_pin_states(&database, expected_ids)
}

impl SessionGateway for AppServerClient {
    fn scan(&mut self) -> PortFuture<'_, ScanSnapshot> {
        Box::pin(async move {
            self.restart_if_needed().await?;
            self.scan_inner().await
        })
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

enum RpcOutcome {
    Success(Value),
    Error { code: Option<i64>, message: String },
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
    recency_at: Option<Value>,
    #[serde(default)]
    updated_at: Option<Value>,
    #[serde(default)]
    created_at: Option<Value>,
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
        Some("appServer") => SessionSource::AppServer,
        Some(
            "subAgent"
            | "subAgentReview"
            | "subAgentCompact"
            | "subAgentThreadSpawn"
            | "subAgentOther",
        ) => SessionSource::Descendant,
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
) -> Result<(SessionNode, Vec<String>), VaultError> {
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
    let mut warnings = Vec::new();
    let timestamps = [
        ("recencyAt", dto.recency_at),
        ("updatedAt", dto.updated_at),
        ("createdAt", dto.created_at),
    ];
    let mut last_activity = None;
    for (field, raw) in timestamps {
        let Some(raw) = raw else {
            continue;
        };
        match parse_timestamp(&raw) {
            Some(value) => {
                if last_activity.is_none() {
                    last_activity = Some(value);
                }
            }
            None => warnings.push(format!(
                "thread {} has an invalid {field} value: {raw}",
                dto.id
            )),
        }
    }
    Ok((
        SessionNode {
            id: dto.id,
            title,
            project: dto.project_id,
            cwd,
            last_activity,
            archived,
            pinned: dto.is_pinned,
            status,
            parent_id,
            source,
        },
        warnings,
    ))
}

fn parse_timestamp(value: &Value) -> Option<i64> {
    match value {
        Value::Number(number) => number.as_i64(),
        Value::String(value) => value.parse::<i64>().ok().or_else(|| {
            chrono::DateTime::parse_from_rfc3339(value)
                .ok()
                .map(|value| value.timestamp())
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    #[cfg(unix)]
    async fn spawn_mock_client(
        after_probes: &str,
        missing_probe: Option<u64>,
    ) -> (tempfile::TempDir, AppServerClient) {
        fn probe_response(id: u64, missing_probe: Option<u64>) -> String {
            let code = if missing_probe == Some(id) {
                -32601
            } else {
                -32602
            };
            format!(
                "if printf '%s' \"$line\" | grep -q '\"threadId\":\"\"'; then \
                 printf '%s\\n' '{{\"id\":{id},\"error\":{{\"code\":{code},\"message\":\"probe\"}}}}'; \
                 else printf '%s\\n' '{{\"id\":{id},\"error\":{{\"code\":-32603,\"message\":\"unsafe probe\"}}}}'; fi"
            )
        }

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("mock-codex");
        let script = format!(
            "#!/bin/sh\n\
             n=0\n\
             while IFS= read -r line; do\n\
               n=$((n + 1))\n\
               case \"$n\" in\n\
                 1) printf '%s\\n' '{{\"id\":1,\"result\":{{}}}}' ;;\n\
                 2) : ;;\n\
                 3) {} ;;\n\
                 4) {} ;;\n\
                 5) {} ;;\n\
                 6) {} ;;\n\
                 7) {} ;;\n\
               esac\n\
             done\n",
            probe_response(2, missing_probe),
            probe_response(3, missing_probe),
            probe_response(4, missing_probe),
            probe_response(5, missing_probe),
            after_probes,
        );
        fs::write(&path, script).unwrap();
        let mut permissions = fs::metadata(&path).unwrap().permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(&path, permissions).unwrap();
        let client = AppServerClient::spawn(&path).await.unwrap();
        (directory, client)
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
        let node = map_thread(dto, false, SessionSource::Descendant).unwrap().0;
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
        let node = map_thread(dto, false, SessionSource::Cli).unwrap().0;
        let expected = BTreeSet::from([node.id]);
        let pinned = BTreeSet::from(["root".into()]);
        let unpinned = BTreeSet::from(["root".into()]);
        assert!(resolve_pin_partition(&expected, &BTreeMap::new(), &pinned, &unpinned).is_err());
    }

    #[test]
    fn complete_explicit_pin_fields_are_still_checked_against_filters() {
        let expected = BTreeSet::from(["one".into(), "two".into()]);
        let explicit = BTreeMap::from([("one".into(), false), ("two".into(), true)]);
        let pinned = BTreeSet::from(["one".into()]);
        let unpinned = BTreeSet::from(["two".into()]);
        assert!(resolve_pin_partition(&expected, &explicit, &pinned, &unpinned).is_err());
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
        let expected = BTreeSet::from(["root".into(), "child".into()]);
        let overlapping = BTreeSet::from(["root".into(), "child".into()]);

        assert!(
            resolve_pin_partition(&expected, &BTreeMap::new(), &overlapping, &overlapping).is_err()
        );
        let states = read_pin_state_fallback(directory.path(), &expected).unwrap();

        assert!(!states["root"]);
        assert!(states["child"]);
        assert_eq!(fs::read(&database).unwrap(), before);
    }

    #[test]
    fn pin_state_fallback_fails_closed_for_missing_or_incompatible_data() {
        let empty = tempfile::tempdir().unwrap();
        let expected = BTreeSet::from(["root".into()]);
        assert!(read_pin_state_fallback(empty.path(), &expected).is_err());

        let missing_column = tempfile::tempdir().unwrap();
        let connection = Connection::open(missing_column.path().join("state_1.sqlite")).unwrap();
        connection
            .execute("CREATE TABLE threads (id TEXT PRIMARY KEY)", [])
            .unwrap();
        drop(connection);
        assert!(read_pin_state_fallback(missing_column.path(), &expected).is_err());

        let invalid = tempfile::tempdir().unwrap();
        create_state_database(&invalid.path().join("state_2.sqlite"), &[("root", 2)]);
        assert!(read_pin_state_fallback(invalid.path(), &expected).is_err());

        let unreadable = tempfile::tempdir().unwrap();
        fs::write(
            unreadable.path().join("state_3.sqlite"),
            b"not a sqlite database",
        )
        .unwrap();
        assert!(read_pin_state_fallback(unreadable.path(), &expected).is_err());

        let incomplete = tempfile::tempdir().unwrap();
        create_state_database(&incomplete.path().join("state_3.sqlite"), &[("other", 0)]);
        assert!(read_pin_state_fallback(incomplete.path(), &expected).is_err());
    }

    #[test]
    fn fallback_validates_all_scanned_ids_and_explicit_conflicts() {
        let directory = tempfile::tempdir().unwrap();
        create_state_database(
            &directory.path().join("state_1.sqlite"),
            &[("root", 0), ("child", 1)],
        );
        let expected = BTreeSet::from(["root".into(), "child".into()]);
        let states = read_pin_state_fallback(directory.path(), &expected).unwrap();
        assert!(
            validate_explicit_pin_states(&states, &BTreeMap::from([("root".into(), true)]))
                .is_err()
        );

        let all_scanned = BTreeSet::from(["root".into(), "child".into(), "other".into()]);
        assert!(read_pin_state_fallback(directory.path(), &all_scanned).is_err());
    }

    #[test]
    fn parses_numeric_rfc3339_and_created_at_timestamps() {
        assert_eq!(parse_timestamp(&json!("123")), Some(123));
        assert_eq!(parse_timestamp(&json!("1970-01-01T00:02:03Z")), Some(123));
        let dto: ThreadDto = serde_json::from_value(json!({
            "id": "root",
            "cwd": "/tmp",
            "createdAt": "1970-01-01T00:02:03Z",
            "isPinned": false,
            "source": "cli",
            "status": {"type": "idle"}
        }))
        .unwrap();
        assert_eq!(
            map_thread(dto, false, SessionSource::Cli)
                .unwrap()
                .0
                .last_activity,
            Some(123)
        );
    }

    #[test]
    fn unknown_sources_and_missing_methods_fail_capability_checks() {
        assert!(!parse_source(&json!("futureHost")).is_known());
        assert!(!parse_source(&json!("subAgentFuture")).is_known());
        assert!(!parse_source(&Value::Null).is_known());
        assert!(!probe_establishes_support(&RpcOutcome::Error {
            code: Some(-32601),
            message: "method not found".into(),
        }));
        assert!(probe_establishes_support(&RpcOutcome::Error {
            code: Some(-32602),
            message: "invalid params".into(),
        }));
        assert!(!probe_establishes_support(&RpcOutcome::Error {
            code: Some(-32603),
            message: "internal error".into(),
        }));
        assert!(!probe_establishes_support(&RpcOutcome::Error {
            code: None,
            message: "permission denied".into(),
        }));
        assert!(!probe_establishes_support(&RpcOutcome::Success(json!({}))));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn every_required_method_is_probed_independently() {
        let methods = [
            (2, "thread/read"),
            (3, "thread/archive"),
            (4, "thread/unarchive"),
            (5, "thread/delete"),
        ];
        for (id, method) in methods {
            let (_directory, client) = spawn_mock_client(":", Some(id)).await;
            assert!(!client.capabilities_complete);
            assert!(
                client
                    .capability_diagnostics
                    .iter()
                    .any(|value| value.contains(method))
            );
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn required_method_probes_use_protocol_invalid_empty_thread_ids() {
        let (_directory, client) = spawn_mock_client(":", None).await;
        assert!(client.capabilities_complete);
        assert!(client.capability_diagnostics.is_empty());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn nonresponsive_request_times_out_and_invalidates_connection() {
        let (_directory, mut client) = spawn_mock_client("sleep 5", None).await;
        client.request_timeout = Duration::from_millis(100);
        let started = tokio::time::Instant::now();
        assert!(client.scan().await.is_err());
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(!client.usable);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn pin_filter_transport_failure_cannot_fall_back_to_write_capable() {
        let body = "printf '%s\\n' '{\"id\":6,\"result\":{\"data\":[{\"id\":\"root\",\"cwd\":\"/tmp\",\"recencyAt\":1,\"isPinned\":false,\"source\":\"cli\",\"status\":{\"type\":\"idle\"}}],\"nextCursor\":null}}'; \
                    IFS= read -r line; printf '%s\\n' '{\"id\":7,\"result\":{\"data\":[],\"nextCursor\":null}}'; \
                    IFS= read -r line; printf '%s\\n' '{\"id\":8,\"result\":{\"data\":[],\"nextCursor\":null}}'; \
                    IFS= read -r line; printf '%s\\n' '{\"id\":9,\"result\":{\"data\":[],\"nextCursor\":null}}'; \
                    IFS= read -r line; sleep 5";
        let (_mock_directory, mut client) = spawn_mock_client(body, None).await;
        let state_directory = tempfile::tempdir().unwrap();
        create_state_database(
            &state_directory.path().join("state_1.sqlite"),
            &[("root", 0)],
        );
        client.codex_state_dir = Ok(state_directory.path().to_owned());
        client.request_timeout = Duration::from_millis(100);
        assert!(client.scan().await.is_err());
        assert!(!client.usable);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn notification_drain_has_a_total_budget_under_continuous_traffic() {
        let body = "printf '%s\\n' '{\"id\":6,\"result\":{}}'; \
                    i=0; while [ \"$i\" -lt 100 ]; do \
                    printf '%s\\n' '{\"method\":\"thread/archived\",\"params\":{\"threadId\":\"root\"}}'; \
                    sleep 0.01; i=$((i + 1)); done";
        let (_directory, mut client) = spawn_mock_client(body, None).await;
        client.notification_drain_budget = Duration::from_millis(80);
        let started = tokio::time::Instant::now();
        let ack = client.mutate(Action::Archive, "root").await.unwrap();
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(ack.response_received);
        assert!(ack.notification_seen);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn response_followed_by_process_exit_is_reported_and_invalidated() {
        let body =
            "printf '%s\\n' '{\"id\":6,\"result\":{\"data\":[],\"nextCursor\":null}}'; exit 0";
        let (_directory, mut client) = spawn_mock_client(body, None).await;
        client.request_timeout = Duration::from_millis(200);
        assert!(client.scan().await.is_err());
        assert!(!client.usable);
    }
}
