use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs, io,
    io::{BufRead as _, Write as _},
    path::{Path, PathBuf},
    pin::Pin,
    process::Stdio,
    task::{Context, Poll},
    time::{Duration, UNIX_EPOCH},
};

use futures_util::{SinkExt, StreamExt};
use rusqlite::{Connection, DatabaseName, OpenFlags, params, types::ValueRef};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, Lines, ReadBuf},
    process::{Child, ChildStdin, ChildStdout, Command},
    time::{sleep, timeout},
};
use tokio_tungstenite::{WebSocketStream, client_async, tungstenite::Message};

use crate::domain::{
    Action, MaintenanceAck, MaintenanceCandidate, MaintenanceKind, MutationAck, PortFuture,
    RuntimeStatus, ScanSnapshot, SessionGateway, SessionNode, SessionSource, VaultError,
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
const THREAD_READ_BATCH_SIZE: usize = 64;
const APP_SERVER_SOCKET_RELATIVE_PATH: &str = "app-server-control/app-server-control.sock";
const CLI_ERROR_LIMIT: usize = 512;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionMode {
    Auto,
    Attached,
    Managed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ActiveConnection {
    Attached(PathBuf),
    Managed,
}

pub struct AppServerClient {
    transport: AppServerTransport,
    next_id: u64,
    codex_home: Result<PathBuf, String>,
    codex_state_dir: Result<PathBuf, String>,
    codex_path: PathBuf,
    usable: bool,
    capabilities_complete: bool,
    capability_diagnostics: Vec<String>,
    request_timeout: Duration,
    requested_connection: ConnectionMode,
    target_summary: String,
    known_rollout_paths: BTreeMap<String, PathBuf>,
    pending_delete_paths: BTreeMap<String, PathBuf>,
    maintenance_candidates: Vec<MaintenanceCandidate>,
    maintenance_backup_roots: BTreeMap<String, PathBuf>,
}

#[derive(Debug, Clone)]
struct CatalogThread {
    id: String,
    title: Option<String>,
    cwd: String,
    project: Option<String>,
    provider: Option<String>,
    model: Option<String>,
    archived: bool,
    pinned: bool,
    last_activity_ms: i64,
    rollout_path: PathBuf,
    source: Value,
    parent_id: Option<String>,
}

#[derive(Debug)]
struct CatalogSnapshot {
    threads: BTreeMap<String, CatalogThread>,
    ignored_orphan_edges: usize,
    unresolved_child_edges: usize,
    stale_edges: Vec<StaleSpawnEdge>,
}

#[derive(Debug, Clone)]
struct StaleSpawnEdge {
    parent: String,
    child: String,
    status: String,
}

struct MaintenanceDiscovery {
    active_rollouts: usize,
    archived_rollouts: usize,
    candidates: Vec<MaintenanceCandidate>,
}

impl CatalogThread {
    fn fallback_dto(&self, reason: String) -> ThreadDto {
        ThreadDto {
            id: self.id.clone(),
            name: None,
            title: self.title.clone(),
            cwd: Some(self.cwd.clone()),
            project_id: self.project.clone(),
            recency_at: Some(Value::from(self.last_activity_ms / 1_000)),
            updated_at: None,
            created_at: None,
            is_pinned: Some(self.pinned),
            parent_thread_id: self.parent_id.clone(),
            source: self.source.clone(),
            status: Some(StatusDto {
                kind: format!("catalogOnly:{reason}"),
                active_flags: Vec::new(),
            }),
        }
    }

    fn apply_to_dto(&self, dto: &mut ThreadDto) {
        dto.title = self.title.clone().or(dto.title.take());
        dto.cwd = Some(self.cwd.clone());
        dto.project_id = self.project.clone();
        dto.recency_at = Some(Value::from(self.last_activity_ms / 1_000));
        dto.is_pinned = Some(self.pinned);
        dto.parent_thread_id = self.parent_id.clone();
        dto.source = self.source.clone();
    }
}

enum AppServerTransport {
    Attached {
        child: Child,
        websocket: WebSocketStream<ProxyStream>,
    },
    Managed {
        child: Child,
        stdin: ChildStdin,
        stdout: Lines<BufReader<ChildStdout>>,
    },
}

struct ProxyStream {
    reader: ChildStdout,
    writer: ChildStdin,
}

impl AsyncRead for ProxyStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.reader).poll_read(context, buffer)
    }
}

impl AsyncWrite for ProxyStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        Pin::new(&mut self.writer).poll_write(context, buffer)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.writer).poll_flush(context)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.writer).poll_shutdown(context)
    }
}

impl AppServerTransport {
    async fn attach(codex: &Path, socket: &Path) -> Result<Self, VaultError> {
        let mut child = spawn_with_retry(|| {
            let mut command = Command::new(codex);
            command
                .arg("app-server")
                .arg("proxy")
                .arg("--sock")
                .arg(socket)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .kill_on_drop(true);
            command
        })
        .await
        .map_err(|error| VaultError::Unavailable(error.to_string()))?;
        let writer = child
            .stdin
            .take()
            .ok_or_else(|| VaultError::Unavailable("app-server proxy stdin unavailable".into()))?;
        let reader = child
            .stdout
            .take()
            .ok_or_else(|| VaultError::Unavailable("app-server proxy stdout unavailable".into()))?;
        let (websocket, _) = client_async("ws://localhost/", ProxyStream { reader, writer })
            .await
            .map_err(|error| {
                VaultError::Unavailable(format!(
                    "app-server control socket WebSocket handshake failed: {error}"
                ))
            })?;
        Ok(Self::Attached { child, websocket })
    }

    async fn spawn(codex: &Path) -> Result<Self, VaultError> {
        let mut child = spawn_with_retry(|| {
            let mut command = Command::new(codex);
            command
                .arg("app-server")
                .arg("--stdio")
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .kill_on_drop(true);
            command
        })
        .await
        .map_err(|error| VaultError::Unavailable(error.to_string()))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| VaultError::Unavailable("app-server stdin unavailable".into()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| VaultError::Unavailable("app-server stdout unavailable".into()))?;
        Ok(Self::Managed {
            child,
            stdin,
            stdout: BufReader::new(stdout).lines(),
        })
    }

    async fn send(&mut self, value: &Value) -> Result<(), VaultError> {
        match self {
            Self::Attached { websocket, .. } => {
                let encoded = serde_json::to_string(value)
                    .map_err(|error| VaultError::Protocol(error.to_string()))?;
                websocket
                    .send(Message::Text(encoded.into()))
                    .await
                    .map_err(|error| VaultError::Unavailable(error.to_string()))
            }
            Self::Managed { stdin, .. } => {
                let mut encoded = serde_json::to_vec(value)
                    .map_err(|error| VaultError::Protocol(error.to_string()))?;
                encoded.push(b'\n');
                stdin
                    .write_all(encoded.as_slice())
                    .await
                    .map_err(|error| VaultError::Unavailable(error.to_string()))?;
                stdin
                    .flush()
                    .await
                    .map_err(|error| VaultError::Unavailable(error.to_string()))
            }
        }
    }

    async fn next_value(&mut self) -> Result<Option<Value>, VaultError> {
        match self {
            Self::Attached { websocket, .. } => loop {
                match websocket.next().await {
                    Some(Ok(Message::Text(text))) => {
                        return serde_json::from_str(text.as_ref())
                            .map(Some)
                            .map_err(|error| VaultError::Protocol(error.to_string()));
                    }
                    Some(Ok(Message::Binary(bytes))) => {
                        return serde_json::from_slice(bytes.as_ref())
                            .map(Some)
                            .map_err(|error| VaultError::Protocol(error.to_string()));
                    }
                    Some(Ok(Message::Ping(bytes))) => websocket
                        .send(Message::Pong(bytes))
                        .await
                        .map_err(|error| VaultError::Unavailable(error.to_string()))?,
                    Some(Ok(Message::Pong(_) | Message::Frame(_))) => {}
                    Some(Ok(Message::Close(_))) | None => return Ok(None),
                    Some(Err(error)) => {
                        return Err(VaultError::Unavailable(error.to_string()));
                    }
                }
            },
            Self::Managed { stdout, .. } => stdout
                .next_line()
                .await
                .map_err(|error| VaultError::Unavailable(error.to_string()))?
                .map(|line| {
                    serde_json::from_str(&line)
                        .map_err(|error| VaultError::Protocol(error.to_string()))
                })
                .transpose(),
        }
    }

    fn terminate(&mut self) {
        match self {
            Self::Attached { child, .. } | Self::Managed { child, .. } => {
                let _ = child.start_kill();
            }
        }
    }
}

async fn spawn_with_retry(mut build: impl FnMut() -> Command) -> Result<Child, io::Error> {
    const MAX_ATTEMPTS: usize = 4;
    for attempt in 1..=MAX_ATTEMPTS {
        match build().spawn() {
            Ok(child) => return Ok(child),
            Err(error) if error.raw_os_error() == Some(26) && attempt < MAX_ATTEMPTS => {
                sleep(Duration::from_millis(10)).await;
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("spawn retry loop always returns")
}

impl AppServerClient {
    pub async fn spawn(codex: &Path) -> Result<Self, VaultError> {
        Self::connect(codex, ConnectionMode::Managed).await
    }

    pub async fn connect(codex: &Path, mode: ConnectionMode) -> Result<Self, VaultError> {
        let codex_home = default_codex_state_dir();
        let codex_state_dir = codex_home
            .as_ref()
            .map_err(Clone::clone)
            .and_then(|home| default_codex_sqlite_dir(home));
        let socket = codex_home
            .as_ref()
            .ok()
            .map(|directory| directory.join(APP_SERVER_SOCKET_RELATIVE_PATH));
        let active = select_connection(mode, socket.as_deref())?;
        Self::spawn_connection(codex, mode, active, codex_home, codex_state_dir).await
    }

    async fn spawn_connection(
        codex: &Path,
        requested_connection: ConnectionMode,
        active_connection: ActiveConnection,
        codex_home: Result<PathBuf, String>,
        codex_state_dir: Result<PathBuf, String>,
    ) -> Result<Self, VaultError> {
        let transport = match &active_connection {
            ActiveConnection::Attached(socket) => AppServerTransport::attach(codex, socket).await?,
            ActiveConnection::Managed => AppServerTransport::spawn(codex).await?,
        };
        let mut client = Self {
            transport,
            next_id: 1,
            target_summary: connection_target_summary(
                &active_connection,
                &codex_home,
                &codex_state_dir,
            ),
            codex_home,
            codex_state_dir,
            codex_path: codex.to_owned(),
            usable: true,
            capabilities_complete: true,
            capability_diagnostics: Vec::new(),
            request_timeout: REQUEST_TIMEOUT,
            requested_connection,
            known_rollout_paths: BTreeMap::new(),
            pending_delete_paths: BTreeMap::new(),
            maintenance_candidates: Vec::new(),
            maintenance_backup_roots: BTreeMap::new(),
        };
        client.initialize().await?;
        client.probe_required_methods().await?;
        client.probe_required_cli_commands().await;
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
        self.transport.send(&value).await
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
                let value = self.transport.next_value().await?.ok_or_else(|| {
                    VaultError::Unavailable(format!("app-server exited while waiting for {method}"))
                })?;
                if value.get("method").is_some() {
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
        self.transport.terminate();
    }

    async fn probe_required_methods(&mut self) -> Result<(), VaultError> {
        let probes = [(
            "thread/read",
            json!({"threadId": "", "includeTurns": false}),
        )];
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

    async fn probe_required_cli_commands(&mut self) {
        for command in ["archive", "unarchive", "delete"] {
            let result = timeout(self.request_timeout, async {
                let mut child = spawn_with_retry(|| {
                    let mut process = Command::new(&self.codex_path);
                    process
                        .arg(command)
                        .arg("--help")
                        .stdin(Stdio::null())
                        .stdout(Stdio::null())
                        .stderr(Stdio::null())
                        .kill_on_drop(true);
                    process
                })
                .await?;
                child.wait().await
            })
            .await;
            let supported = matches!(result, Ok(Ok(status)) if status.success());
            if !supported {
                self.capabilities_complete = false;
                self.capability_diagnostics.push(format!(
                    "required Codex CLI command is unavailable: codex {command}"
                ));
            }
        }
    }

    async fn run_cli_mutation(
        &mut self,
        action: Action,
        thread_id: &str,
    ) -> Result<MutationAck, VaultError> {
        let command = match action {
            Action::Archive => "archive",
            Action::Restore => "unarchive",
            Action::Delete => "delete",
            Action::Cleanup => {
                return Err(VaultError::Blocked(
                    "dirty-data cleanup does not use the Codex CLI mutation path".into(),
                ));
            }
        };
        let child = spawn_with_retry(|| {
            let mut process = Command::new(&self.codex_path);
            process.arg(command);
            if action == Action::Delete {
                process.arg("--force");
            }
            process
                .arg(thread_id)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .kill_on_drop(true);
            process
        })
        .await
        .map_err(|error| VaultError::Command(format!("codex {command}: cannot start: {error}")))?;
        let output = timeout(self.request_timeout, child.wait_with_output())
            .await
            .map_err(|_| VaultError::Command(format!("codex {command}: timed out")))?
            .map_err(|error| {
                VaultError::Command(format!(
                    "codex {command}: cannot wait for completion: {error}"
                ))
            })?;
        if !output.status.success() {
            let detail = cli_error_detail(&output.stderr, &output.stdout);
            return Err(VaultError::Command(format!(
                "codex {command} exited with {}{detail}",
                output.status
            )));
        }
        if action == Action::Delete
            && let Some(path) = self.known_rollout_paths.get(thread_id)
        {
            self.pending_delete_paths
                .insert(thread_id.to_owned(), path.clone());
        }
        Ok(MutationAck {
            response_received: true,
            notification_seen: false,
        })
    }

    async fn restart_if_needed(&mut self) -> Result<(), VaultError> {
        if self.usable {
            return Ok(());
        }
        let replacement = Self::connect(&self.codex_path, self.requested_connection).await?;
        *self = replacement;
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

    async fn read_thread_metadata_batch(
        &mut self,
        thread_ids: &[String],
    ) -> Result<BTreeMap<String, Result<ThreadDto, VaultError>>, VaultError> {
        if !self.usable {
            return Err(VaultError::Unavailable(
                "app-server connection is no longer usable".into(),
            ));
        }
        let request_timeout = self.request_timeout;
        let result = timeout(request_timeout, async {
            let mut pending = BTreeMap::new();
            for thread_id in thread_ids {
                let request_id = self.next_id;
                self.next_id += 1;
                self.send(json!({
                    "method": "thread/read",
                    "id": request_id,
                    "params": {"threadId": thread_id, "includeTurns": false}
                }))
                .await?;
                pending.insert(request_id, thread_id.clone());
            }

            let mut resolved = BTreeMap::new();
            while !pending.is_empty() {
                let value = self.transport.next_value().await?.ok_or_else(|| {
                    VaultError::Unavailable(
                        "app-server exited while waiting for thread/read batch".into(),
                    )
                })?;
                if value.get("method").is_some() {
                    continue;
                }
                let Some(request_id) = value.get("id").and_then(Value::as_u64) else {
                    continue;
                };
                let Some(thread_id) = pending.remove(&request_id) else {
                    continue;
                };
                if let Some(error) = value.get("error") {
                    let message = error
                        .get("message")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown app-server error");
                    resolved.insert(
                        thread_id,
                        Err(VaultError::Protocol(format!("thread/read: {message}"))),
                    );
                    continue;
                }
                let response = value
                    .get("result")
                    .cloned()
                    .ok_or_else(|| VaultError::Protocol("thread/read: missing result".into()))?;
                let read = serde_json::from_value::<ThreadReadResponse>(response)
                    .map(|response| response.thread)
                    .map_err(|error| VaultError::Protocol(error.to_string()))
                    .and_then(|thread| {
                        if thread.id == thread_id {
                            Ok(thread)
                        } else {
                            Err(VaultError::Protocol(format!(
                                "thread/read returned {} while hydrating {thread_id}",
                                thread.id
                            )))
                        }
                    });
                resolved.insert(thread_id, read);
            }
            Ok(resolved)
        })
        .await;
        match result {
            Ok(Ok(values)) => Ok(values),
            Ok(Err(error)) => {
                self.invalidate();
                Err(error)
            }
            Err(_) => {
                self.invalidate();
                Err(VaultError::Unavailable(
                    "app-server timed out while waiting for thread/read batch".into(),
                ))
            }
        }
    }

    async fn hydrate_missing_parent_relations(
        &mut self,
        raw: &mut BTreeMap<String, (ThreadDto, bool)>,
        diagnostics: &mut Vec<String>,
    ) -> bool {
        let missing = raw
            .iter()
            .filter(|(_, (dto, _))| {
                parse_source(&dto.source) == SessionSource::Descendant
                    && dto.parent_thread_id().is_none()
            })
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        let mut complete = true;
        for batch in missing.chunks(THREAD_READ_BATCH_SIZE) {
            let mut reads = match self.read_thread_metadata_batch(batch).await {
                Ok(reads) => reads,
                Err(error) => {
                    complete = false;
                    diagnostics.push(format!(
                        "thread/read could not hydrate {} parent relationships: {error}",
                        batch.len()
                    ));
                    break;
                }
            };
            for id in batch {
                let read = reads.remove(id).unwrap_or_else(|| {
                    Err(VaultError::Protocol(format!(
                        "thread/read did not return metadata for {id}"
                    )))
                });
                match read {
                    Ok(read) => {
                        let parent_id = read.parent_thread_id().map(str::to_owned);
                        let read_source = parse_source(&read.source);
                        let Some((listed, _)) = raw.get_mut(id) else {
                            complete = false;
                            diagnostics.push(format!(
                                "thread {id} disappeared while hydrating its parent relationship"
                            ));
                            continue;
                        };
                        if parse_source(&listed.source) != read_source {
                            complete = false;
                            diagnostics.push(format!(
                                "thread {id} source changed between thread/list and thread/read"
                            ));
                        }
                        listed.source = read.source;
                        listed.parent_thread_id = parent_id;
                        if listed.parent_thread_id().is_none() {
                            complete = false;
                            diagnostics.push(format!(
                                "thread {id} remains an unparented sub-agent after thread/read"
                            ));
                        }
                    }
                    Err(error) => {
                        complete = false;
                        diagnostics.push(format!(
                            "thread/read could not hydrate the parent of {id}: {error}"
                        ));
                    }
                }
            }
        }
        complete
    }

    async fn scan_inner(&mut self) -> Result<ScanSnapshot, VaultError> {
        let mut diagnostics = self.capability_diagnostics.clone();
        let mut raw = BTreeMap::<String, (ThreadDto, bool)>::new();
        let mut relation_complete = true;
        let mut storage_complete = true;
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

        let catalog = self
            .codex_state_dir
            .as_ref()
            .map_err(Clone::clone)
            .and_then(|directory| find_state_database(directory))
            .and_then(|database| read_thread_catalog(&database).map(|catalog| (database, catalog)));
        let catalog = match catalog {
            Ok((database, catalog)) => {
                match self
                    .codex_home
                    .as_ref()
                    .map_err(Clone::clone)
                    .and_then(|home| discover_maintenance(home, &database, &catalog))
                {
                    Ok(discovery) => {
                        let orphaned = discovery
                            .candidates
                            .iter()
                            .filter(|candidate| {
                                candidate.kind == MaintenanceKind::UnreferencedRollout
                            })
                            .count();
                        if orphaned > 0 {
                            diagnostics.push(format!(
                                "recursive rollout scan found {} active and {} archived JSONL files; {orphaned} are not referenced by the task catalog",
                                discovery.active_rollouts, discovery.archived_rollouts
                            ));
                        }
                        self.maintenance_candidates = discovery.candidates;
                    }
                    Err(error) => {
                        self.maintenance_candidates.clear();
                        storage_complete = false;
                        diagnostics.push(format!("rollout root scan is incomplete: {error}"));
                    }
                }
                if catalog.ignored_orphan_edges > 0 {
                    diagnostics.push(format!(
                        "ignored {} stale spawn edges whose child is absent from the task catalog",
                        catalog.ignored_orphan_edges
                    ));
                }
                if catalog.unresolved_child_edges > 0 {
                    relation_complete = false;
                    diagnostics.push(format!(
                        "{} catalog tasks reference missing parent rows",
                        catalog.unresolved_child_edges
                    ));
                }
                let app_only = raw
                    .keys()
                    .filter(|id| !catalog.threads.contains_key(*id))
                    .count();
                if app_only > 0 {
                    diagnostics.push(format!(
                        "ignored {app_only} app-server rows absent from the authoritative task catalog"
                    ));
                }
                raw.retain(|id, _| catalog.threads.contains_key(id));
                let mut missing = Vec::new();
                for (id, thread) in &catalog.threads {
                    if let Some((dto, archived)) = raw.get_mut(id) {
                        thread.apply_to_dto(dto);
                        *archived = thread.archived;
                    } else {
                        missing.push(id.clone());
                    }
                }
                for batch in missing.chunks(THREAD_READ_BATCH_SIZE) {
                    let mut reads = self.read_thread_metadata_batch(batch).await?;
                    for id in batch {
                        let Some(thread) = catalog.threads.get(id) else {
                            continue;
                        };
                        let dto = match reads.remove(id) {
                            Some(Ok(mut dto)) => {
                                thread.apply_to_dto(&mut dto);
                                dto
                            }
                            Some(Err(error)) => {
                                diagnostics.push(format!(
                                    "thread {id} is catalogued but app-server metadata is unavailable: {error}"
                                ));
                                thread.fallback_dto("app-server metadata unavailable".into())
                            }
                            None => {
                                diagnostics.push(format!(
                                    "thread/read returned no result for catalogued thread {id}"
                                ));
                                thread.fallback_dto("app-server metadata missing".into())
                            }
                        };
                        raw.insert(id.clone(), (dto, thread.archived));
                    }
                }
                Some(catalog)
            }
            Err(error) => {
                self.maintenance_candidates.clear();
                diagnostics.push(format!(
                    "authoritative Codex task catalog is unavailable; discovery is compatibility-only: {error}"
                ));
                None
            }
        };

        if !self
            .hydrate_missing_parent_relations(&mut raw, &mut diagnostics)
            .await
        {
            relation_complete = false;
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

        for id in &included_ids {
            let Some((dto, _)) = raw.get(id) else {
                continue;
            };
            if !parse_source(&dto.source).is_known() {
                diagnostics.push(format!(
                    "thread {id} has an unknown source; only its interactive tree is protected: {}",
                    dto.source
                ));
            }
        }

        if catalog.is_none()
            && let Some(probe_root) = root_ids.first()
        {
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

        let all_ids = included_ids.clone();
        let explicit_pin_states = included_ids
            .iter()
            .filter_map(|id| {
                raw.get(id)
                    .and_then(|(dto, _)| dto.is_pinned.map(|value| (id.clone(), value)))
            })
            .collect::<BTreeMap<_, _>>();
        let mut nodes = BTreeMap::<String, SessionNode>::new();
        let mut write_capable = relation_complete && storage_complete && self.capabilities_complete;
        if catalog.is_some() {
            self.known_rollout_paths.clear();
        }
        for id in &included_ids {
            let Some((dto, archived)) = raw.remove(id) else {
                continue;
            };
            let parsed_source = parse_source(&dto.source);
            let source = if root_ids.contains(id) || !parsed_source.is_known() {
                parsed_source
            } else {
                SessionSource::Descendant
            };
            match map_thread(dto, archived, source) {
                Ok((mut node, warnings)) => {
                    diagnostics.extend(warnings);
                    if let Some(thread) =
                        catalog.as_ref().and_then(|catalog| catalog.threads.get(id))
                    {
                        node.provider = thread.provider.clone();
                        node.model = thread.model.clone();
                        self.known_rollout_paths
                            .insert(id.clone(), thread.rollout_path.clone());
                        match self
                            .codex_home
                            .as_ref()
                            .map_err(Clone::clone)
                            .and_then(|home| validate_rollout_path(home, thread))
                        {
                            Ok(bytes) => node.rollout_bytes = Some(bytes),
                            Err(error) => {
                                node.status = RuntimeStatus::Unknown(error.clone());
                                diagnostics.push(error);
                            }
                        }
                    }
                    nodes.insert(id.clone(), node);
                }
                Err(error) => {
                    write_capable = false;
                    diagnostics.push(error.to_string());
                }
            }
        }

        if let Some(catalog) = &catalog {
            for node in nodes.values_mut() {
                node.pinned = catalog.threads.get(&node.id).map(|thread| thread.pinned);
            }
        } else {
            let mut pinned_ids = BTreeSet::new();
            let mut unpinned_ids = BTreeSet::new();
            let mut pin_diagnostics = Vec::new();
            let mut filters_available = true;
            for archived in [false, true] {
                match self.list_all(archived, None, Some(true), true, true).await {
                    Ok(values) => pinned_ids.extend(
                        values
                            .into_iter()
                            .map(|value| value.id)
                            .filter(|id| all_ids.contains(id)),
                    ),
                    Err(error) => {
                        filters_available = false;
                        pin_diagnostics.push(format!("isPinned=true filter unavailable: {error}"));
                    }
                }
                match self.list_all(archived, None, Some(false), true, true).await {
                    Ok(values) => unpinned_ids.extend(
                        values
                            .into_iter()
                            .map(|value| value.id)
                            .filter(|id| all_ids.contains(id)),
                    ),
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
                        "pin state cannot be proven; all state-changing operations are disabled"
                            .into(),
                    );
                }
            }
        }

        if !relation_complete {
            write_capable = false;
        }
        if !self.pending_delete_paths.is_empty() {
            let verification_context =
                self.codex_home
                    .as_ref()
                    .map_err(Clone::clone)
                    .and_then(|home| {
                        self.codex_state_dir
                            .as_ref()
                            .map_err(Clone::clone)
                            .and_then(|sqlite_home| {
                                find_state_database(sqlite_home)
                                    .map(|database| (home.clone(), sqlite_home.clone(), database))
                            })
                    });
            let pending = self.pending_delete_paths.clone();
            let mut verified = Vec::new();
            for (id, rollout_path) in pending {
                let residue = verification_context
                    .as_ref()
                    .map_err(Clone::clone)
                    .and_then(|(home, sqlite_home, database)| {
                        deletion_residue(home, sqlite_home, database, &id, &rollout_path)
                    });
                match residue {
                    Ok(values) if values.is_empty() => verified.push(id),
                    Ok(values) => {
                        let reason = format!(
                            "delete cleanup is incomplete for {id}: {}",
                            values.join(", ")
                        );
                        diagnostics.push(reason.clone());
                        nodes.entry(id.clone()).or_insert_with(|| SessionNode {
                            id: id.clone(),
                            title: id,
                            project: None,
                            provider: None,
                            model: None,
                            cwd: rollout_path.display().to_string(),
                            rollout_bytes: None,
                            last_activity: None,
                            archived: true,
                            pinned: Some(false),
                            status: RuntimeStatus::Unknown(reason),
                            parent_id: None,
                            source: SessionSource::Other("cleanupResidue".into()),
                        });
                    }
                    Err(error) => {
                        let reason = format!("delete cleanup cannot be verified for {id}: {error}");
                        diagnostics.push(reason.clone());
                        nodes.entry(id.clone()).or_insert_with(|| SessionNode {
                            id: id.clone(),
                            title: id,
                            project: None,
                            provider: None,
                            model: None,
                            cwd: rollout_path.display().to_string(),
                            rollout_bytes: None,
                            last_activity: None,
                            archived: true,
                            pinned: Some(false),
                            status: RuntimeStatus::Unknown(reason),
                            parent_id: None,
                            source: SessionSource::Other("cleanupUnverified".into()),
                        });
                    }
                }
            }
            for id in verified {
                self.pending_delete_paths.remove(&id);
                self.known_rollout_paths.remove(&id);
            }
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
    // Current app-server releases classify the deliberately invalid empty thread ID as an
    // invalid request, while older releases reported invalid params. Both responses prove that
    // the method was dispatched; method-not-found and ambiguous failures remain fail-closed.
    matches!(
        outcome,
        RpcOutcome::Error {
            code: Some(-32600 | -32602),
            ..
        }
    )
}

fn select_connection(
    requested: ConnectionMode,
    socket: Option<&Path>,
) -> Result<ActiveConnection, VaultError> {
    match requested {
        ConnectionMode::Managed => Ok(ActiveConnection::Managed),
        ConnectionMode::Attached => socket
            .filter(|path| path.exists())
            .map(|path| ActiveConnection::Attached(path.to_owned()))
            .ok_or_else(|| {
                VaultError::Unavailable(
                    "no running app-server control socket is available for attached mode".into(),
                )
            }),
        ConnectionMode::Auto => Ok(socket
            .filter(|path| path.exists())
            .map(|path| ActiveConnection::Attached(path.to_owned()))
            .unwrap_or(ActiveConnection::Managed)),
    }
}

fn connection_target_summary(
    active: &ActiveConnection,
    codex_home: &Result<PathBuf, String>,
    codex_state_dir: &Result<PathBuf, String>,
) -> String {
    let user = env::var("USER").unwrap_or_else(|_| "?".into());
    let host = env::var("HOSTNAME")
        .ok()
        .filter(|value| !value.is_empty())
        .or_else(|| {
            fs::read_to_string("/etc/hostname")
                .ok()
                .map(|value| value.trim().to_owned())
                .filter(|value| !value.is_empty())
        })
        .unwrap_or_else(|| "?".into());
    let home = codex_home
        .as_ref()
        .map(|path| format!("CODEX_HOME={}", path.display()))
        .unwrap_or_else(|_| "CODEX_HOME=?".into());
    let sqlite = codex_state_dir
        .as_ref()
        .map(|path| format!("SQLite={}", path.display()))
        .unwrap_or_else(|_| "SQLite=?".into());
    let connection = match active {
        ActiveConnection::Attached(_) => "app-server=attached",
        ActiveConnection::Managed => "app-server=managed",
    };
    format!("{user}@{host} · {home} · {sqlite} · {connection}")
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

fn default_codex_sqlite_dir(codex_home: &Path) -> Result<PathBuf, String> {
    let config_path = codex_home.join("config.toml");
    match fs::read_to_string(&config_path) {
        Ok(content) => {
            let config: toml::Value = toml::from_str(&content)
                .map_err(|error| format!("cannot parse {}: {error}", config_path.display()))?;
            if let Some(value) = config.get("sqlite_home") {
                let value = value.as_str().ok_or_else(|| {
                    format!("sqlite_home in {} is not a string", config_path.display())
                })?;
                if value.is_empty() {
                    return Err(format!("sqlite_home in {} is empty", config_path.display()));
                }
                return Ok(PathBuf::from(value));
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(format!("cannot read {}: {error}", config_path.display())),
    }
    match env::var_os("CODEX_SQLITE_HOME") {
        Some(value) if value.is_empty() => Err("CODEX_SQLITE_HOME is empty".into()),
        Some(value) => Ok(PathBuf::from(value)),
        None => Ok(codex_home.to_owned()),
    }
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

fn sqlite_bool(row: &rusqlite::Row<'_>, index: usize, field: &str) -> Result<bool, String> {
    match row
        .get_ref(index)
        .map_err(|error| format!("{field} cannot be read: {error}"))?
    {
        ValueRef::Integer(0) => Ok(false),
        ValueRef::Integer(1) => Ok(true),
        _ => Err(format!("{field} is not 0 or 1")),
    }
}

fn parse_catalog_source(raw: &str) -> Value {
    serde_json::from_str(raw).unwrap_or_else(|_| Value::String(raw.to_owned()))
}

fn catalog_parent_from_source(source: &Value) -> Option<String> {
    [
        "/subAgent/thread_spawn/parent_thread_id",
        "/subagent/thread_spawn/parent_thread_id",
    ]
    .into_iter()
    .find_map(|pointer| source.pointer(pointer).and_then(Value::as_str))
    .map(str::to_owned)
}

fn read_thread_catalog(database: &Path) -> Result<CatalogSnapshot, String> {
    let connection = Connection::open_with_flags(
        database,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|error| format!("cannot open {} read-only: {error}", database.display()))?;
    let mut statement = connection
        .prepare(
            "SELECT id, title, cwd, project_id, model_provider, model, archived, is_pinned, \
                    updated_at_ms, rollout_path, source \
             FROM threads",
        )
        .map_err(|error| format!("threads catalog schema is unavailable: {error}"))?;
    let mut rows = statement
        .query([])
        .map_err(|error| format!("cannot query threads catalog: {error}"))?;
    let mut threads = BTreeMap::new();
    while let Some(row) = rows
        .next()
        .map_err(|error| format!("cannot read threads catalog row: {error}"))?
    {
        let id: String = row
            .get(0)
            .map_err(|error| format!("thread id is not text: {error}"))?;
        if id.trim().is_empty() {
            return Err("thread id is empty".into());
        }
        let cwd: String = row
            .get(2)
            .map_err(|error| format!("cwd cannot be read for {id}: {error}"))?;
        if cwd.trim().is_empty() {
            return Err(format!("cwd is empty for {id}"));
        }
        let rollout_path: String = row
            .get(9)
            .map_err(|error| format!("rollout_path cannot be read for {id}: {error}"))?;
        if rollout_path.trim().is_empty() {
            return Err(format!("rollout_path is empty for {id}"));
        }
        let raw_source: String = row
            .get(10)
            .map_err(|error| format!("source cannot be read for {id}: {error}"))?;
        let source = parse_catalog_source(&raw_source);
        let thread = CatalogThread {
            id: id.clone(),
            title: row
                .get::<_, Option<String>>(1)
                .map_err(|error| format!("title cannot be read for {id}: {error}"))?
                .filter(|value| !value.trim().is_empty()),
            cwd,
            project: row
                .get(3)
                .map_err(|error| format!("project_id cannot be read for {id}: {error}"))?,
            provider: row
                .get(4)
                .map_err(|error| format!("model_provider cannot be read for {id}: {error}"))?,
            model: row
                .get(5)
                .map_err(|error| format!("model cannot be read for {id}: {error}"))?,
            archived: sqlite_bool(row, 6, &format!("archived for {id}"))?,
            pinned: sqlite_bool(row, 7, &format!("is_pinned for {id}"))?,
            last_activity_ms: row
                .get(8)
                .map_err(|error| format!("updated_at_ms cannot be read for {id}: {error}"))?,
            rollout_path: PathBuf::from(rollout_path),
            parent_id: catalog_parent_from_source(&source),
            source,
        };
        if threads.insert(id.clone(), thread).is_some() {
            return Err(format!("duplicate thread row for {id}"));
        }
    }
    drop(rows);
    drop(statement);

    let edge_has_status = {
        let mut columns = connection
            .prepare("PRAGMA table_info(thread_spawn_edges)")
            .map_err(|error| format!("thread_spawn_edges schema is unavailable: {error}"))?;
        let names = columns
            .query_map([], |row| row.get::<_, String>(1))
            .map_err(|error| format!("cannot inspect thread_spawn_edges columns: {error}"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| format!("cannot read thread_spawn_edges columns: {error}"))?;
        names.iter().any(|name| name == "status")
    };
    let edge_sql = if edge_has_status {
        "SELECT parent_thread_id, child_thread_id, status FROM thread_spawn_edges"
    } else {
        "SELECT parent_thread_id, child_thread_id, '' FROM thread_spawn_edges"
    };
    let mut edge_statement = connection
        .prepare(edge_sql)
        .map_err(|error| format!("thread_spawn_edges schema is unavailable: {error}"))?;
    let mut edge_rows = edge_statement
        .query([])
        .map_err(|error| format!("cannot query thread_spawn_edges: {error}"))?;
    let mut ignored_orphan_edges = 0usize;
    let mut unresolved_child_edges = 0usize;
    let mut stale_edges = Vec::new();
    while let Some(row) = edge_rows
        .next()
        .map_err(|error| format!("cannot read thread_spawn_edges row: {error}"))?
    {
        let parent: String = row
            .get(0)
            .map_err(|error| format!("spawn parent id is not text: {error}"))?;
        let child: String = row
            .get(1)
            .map_err(|error| format!("spawn child id is not text: {error}"))?;
        let status: String = row
            .get(2)
            .map_err(|error| format!("spawn status is not text: {error}"))?;
        let parent_exists = threads.contains_key(&parent);
        let Some(child_thread) = threads.get_mut(&child) else {
            ignored_orphan_edges = ignored_orphan_edges.saturating_add(1);
            stale_edges.push(StaleSpawnEdge {
                parent,
                child,
                status,
            });
            continue;
        };
        if !parent_exists {
            unresolved_child_edges = unresolved_child_edges.saturating_add(1);
            continue;
        }
        match child_thread.parent_id.as_deref() {
            Some(source_parent) if source_parent != parent => {
                return Err(format!(
                    "conflicting parent metadata for {child}: source={source_parent}, edge={parent}"
                ));
            }
            Some(_) => {}
            None => child_thread.parent_id = Some(parent),
        }
    }
    Ok(CatalogSnapshot {
        threads,
        ignored_orphan_edges,
        unresolved_child_edges,
        stale_edges,
    })
}

fn validate_rollout_path(codex_home: &Path, thread: &CatalogThread) -> Result<u64, String> {
    let root = codex_home.join(if thread.archived {
        "archived_sessions"
    } else {
        "sessions"
    });
    if !thread.rollout_path.is_absolute() || !thread.rollout_path.starts_with(&root) {
        return Err(format!(
            "thread {} rollout path is outside the expected {} tree",
            thread.id,
            root.display()
        ));
    }
    let metadata = fs::symlink_metadata(&thread.rollout_path).map_err(|error| {
        format!(
            "thread {} rollout file is unavailable at {}: {error}",
            thread.id,
            thread.rollout_path.display()
        )
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(format!(
            "thread {} rollout path is not a regular file: {}",
            thread.id,
            thread.rollout_path.display()
        ));
    }
    Ok(metadata.len())
}

fn collect_jsonl_paths(root: &Path, recursive: bool) -> Result<BTreeSet<PathBuf>, String> {
    if !root.exists() {
        return Ok(BTreeSet::new());
    }
    let mut files = BTreeSet::new();
    let mut pending = vec![root.to_owned()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(&directory)
            .map_err(|error| format!("cannot scan {}: {error}", directory.display()))?
        {
            let entry = entry
                .map_err(|error| format!("cannot inspect {}: {error}", directory.display()))?;
            let file_type = entry
                .file_type()
                .map_err(|error| format!("cannot inspect {}: {error}", entry.path().display()))?;
            if file_type.is_symlink() {
                continue;
            }
            if file_type.is_dir() && recursive {
                pending.push(entry.path());
            } else if file_type.is_file()
                && entry.path().extension().and_then(|value| value.to_str()) == Some("jsonl")
            {
                files.insert(entry.path());
            }
        }
    }
    Ok(files)
}

fn audit_rollout_roots(
    codex_home: &Path,
    catalog: &CatalogSnapshot,
) -> Result<(usize, usize, Vec<PathBuf>), String> {
    let sessions = collect_jsonl_paths(&codex_home.join("sessions"), true)?;
    let archived = collect_jsonl_paths(&codex_home.join("archived_sessions"), false)?;
    let expected = catalog
        .threads
        .values()
        .map(|thread| thread.rollout_path.clone())
        .collect::<BTreeSet<_>>();
    let orphaned = sessions
        .union(&archived)
        .filter(|path| !expected.contains(*path))
        .cloned()
        .collect::<Vec<_>>();
    Ok((sessions.len(), archived.len(), orphaned))
}

fn discover_maintenance(
    codex_home: &Path,
    _database: &Path,
    catalog: &CatalogSnapshot,
) -> Result<MaintenanceDiscovery, String> {
    let (active_rollouts, archived_rollouts, orphaned) = audit_rollout_roots(codex_home, catalog)?;
    let mut candidates = Vec::new();
    for path in orphaned {
        let metadata = fs::symlink_metadata(&path)
            .map_err(|error| format!("cannot inspect {}: {error}", path.display()))?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            continue;
        }
        let modified = metadata
            .modified()
            .ok()
            .and_then(|value| value.duration_since(UNIX_EPOCH).ok())
            .map(|value| value.as_nanos())
            .unwrap_or_default();
        let matching_thread = catalog
            .threads
            .values()
            .find(|thread| path.to_string_lossy().contains(&thread.id));
        let project = matching_thread
            .map(|thread| thread.project.clone().unwrap_or_else(|| thread.cwd.clone()));
        candidates.push(MaintenanceCandidate {
            key: format!("rollout:{}", path.display()),
            kind: MaintenanceKind::UnreferencedRollout,
            label: path
                .file_name()
                .and_then(|value| value.to_str())
                .unwrap_or("rollout.jsonl")
                .to_owned(),
            detail: path.display().to_string(),
            project,
            bytes: metadata.len(),
            fingerprint: format!("{}:{modified}", metadata.len()),
        });
    }
    for edge in &catalog.stale_edges {
        candidates.push(MaintenanceCandidate {
            key: format!("edge:{}:{}", edge.parent, edge.child),
            kind: MaintenanceKind::StaleSpawnEdge,
            label: format!("{} → {}", edge.parent, edge.child),
            detail: edge.status.clone(),
            project: None,
            bytes: 0,
            fingerprint: format!("{}:{}:{}", edge.parent, edge.child, edge.status),
        });
    }
    for thread in catalog.threads.values() {
        let expected_root = codex_home.join(if thread.archived {
            "archived_sessions"
        } else {
            "sessions"
        });
        if !thread.rollout_path.is_absolute() || !thread.rollout_path.starts_with(&expected_root) {
            continue;
        }
        match fs::symlink_metadata(&thread.rollout_path) {
            Ok(_) => continue,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(format!(
                    "cannot inspect {}: {error}",
                    thread.rollout_path.display()
                ));
            }
        }
        candidates.push(MaintenanceCandidate {
            key: format!("thread:{}", thread.id),
            kind: MaintenanceKind::MissingRolloutThread,
            label: thread.title.clone().unwrap_or_else(|| thread.id.clone()),
            detail: thread.rollout_path.display().to_string(),
            project: Some(thread.project.clone().unwrap_or_else(|| thread.cwd.clone())),
            bytes: 0,
            fingerprint: format!(
                "{}:{}:{}",
                thread.id,
                thread.last_activity_ms,
                thread.rollout_path.display()
            ),
        });
    }
    candidates.sort_by(|left, right| {
        left.kind
            .cmp(&right.kind)
            .then_with(|| left.key.cmp(&right.key))
    });
    Ok(MaintenanceDiscovery {
        active_rollouts,
        archived_rollouts,
        candidates,
    })
}

fn latest_versioned_database(directory: &Path, prefix: &str) -> Option<PathBuf> {
    fs::read_dir(directory)
        .ok()?
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let name = entry.file_name();
            let name = name.to_str()?;
            let version = name
                .strip_prefix(prefix)?
                .strip_suffix(".sqlite")?
                .parse::<u32>()
                .ok()?;
            entry
                .file_type()
                .ok()?
                .is_file()
                .then(|| (version, entry.path()))
        })
        .max_by_key(|(version, _)| *version)
        .map(|(_, path)| path)
}

fn table_has_thread_id(
    connection: &Connection,
    table: &str,
    column: &str,
    thread_id: &str,
) -> Result<bool, String> {
    let exists: bool = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1)",
            [table],
            |row| row.get(0),
        )
        .map_err(|error| format!("cannot inspect {table}: {error}"))?;
    if !exists {
        return Ok(false);
    }
    connection
        .query_row(
            &format!("SELECT EXISTS(SELECT 1 FROM {table} WHERE {column} = ?1 LIMIT 1)"),
            [thread_id],
            |row| row.get(0),
        )
        .map_err(|error| format!("cannot verify {table}.{column}: {error}"))
}

fn session_index_contains(codex_home: &Path, thread_id: &str) -> Result<bool, String> {
    let path = codex_home.join("session_index.jsonl");
    let file = match fs::File::open(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(format!("cannot open {}: {error}", path.display())),
    };
    for line in io::BufReader::new(file).lines() {
        let line = line.map_err(|error| format!("cannot read {}: {error}", path.display()))?;
        let value: Value = serde_json::from_str(&line)
            .map_err(|error| format!("cannot parse {}: {error}", path.display()))?;
        if value.get("id").and_then(Value::as_str) == Some(thread_id) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn deletion_residue(
    codex_home: &Path,
    sqlite_home: &Path,
    state_database: &Path,
    thread_id: &str,
    rollout_path: &Path,
) -> Result<Vec<String>, String> {
    let mut residue = Vec::new();
    if fs::symlink_metadata(rollout_path).is_ok() {
        residue.push("rollout file".into());
    }
    if session_index_contains(codex_home, thread_id)? {
        residue.push("session_index.jsonl".into());
    }
    let state = Connection::open_with_flags(
        state_database,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|error| {
        format!(
            "cannot reopen {} read-only: {error}",
            state_database.display()
        )
    })?;
    for (table, column) in [
        ("threads", "id"),
        ("thread_attachments", "thread_id"),
        ("thread_dynamic_tools", "thread_id"),
        ("thread_spawn_edges", "parent_thread_id"),
        ("thread_spawn_edges", "child_thread_id"),
    ] {
        if table_has_thread_id(&state, table, column, thread_id)? {
            residue.push(format!("{table}.{column}"));
        }
    }
    if let Some(history_path) = latest_versioned_database(sqlite_home, "thread_history_") {
        let history = Connection::open_with_flags(
            &history_path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(|error| format!("cannot open {} read-only: {error}", history_path.display()))?;
        for table in ["thread_items", "thread_turns", "thread_realtime_items"] {
            if table_has_thread_id(&history, table, "thread_id", thread_id)? {
                residue.push(table.into());
            }
        }
    }
    Ok(residue)
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

impl AppServerClient {
    fn maintenance_context(&self) -> Result<(PathBuf, PathBuf, MaintenanceDiscovery), VaultError> {
        let codex_home = self
            .codex_home
            .as_ref()
            .map_err(|error| VaultError::Blocked(error.clone()))?
            .clone();
        let state_dir = self
            .codex_state_dir
            .as_ref()
            .map_err(|error| VaultError::Blocked(error.clone()))?;
        let database = find_state_database(state_dir).map_err(VaultError::Blocked)?;
        let catalog = read_thread_catalog(&database).map_err(VaultError::Blocked)?;
        let discovery =
            discover_maintenance(&codex_home, &database, &catalog).map_err(VaultError::Blocked)?;
        Ok((codex_home, database, discovery))
    }

    fn maintenance_backup_root(
        &mut self,
        codex_home: &Path,
        batch_id: &str,
    ) -> Result<PathBuf, VaultError> {
        if batch_id.is_empty()
            || !batch_id
                .chars()
                .all(|value| value.is_ascii_alphanumeric() || matches!(value, '-' | '_'))
        {
            return Err(VaultError::Blocked("invalid maintenance batch id".into()));
        }
        if let Some(path) = self.maintenance_backup_roots.get(batch_id) {
            return Ok(path.clone());
        }
        let root = codex_home.join("codex-vault-backups").join(batch_id);
        fs::create_dir_all(&root).map_err(|error| {
            VaultError::Storage(format!("cannot create backup {}: {error}", root.display()))
        })?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).map_err(|error| {
                VaultError::Storage(format!(
                    "cannot restrict backup permissions for {}: {error}",
                    root.display()
                ))
            })?;
        }
        self.maintenance_backup_roots
            .insert(batch_id.to_owned(), root.clone());
        Ok(root)
    }

    fn cleanup_candidate(
        &mut self,
        batch_id: &str,
        expected: &MaintenanceCandidate,
    ) -> Result<MaintenanceAck, VaultError> {
        let (codex_home, database, discovery) = self.maintenance_context()?;
        let actual = discovery
            .candidates
            .into_iter()
            .find(|candidate| candidate.key == expected.key)
            .ok_or(VaultError::StalePreview)?;
        if actual.kind != expected.kind || actual.fingerprint != expected.fingerprint {
            return Err(VaultError::StalePreview);
        }
        let backup_root = self.maintenance_backup_root(&codex_home, batch_id)?;
        match actual.kind {
            MaintenanceKind::UnreferencedRollout => {
                cleanup_unreferenced_rollout(&codex_home, &backup_root, &actual)?;
            }
            MaintenanceKind::StaleSpawnEdge => {
                backup_sqlite_database(&database, &backup_root)?;
                cleanup_stale_spawn_edge(&database, &actual)?;
            }
            MaintenanceKind::MissingRolloutThread => {
                backup_sqlite_database(&database, &backup_root)?;
                cleanup_missing_rollout_thread(&codex_home, &database, &backup_root, &actual)?;
            }
        }
        Ok(MaintenanceAck {
            backup_path: backup_root.display().to_string(),
        })
    }
}

fn backup_sqlite_database(database: &Path, backup_root: &Path) -> Result<PathBuf, VaultError> {
    let directory = backup_root.join("sqlite");
    fs::create_dir_all(&directory).map_err(|error| {
        VaultError::Storage(format!("cannot create {}: {error}", directory.display()))
    })?;
    let target = directory.join(
        database
            .file_name()
            .unwrap_or_else(|| std::ffi::OsStr::new("state.sqlite")),
    );
    if target.exists() {
        return Ok(target);
    }
    let source = Connection::open_with_flags(
        database,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|error| VaultError::Storage(format!("cannot open state database: {error}")))?;
    source
        .backup(DatabaseName::Main, &target, None)
        .map_err(|error| VaultError::Storage(format!("cannot back up state database: {error}")))?;
    let metadata = fs::metadata(&target).map_err(|error| {
        VaultError::Storage(format!(
            "cannot verify backup {}: {error}",
            target.display()
        ))
    })?;
    if metadata.len() == 0 {
        return Err(VaultError::Storage(format!(
            "state database backup is empty: {}",
            target.display()
        )));
    }
    Ok(target)
}

fn backup_regular_file(source: &Path, target: &Path) -> Result<(), VaultError> {
    if target.exists() {
        let source_len = fs::metadata(source)
            .map_err(|error| VaultError::Storage(error.to_string()))?
            .len();
        let target_len = fs::metadata(target)
            .map_err(|error| VaultError::Storage(error.to_string()))?
            .len();
        if source_len == target_len {
            return Ok(());
        }
        return Err(VaultError::Storage(format!(
            "backup target already exists with a different size: {}",
            target.display()
        )));
    }
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            VaultError::Storage(format!("cannot create {}: {error}", parent.display()))
        })?;
    }
    let copied = fs::copy(source, target).map_err(|error| {
        VaultError::Storage(format!(
            "cannot back up {} to {}: {error}",
            source.display(),
            target.display()
        ))
    })?;
    let expected = fs::metadata(source)
        .map_err(|error| VaultError::Storage(error.to_string()))?
        .len();
    if copied != expected {
        return Err(VaultError::Storage(format!(
            "backup size mismatch for {}",
            source.display()
        )));
    }
    fs::File::open(target)
        .and_then(|file| file.sync_all())
        .map_err(|error| VaultError::Storage(format!("cannot sync backup: {error}")))?;
    Ok(())
}

fn cleanup_unreferenced_rollout(
    codex_home: &Path,
    backup_root: &Path,
    candidate: &MaintenanceCandidate,
) -> Result<(), VaultError> {
    let source = PathBuf::from(&candidate.detail);
    let allowed = [
        codex_home.join("sessions"),
        codex_home.join("archived_sessions"),
    ];
    if !source.is_absolute() || !allowed.iter().any(|root| source.starts_with(root)) {
        return Err(VaultError::Blocked(format!(
            "rollout cleanup path is outside CODEX_HOME: {}",
            source.display()
        )));
    }
    let metadata = fs::symlink_metadata(&source)
        .map_err(|error| VaultError::Blocked(format!("rollout changed: {error}")))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(VaultError::Blocked("rollout is not a regular file".into()));
    }
    let relative = source
        .strip_prefix(codex_home)
        .map_err(|_| VaultError::Blocked("rollout path escaped CODEX_HOME".into()))?;
    let target = backup_root.join("rollouts").join(relative);
    backup_regular_file(&source, &target)?;
    fs::remove_file(&source).map_err(|error| {
        VaultError::Storage(format!("cannot remove {}: {error}", source.display()))
    })
}

fn cleanup_stale_spawn_edge(
    database: &Path,
    candidate: &MaintenanceCandidate,
) -> Result<(), VaultError> {
    let mut parts = candidate.fingerprint.splitn(3, ':');
    let parent = parts.next().unwrap_or_default();
    let child = parts.next().unwrap_or_default();
    let status = parts.next().unwrap_or_default();
    if parent.is_empty() || child.is_empty() {
        return Err(VaultError::StalePreview);
    }
    let mut connection = Connection::open_with_flags(
        database,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|error| VaultError::Storage(format!("cannot open state database: {error}")))?;
    let transaction = connection
        .transaction()
        .map_err(|error| VaultError::Storage(error.to_string()))?;
    let edge_has_status = {
        let mut statement = transaction
            .prepare("PRAGMA table_info(thread_spawn_edges)")
            .map_err(|error| VaultError::Storage(error.to_string()))?;
        let names = statement
            .query_map([], |row| row.get::<_, String>(1))
            .map_err(|error| VaultError::Storage(error.to_string()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| VaultError::Storage(error.to_string()))?;
        names.iter().any(|name| name == "status")
    };
    let changed = if edge_has_status {
        transaction.execute(
            "DELETE FROM thread_spawn_edges WHERE parent_thread_id=?1 AND child_thread_id=?2 AND status=?3",
            params![parent, child, status],
        )
    } else {
        transaction.execute(
            "DELETE FROM thread_spawn_edges WHERE parent_thread_id=?1 AND child_thread_id=?2",
            params![parent, child],
        )
    }
    .map_err(|error| VaultError::Storage(error.to_string()))?;
    if changed != 1 {
        return Err(VaultError::StalePreview);
    }
    transaction
        .commit()
        .map_err(|error| VaultError::Storage(error.to_string()))
}

fn cleanup_missing_rollout_thread(
    codex_home: &Path,
    database: &Path,
    backup_root: &Path,
    candidate: &MaintenanceCandidate,
) -> Result<(), VaultError> {
    let thread_id = candidate
        .key
        .strip_prefix("thread:")
        .filter(|value| !value.is_empty())
        .ok_or(VaultError::StalePreview)?;
    let session_index = codex_home.join("session_index.jsonl");
    let index_backup = backup_root.join("index/session_index.jsonl");
    let index_changed = if session_index.is_file() {
        backup_regular_file(&session_index, &index_backup)?;
        remove_session_index_entry(&session_index, thread_id, backup_root)?
    } else {
        false
    };

    let result = (|| {
        let mut connection = Connection::open_with_flags(
            database,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(|error| VaultError::Storage(format!("cannot open state database: {error}")))?;
        let transaction = connection
            .transaction()
            .map_err(|error| VaultError::Storage(error.to_string()))?;
        transaction
            .execute(
                "DELETE FROM thread_attachments WHERE thread_id=?1",
                [thread_id],
            )
            .map_err(|error| VaultError::Storage(error.to_string()))?;
        transaction
            .execute(
                "DELETE FROM thread_dynamic_tools WHERE thread_id=?1",
                [thread_id],
            )
            .map_err(|error| VaultError::Storage(error.to_string()))?;
        transaction
            .execute(
                "DELETE FROM thread_spawn_edges WHERE parent_thread_id=?1 OR child_thread_id=?1",
                [thread_id],
            )
            .map_err(|error| VaultError::Storage(error.to_string()))?;
        let changed = transaction
            .execute("DELETE FROM threads WHERE id=?1", [thread_id])
            .map_err(|error| VaultError::Storage(error.to_string()))?;
        if changed != 1 {
            return Err(VaultError::StalePreview);
        }
        transaction
            .commit()
            .map_err(|error| VaultError::Storage(error.to_string()))
    })();
    if result.is_err() && index_changed {
        let _ = fs::copy(&index_backup, &session_index);
    }
    result
}

fn remove_session_index_entry(
    path: &Path,
    thread_id: &str,
    backup_root: &Path,
) -> Result<bool, VaultError> {
    let input = fs::File::open(path)
        .map_err(|error| VaultError::Storage(format!("cannot read session index: {error}")))?;
    let metadata = input
        .metadata()
        .map_err(|error| VaultError::Storage(error.to_string()))?;
    let temporary = backup_root.join("index/session_index.rewrite.tmp");
    if temporary.exists() {
        fs::remove_file(&temporary).map_err(|error| VaultError::Storage(error.to_string()))?;
    }
    let mut output = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .map_err(|error| VaultError::Storage(format!("cannot create rewritten index: {error}")))?;
    let mut changed = false;
    for line in io::BufReader::new(input).lines() {
        let line = line.map_err(|error| VaultError::Storage(error.to_string()))?;
        let remove = serde_json::from_str::<Value>(&line).is_ok_and(|value| {
            value.get("id").and_then(Value::as_str) == Some(thread_id)
                || value.get("thread_id").and_then(Value::as_str) == Some(thread_id)
        });
        if remove {
            changed = true;
        } else {
            writeln!(output, "{line}").map_err(|error| VaultError::Storage(error.to_string()))?;
        }
    }
    output
        .flush()
        .and_then(|_| output.sync_all())
        .map_err(|error| VaultError::Storage(format!("cannot sync rewritten index: {error}")))?;
    drop(output);
    if changed {
        fs::set_permissions(&temporary, metadata.permissions())
            .map_err(|error| VaultError::Storage(error.to_string()))?;
        fs::rename(&temporary, path).map_err(|error| {
            VaultError::Storage(format!("cannot replace session index: {error}"))
        })?;
    } else {
        fs::remove_file(&temporary).map_err(|error| VaultError::Storage(error.to_string()))?;
    }
    Ok(changed)
}

impl SessionGateway for AppServerClient {
    fn target_summary(&self) -> Option<&str> {
        Some(&self.target_summary)
    }

    fn scan(&mut self) -> PortFuture<'_, ScanSnapshot> {
        Box::pin(async move {
            self.restart_if_needed().await?;
            self.scan_inner().await
        })
    }

    fn mutate<'a>(&'a mut self, action: Action, thread_id: &'a str) -> PortFuture<'a, MutationAck> {
        Box::pin(async move { self.run_cli_mutation(action, thread_id).await })
    }

    fn maintenance_candidates(&self) -> &[MaintenanceCandidate] {
        &self.maintenance_candidates
    }

    fn cleanup<'a>(
        &'a mut self,
        batch_id: &'a str,
        candidate: &'a MaintenanceCandidate,
    ) -> PortFuture<'a, MaintenanceAck> {
        Box::pin(async move { self.cleanup_candidate(batch_id, candidate) })
    }
}

impl Drop for AppServerClient {
    fn drop(&mut self) {
        self.transport.terminate();
    }
}

enum RpcOutcome {
    Success(Value),
    Error { code: Option<i64>, message: String },
}

fn cli_error_detail(stderr: &[u8], stdout: &[u8]) -> String {
    let raw = if stderr.is_empty() { stdout } else { stderr };
    let normalized = String::from_utf8_lossy(raw)
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    if normalized.is_empty() {
        return String::new();
    }
    let mut characters = normalized.chars();
    let mut limited = characters
        .by_ref()
        .take(CLI_ERROR_LIMIT)
        .collect::<String>();
    if characters.next().is_some() {
        limited.push('…');
    }
    format!(": {limited}")
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListResponse {
    data: Vec<ThreadDto>,
    #[serde(default)]
    next_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ThreadReadResponse {
    thread: ThreadDto,
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
        None if value.get("subAgent").is_some() || value.get("subagent").is_some() => {
            SessionSource::Descendant
        }
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
            provider: None,
            model: None,
            cwd,
            rollout_bytes: None,
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

    #[test]
    fn auto_connection_attaches_only_when_the_control_socket_exists() {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("app-server.sock");
        assert_eq!(
            select_connection(ConnectionMode::Auto, Some(&socket)).unwrap(),
            ActiveConnection::Managed
        );
        fs::write(&socket, b"socket marker").unwrap();
        assert_eq!(
            select_connection(ConnectionMode::Auto, Some(&socket)).unwrap(),
            ActiveConnection::Attached(socket)
        );
    }

    #[test]
    fn attached_connection_fails_closed_without_a_control_socket() {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("missing.sock");
        assert!(select_connection(ConnectionMode::Attached, Some(&socket)).is_err());
        assert_eq!(
            select_connection(ConnectionMode::Managed, Some(&socket)).unwrap(),
            ActiveConnection::Managed
        );
    }

    #[cfg(unix)]
    async fn spawn_mock_client(
        after_probes: &str,
        missing_probe: Option<u64>,
        missing_cli: Option<&str>,
    ) -> (tempfile::TempDir, AppServerClient) {
        fn probe_response(id: u64, missing_probe: Option<u64>) -> String {
            let code = if missing_probe == Some(id) {
                -32601
            } else {
                -32600
            };
            format!(
                "if printf '%s' \"$line\" | grep -q '\"threadId\":\"\"'; then \
                 printf '%s\\n' '{{\"id\":{id},\"error\":{{\"code\":{code},\"message\":\"probe\"}}}}'; \
                 else printf '%s\\n' '{{\"id\":{id},\"error\":{{\"code\":-32603,\"message\":\"unsafe probe\"}}}}'; fi"
            )
        }

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("mock-codex");
        let missing_cli = missing_cli.unwrap_or("");
        let cli_log = directory.path().join("cli-arguments.log");
        let cli_fail = directory.path().join("cli-fail");
        let cli_sleep = directory.path().join("cli-sleep");
        let script = format!(
            "#!/bin/sh\n\
             case \"${{1:-}}\" in\n\
               archive|unarchive|delete)\n\
                 [ \"$1\" = \"{missing_cli}\" ] && exit 64\n\
                 if [ \"${{2:-}}\" != \"--help\" ]; then\n\
                   printf '%s\\n' \"$@\" >> '{cli_log}'\n\
                   if [ -f '{cli_fail}' ]; then\n\
                     printf 'synthetic CLI failure\\n' >&2\n\
                     exit 7\n\
                   fi\n\
                   [ -f '{cli_sleep}' ] && sleep 5\n\
                 fi\n\
                 exit 0\n\
                 ;;\n\
             esac\n\
             n=0\n\
             while IFS= read -r line; do\n\
               n=$((n + 1))\n\
               case \"$n\" in\n\
                 1) printf '%s\\n' '{{\"id\":1,\"result\":{{}}}}' ;;\n\
                 2) : ;;\n\
                 3) {} ;;\n\
                 4) {} ;;\n\
               esac\n\
             done\n",
            probe_response(2, missing_probe),
            after_probes,
            cli_log = cli_log.display(),
            cli_fail = cli_fail.display(),
            cli_sleep = cli_sleep.display(),
        );
        fs::write(&path, script).unwrap();
        let mut permissions = fs::metadata(&path).unwrap().permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(&path, permissions).unwrap();
        let mut client = AppServerClient::spawn(&path).await.unwrap();
        client.codex_home = Ok(directory.path().to_owned());
        client.codex_state_dir = Ok(directory.path().to_owned());
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

    fn create_catalog_database(path: &Path, rollout_path: &Path) {
        let connection = Connection::open(path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE threads (
                    id TEXT PRIMARY KEY,
                    title TEXT,
                    cwd TEXT NOT NULL,
                    project_id TEXT,
                    model_provider TEXT,
                    model TEXT,
                    archived INTEGER NOT NULL,
                    is_pinned INTEGER NOT NULL,
                    updated_at_ms INTEGER NOT NULL,
                    rollout_path TEXT NOT NULL,
                    source TEXT NOT NULL
                );
                CREATE TABLE thread_attachments (thread_id TEXT);
                CREATE TABLE thread_dynamic_tools (thread_id TEXT);
                CREATE TABLE thread_spawn_edges (
                    parent_thread_id TEXT NOT NULL,
                    child_thread_id TEXT NOT NULL
                );",
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO threads VALUES (?1, 'Root', '/work/project', NULL, 'custom', \
                 'gpt-5.5', 0, 0, 123000, ?2, 'cli')",
                ("root", rollout_path.to_string_lossy().as_ref()),
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO threads VALUES (?1, 'Child', '/work/project', NULL, 'free', \
                 'gpt-5.6-sol', 0, 0, 124000, ?2, ?3)",
                (
                    "child",
                    rollout_path.to_string_lossy().as_ref(),
                    r#"{"subagent":"review"}"#,
                ),
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO thread_spawn_edges VALUES ('root', 'child')",
                [],
            )
            .unwrap();
    }

    #[test]
    fn sqlite_home_config_precedes_environment_fallback() {
        let home = tempfile::tempdir().unwrap();
        fs::write(
            home.path().join("config.toml"),
            "sqlite_home = '/catalog'\n",
        )
        .unwrap();
        assert_eq!(
            default_codex_sqlite_dir(home.path()).unwrap(),
            PathBuf::from("/catalog")
        );
    }

    #[test]
    fn state_catalog_supplies_provider_model_relationship_and_recursive_rollout_path() {
        let home = tempfile::tempdir().unwrap();
        let rollout = home.path().join("sessions/2026/09/22/rollout-root.jsonl");
        fs::create_dir_all(rollout.parent().unwrap()).unwrap();
        fs::write(&rollout, b"{}\n").unwrap();
        let database = home.path().join("state_5.sqlite");
        create_catalog_database(&database, &rollout);

        let catalog = read_thread_catalog(&database).unwrap();
        assert_eq!(catalog.threads.len(), 2);
        assert_eq!(catalog.threads["root"].provider.as_deref(), Some("custom"));
        assert_eq!(catalog.threads["root"].model.as_deref(), Some("gpt-5.5"));
        assert_eq!(catalog.threads["child"].parent_id.as_deref(), Some("root"));
        assert_eq!(
            validate_rollout_path(home.path(), &catalog.threads["root"]),
            Ok(3)
        );
        let orphan = home.path().join("sessions/2025/01/01/orphan.jsonl");
        fs::create_dir_all(orphan.parent().unwrap()).unwrap();
        fs::write(&orphan, b"{}\n").unwrap();
        let (active_count, archived_count, orphaned) =
            audit_rollout_roots(home.path(), &catalog).unwrap();
        assert_eq!((active_count, archived_count), (2, 0));
        assert_eq!(orphaned, vec![orphan]);
    }

    #[test]
    fn maintenance_discovery_classifies_only_proven_stale_data() {
        let home = tempfile::tempdir().unwrap();
        let rollout = home.path().join("sessions/2026/09/22/rollout-root.jsonl");
        let orphan = home
            .path()
            .join("sessions/2026/09/21/rollout-root-copy.jsonl");
        fs::create_dir_all(rollout.parent().unwrap()).unwrap();
        fs::create_dir_all(orphan.parent().unwrap()).unwrap();
        fs::write(&rollout, b"current\n").unwrap();
        fs::write(&orphan, b"orphan\n").unwrap();
        let database = home.path().join("state_5.sqlite");
        create_catalog_database(&database, &rollout);
        let connection = Connection::open(&database).unwrap();
        connection
            .execute(
                "INSERT INTO thread_spawn_edges VALUES ('gone-parent', 'gone-child')",
                [],
            )
            .unwrap();
        drop(connection);

        let catalog = read_thread_catalog(&database).unwrap();
        let discovery = discover_maintenance(home.path(), &database, &catalog).unwrap();
        assert_eq!(discovery.active_rollouts, 2);
        assert_eq!(discovery.archived_rollouts, 0);
        assert_eq!(
            discovery
                .candidates
                .iter()
                .filter(|value| value.kind == MaintenanceKind::UnreferencedRollout)
                .count(),
            1
        );
        assert_eq!(
            discovery
                .candidates
                .iter()
                .filter(|value| value.kind == MaintenanceKind::StaleSpawnEdge)
                .count(),
            1
        );
        assert!(
            !discovery
                .candidates
                .iter()
                .any(|value| value.kind == MaintenanceKind::MissingRolloutThread)
        );
    }

    #[test]
    fn maintenance_cleanup_backs_up_files_database_and_index() {
        let home = tempfile::tempdir().unwrap();
        let rollout = home.path().join("sessions/2026/09/22/rollout-root.jsonl");
        let orphan = home.path().join("sessions/2026/09/21/orphan.jsonl");
        fs::create_dir_all(rollout.parent().unwrap()).unwrap();
        fs::create_dir_all(orphan.parent().unwrap()).unwrap();
        fs::write(&rollout, b"current\n").unwrap();
        fs::write(&orphan, b"unused\n").unwrap();
        fs::write(
            home.path().join("session_index.jsonl"),
            "{\"id\":\"root\"}\n{\"id\":\"keep\"}\n",
        )
        .unwrap();
        let database = home.path().join("state_5.sqlite");
        create_catalog_database(&database, &rollout);
        let backup = home.path().join("codex-vault-backups/test-batch");

        let catalog = read_thread_catalog(&database).unwrap();
        let discovery = discover_maintenance(home.path(), &database, &catalog).unwrap();
        let orphan_candidate = discovery
            .candidates
            .iter()
            .find(|value| value.kind == MaintenanceKind::UnreferencedRollout)
            .unwrap();
        cleanup_unreferenced_rollout(home.path(), &backup, orphan_candidate).unwrap();
        assert!(!orphan.exists());
        assert!(
            backup
                .join("rollouts/sessions/2026/09/21/orphan.jsonl")
                .is_file()
        );

        fs::remove_file(&rollout).unwrap();
        let catalog = read_thread_catalog(&database).unwrap();
        let discovery = discover_maintenance(home.path(), &database, &catalog).unwrap();
        let missing = discovery
            .candidates
            .iter()
            .find(|value| {
                value.kind == MaintenanceKind::MissingRolloutThread && value.key == "thread:root"
            })
            .unwrap();
        backup_sqlite_database(&database, &backup).unwrap();
        cleanup_missing_rollout_thread(home.path(), &database, &backup, missing).unwrap();

        let connection = Connection::open(&database).unwrap();
        let root_exists: bool = connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM threads WHERE id='root')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!root_exists);
        assert!(backup.join("sqlite/state_5.sqlite").is_file());
        assert!(backup.join("index/session_index.jsonl").is_file());
        let index = fs::read_to_string(home.path().join("session_index.jsonl")).unwrap();
        assert!(!index.contains("\"root\""));
        assert!(index.contains("\"keep\""));
    }

    #[test]
    fn delete_verification_checks_catalog_file_indexes_relations_and_history() {
        let home = tempfile::tempdir().unwrap();
        let rollout = home.path().join("sessions/2026/09/22/rollout-root.jsonl");
        fs::create_dir_all(rollout.parent().unwrap()).unwrap();
        fs::write(&rollout, b"{}\n").unwrap();
        fs::write(
            home.path().join("session_index.jsonl"),
            "{\"id\":\"root\",\"thread_name\":\"Root\",\"updated_at\":\"now\"}\n",
        )
        .unwrap();
        let state = home.path().join("state_5.sqlite");
        create_catalog_database(&state, &rollout);
        let history_path = home.path().join("thread_history_1.sqlite");
        let history = Connection::open(&history_path).unwrap();
        history
            .execute_batch(
                "CREATE TABLE thread_items (thread_id TEXT);
                 CREATE TABLE thread_turns (thread_id TEXT);
                 CREATE TABLE thread_realtime_items (thread_id TEXT);
                 INSERT INTO thread_items VALUES ('root');
                 INSERT INTO thread_turns VALUES ('root');",
            )
            .unwrap();
        drop(history);

        let residue = deletion_residue(home.path(), home.path(), &state, "root", &rollout).unwrap();
        assert!(residue.iter().any(|value| value == "rollout file"));
        assert!(residue.iter().any(|value| value == "threads.id"));
        assert!(residue.iter().any(|value| value == "session_index.jsonl"));
        assert!(residue.iter().any(|value| value == "thread_items"));

        let connection = Connection::open(&state).unwrap();
        connection.execute("DELETE FROM threads", []).unwrap();
        connection
            .execute("DELETE FROM thread_spawn_edges", [])
            .unwrap();
        drop(connection);
        let history = Connection::open(&history_path).unwrap();
        history.execute("DELETE FROM thread_items", []).unwrap();
        history.execute("DELETE FROM thread_turns", []).unwrap();
        drop(history);
        fs::remove_file(&rollout).unwrap();
        fs::write(home.path().join("session_index.jsonl"), b"").unwrap();

        assert!(
            deletion_residue(home.path(), home.path(), &state, "root", &rollout)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn recognizes_interactive_and_exec_sources() {
        assert_eq!(parse_source(&json!("cli")), SessionSource::Cli);
        assert_eq!(parse_source(&json!("vscode")), SessionSource::Vscode);
        assert_eq!(parse_source(&json!("exec")), SessionSource::Exec);
        for source in ["review", "compact", "memory_consolidation"] {
            assert_eq!(
                parse_source(&json!({"subAgent": source})),
                SessionSource::Descendant
            );
        }
        assert_eq!(
            parse_source(&json!({"subAgent": {"other": "future"}})),
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
            code: Some(-32600),
            message: "invalid thread id".into(),
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
    async fn required_read_method_and_cli_commands_are_probed_independently() {
        let (_directory, client) = spawn_mock_client(":", Some(2), None).await;
        assert!(!client.capabilities_complete);
        assert!(
            client
                .capability_diagnostics
                .iter()
                .any(|value| value.contains("thread/read"))
        );

        for command in ["archive", "unarchive", "delete"] {
            let (_directory, client) = spawn_mock_client(":", None, Some(command)).await;
            assert!(!client.capabilities_complete);
            assert!(
                client
                    .capability_diagnostics
                    .iter()
                    .any(|value| value.contains(&format!("codex {command}")))
            );
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn required_method_probes_use_protocol_invalid_empty_thread_ids() {
        let (_directory, client) = spawn_mock_client(":", None, None).await;
        assert!(client.capabilities_complete);
        assert!(client.capability_diagnostics.is_empty());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn missing_subagent_parents_are_hydrated_and_scoped_to_interactive_trees() {
        let active = serde_json::to_string(&serde_json::from_str::<Value>(r#"{"id":3,"result":{"data":[
            {"id":"root-a","cwd":"/tmp","recencyAt":5,"isPinned":false,"source":"cli","status":{"type":"idle"}},
            {"id":"child-in","cwd":"/tmp","recencyAt":4,"isPinned":false,"source":{"subAgent":"review"},"status":{"type":"idle"}},
            {"id":"root-b","cwd":"/tmp","recencyAt":3,"isPinned":false,"source":"vscode","status":{"type":"idle"}},
            {"id":"exec-root","cwd":"/tmp","recencyAt":2,"isPinned":false,"source":"exec","status":{"type":"idle"}},
            {"id":"child-out","cwd":"/tmp","recencyAt":1,"isPinned":false,"source":{"subAgent":"review"},"status":{"type":"idle"}}
        ],"nextCursor":null}}"#).unwrap()).unwrap();
        let body = format!(
            r#"printf '%s\n' '{active}';
                IFS= read -r line; printf '%s\n' '{{"id":4,"result":{{"data":[],"nextCursor":null}}}}';
                IFS= read -r first_read;
                printf '%s' "$first_read" | grep -q '"includeTurns":false' || exit 91;
                IFS= read -r second_read;
                printf '%s' "$second_read" | grep -q '"includeTurns":false' || exit 92;
                printf '%s\n' '{{"id":6,"result":{{"thread":{{"id":"child-out","cwd":"/tmp","recencyAt":1,"isPinned":false,"parentThreadId":"exec-root","source":{{"subAgent":"review"}},"status":{{"type":"idle"}}}}}}}}';
                printf '%s\n' '{{"id":5,"result":{{"thread":{{"id":"child-in","cwd":"/tmp","recencyAt":4,"isPinned":false,"parentThreadId":"root-a","source":{{"subAgent":"review"}},"status":{{"type":"idle"}}}}}}}}';
                IFS= read -r line; printf '%s\n' '{{"id":7,"result":{{"data":[{{"id":"child-in","cwd":"/tmp","recencyAt":4,"isPinned":false,"parentThreadId":"root-a","source":{{"subAgent":"review"}},"status":{{"type":"idle"}}}}],"nextCursor":null}}}}';
                IFS= read -r line; printf '%s\n' '{{"id":8,"result":{{"data":[],"nextCursor":null}}}}';
                IFS= read -r line; printf '%s\n' '{{"id":9,"result":{{"data":[],"nextCursor":null}}}}';
                IFS= read -r line; printf '%s\n' '{active_10}';
                IFS= read -r line; printf '%s\n' '{{"id":11,"result":{{"data":[],"nextCursor":null}}}}';
                IFS= read -r line; printf '%s\n' '{{"id":12,"result":{{"data":[],"nextCursor":null}}}}'"#,
            active = active,
            active_10 = active.replacen("\"id\":3", "\"id\":10", 1),
        );
        let (_directory, mut client) = spawn_mock_client(&body, None, None).await;
        let snapshot = client.scan().await.unwrap();

        assert!(snapshot.relation_complete);
        assert!(snapshot.write_capable);
        let ids = snapshot
            .nodes
            .iter()
            .map(|node| node.id.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(ids, BTreeSet::from(["child-in", "root-a", "root-b"]));
        let trees = crate::domain::build_trees(&snapshot);
        assert_eq!(trees.len(), 2);
        assert!(trees.iter().all(|tree| tree.protection.is_empty()));
        assert_eq!(
            trees
                .iter()
                .find(|tree| tree.root_id == "root-a")
                .unwrap()
                .nodes
                .len(),
            2
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn nonresponsive_request_times_out_and_invalidates_connection() {
        let (_directory, mut client) = spawn_mock_client("sleep 5", None, None).await;
        client.request_timeout = Duration::from_millis(100);
        let started = tokio::time::Instant::now();
        assert!(client.scan().await.is_err());
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(!client.usable);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn pin_filter_transport_failure_cannot_fall_back_to_write_capable() {
        let body = "printf '%s\\n' '{\"id\":3,\"result\":{\"data\":[{\"id\":\"root\",\"cwd\":\"/tmp\",\"recencyAt\":1,\"isPinned\":false,\"source\":\"cli\",\"status\":{\"type\":\"idle\"}}],\"nextCursor\":null}}'; \
                    IFS= read -r line; printf '%s\\n' '{\"id\":4,\"result\":{\"data\":[],\"nextCursor\":null}}'; \
                    IFS= read -r line; printf '%s\\n' '{\"id\":5,\"result\":{\"data\":[],\"nextCursor\":null}}'; \
                    IFS= read -r line; printf '%s\\n' '{\"id\":6,\"result\":{\"data\":[],\"nextCursor\":null}}'; \
                    IFS= read -r line; sleep 5";
        let (_mock_directory, mut client) = spawn_mock_client(body, None, None).await;
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
    async fn mutations_use_official_cli_commands_and_force_only_delete() {
        let (directory, mut client) = spawn_mock_client(":", None, None).await;
        let log = directory.path().join("cli-arguments.log");

        let archive = client.mutate(Action::Archive, "archive-id").await.unwrap();
        let restore = client.mutate(Action::Restore, "restore-id").await.unwrap();
        let delete = client.mutate(Action::Delete, "delete-id").await.unwrap();

        assert!(archive.response_received && !archive.notification_seen);
        assert!(restore.response_received && !restore.notification_seen);
        assert!(delete.response_received && !delete.notification_seen);
        assert_eq!(
            fs::read_to_string(log).unwrap(),
            "archive\narchive-id\nunarchive\nrestore-id\ndelete\n--force\ndelete-id\n"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cli_mutation_failure_and_timeout_preserve_actionable_errors() {
        let (directory, mut client) = spawn_mock_client(":", None, None).await;
        let failure_marker = directory.path().join("cli-fail");
        fs::write(&failure_marker, b"").unwrap();
        let error = client
            .mutate(Action::Archive, "archive-id")
            .await
            .unwrap_err();
        assert!(matches!(error, VaultError::Command(_)));
        assert!(error.to_string().contains("synthetic CLI failure"));

        fs::remove_file(failure_marker).unwrap();
        fs::write(directory.path().join("cli-sleep"), b"").unwrap();
        client.request_timeout = Duration::from_millis(100);
        let started = tokio::time::Instant::now();
        let error = client
            .mutate(Action::Delete, "delete-id")
            .await
            .unwrap_err();
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(error.to_string().contains("timed out"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn response_followed_by_process_exit_is_reported_and_invalidated() {
        let body =
            "printf '%s\\n' '{\"id\":3,\"result\":{\"data\":[],\"nextCursor\":null}}'; exit 0";
        let (_directory, mut client) = spawn_mock_client(body, None, None).await;
        client.request_timeout = Duration::from_millis(200);
        assert!(client.scan().await.is_err());
        assert!(!client.usable);
    }
}
