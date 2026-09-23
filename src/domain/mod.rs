use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    future::Future,
    pin::Pin,
};

pub type PortFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, VaultError>> + Send + 'a>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VaultError {
    Unavailable(String),
    Protocol(String),
    Command(String),
    Storage(String),
    Blocked(String),
    StalePreview,
    ConfirmationMismatch,
}

impl fmt::Display for VaultError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable(message) => write!(f, "app-server unavailable: {message}"),
            Self::Protocol(message) => write!(f, "app-server protocol error: {message}"),
            Self::Command(message) => write!(f, "Codex CLI command failed: {message}"),
            Self::Storage(message) => {
                write!(f, "local operation record unavailable: {message}")
            }
            Self::Blocked(message) => write!(f, "operation blocked: {message}"),
            Self::StalePreview => write!(f, "preview is stale; rescan and preview again"),
            Self::ConfirmationMismatch => {
                write!(f, "confirmation does not match the previewed delete count")
            }
        }
    }
}

impl std::error::Error for VaultError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Language {
    En,
    ZhCn,
}

impl Language {
    pub fn code(self) -> &'static str {
        match self {
            Self::En => "en",
            Self::ZhCn => "zh-CN",
        }
    }

    pub fn parse(value: &str) -> Self {
        if value.eq_ignore_ascii_case("en") {
            Self::En
        } else {
            Self::ZhCn
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cutoff {
    All,
    RollingDays(u32),
    Absolute(i64),
}

impl Default for Cutoff {
    fn default() -> Self {
        Self::RollingDays(30)
    }
}

impl Cutoff {
    pub fn epoch_seconds(&self, now: i64) -> i64 {
        match self {
            Self::All => i64::MAX,
            Self::RollingDays(days) => now.saturating_sub(i64::from(*days) * 86_400),
            Self::Absolute(value) => *value,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Preferences {
    pub language: Language,
    pub cutoff: Cutoff,
    pub project: Option<String>,
    pub archived: Option<bool>,
}

impl Default for Preferences {
    fn default() -> Self {
        Self {
            language: Language::ZhCn,
            cutoff: Cutoff::default(),
            project: None,
            archived: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionSource {
    Cli,
    Vscode,
    Exec,
    AppServer,
    Descendant,
    Other(String),
    Unknown,
}

impl SessionSource {
    pub fn is_interactive_root(&self) -> bool {
        matches!(self, Self::Cli | Self::Vscode)
    }

    pub fn is_known(&self) -> bool {
        !matches!(self, Self::Other(_) | Self::Unknown)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeStatus {
    NotLoaded,
    Idle,
    SystemError,
    Active(Vec<String>),
    Unknown(String),
}

impl RuntimeStatus {
    pub fn protection_reason(&self) -> Option<ProtectionReason> {
        match self {
            Self::NotLoaded | Self::Idle => None,
            Self::Active(flags) if flags.iter().any(|flag| flag == "waitingOnApproval") => {
                Some(ProtectionReason::WaitingApproval)
            }
            Self::Active(flags) if flags.iter().any(|flag| flag == "waitingOnUserInput") => {
                Some(ProtectionReason::WaitingInput)
            }
            Self::Active(_) => Some(ProtectionReason::Running),
            Self::SystemError => Some(ProtectionReason::UnsafeStatus("systemError".into())),
            Self::Unknown(value) => Some(ProtectionReason::UnsafeStatus(value.clone())),
        }
    }

    fn signature(&self) -> String {
        match self {
            Self::NotLoaded => "notLoaded".into(),
            Self::Idle => "idle".into(),
            Self::SystemError => "systemError".into(),
            Self::Active(flags) => format!("active:{}", flags.join(",")),
            Self::Unknown(value) => format!("unknown:{value}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtectionReason {
    Pinned,
    PinnedUnknown,
    Running,
    WaitingApproval,
    WaitingInput,
    UnsafeStatus(String),
    UnverifiableSource,
    MissingTimestamp,
    IncompleteRelations,
    WriteCapabilityMissing,
    StorageUnavailable,
}

impl fmt::Display for ProtectionReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pinned => write!(f, "pinned"),
            Self::PinnedUnknown => write!(f, "pin state unknown"),
            Self::Running => write!(f, "running or queued"),
            Self::WaitingApproval => write!(f, "waiting for approval"),
            Self::WaitingInput => write!(f, "waiting for user input"),
            Self::UnsafeStatus(value) => write!(f, "unsafe status: {value}"),
            Self::UnverifiableSource => write!(f, "thread source is unverifiable"),
            Self::MissingTimestamp => write!(f, "last activity is unavailable"),
            Self::IncompleteRelations => write!(f, "thread relationship is incomplete"),
            Self::WriteCapabilityMissing => write!(f, "required app-server capability is missing"),
            Self::StorageUnavailable => write!(f, "local operation record is unavailable"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionNode {
    pub id: String,
    pub title: String,
    pub project: Option<String>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub cwd: String,
    /// Size of the validated rollout file at scan time; absent when unavailable.
    pub rollout_bytes: Option<u64>,
    pub last_activity: Option<i64>,
    pub archived: bool,
    pub pinned: Option<bool>,
    pub status: RuntimeStatus,
    pub parent_id: Option<String>,
    pub source: SessionSource,
}

impl SessionNode {
    pub fn protection_reason(&self) -> Option<ProtectionReason> {
        match self.pinned {
            Some(true) => return Some(ProtectionReason::Pinned),
            None => return Some(ProtectionReason::PinnedUnknown),
            Some(false) => {}
        }
        if !self.source.is_known() {
            return Some(ProtectionReason::UnverifiableSource);
        }
        if self.last_activity.is_none() {
            return Some(ProtectionReason::MissingTimestamp);
        }
        self.status.protection_reason()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanSnapshot {
    pub nodes: Vec<SessionNode>,
    pub write_capable: bool,
    pub relation_complete: bool,
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionTree {
    pub root_id: String,
    pub nodes: Vec<SessionNode>,
    pub last_activity: Option<i64>,
    pub protection: Vec<ProtectionReason>,
}

impl SessionTree {
    pub fn storage_bytes(&self) -> Option<u64> {
        self.nodes
            .iter()
            .try_fold(0u64, |total, node| total.checked_add(node.rollout_bytes?))
    }

    pub fn is_fully_archived(&self) -> bool {
        self.nodes.iter().all(|node| node.archived)
    }

    pub fn impacted_ids(&self, action: Action) -> Vec<String> {
        let mut children = BTreeMap::<&str, Vec<&SessionNode>>::new();
        let mut roots = Vec::new();
        for node in &self.nodes {
            if let Some(parent) = node.parent_id.as_deref() {
                children.entry(parent).or_default().push(node);
            } else {
                roots.push(node);
            }
        }
        let mut depths = BTreeMap::<&str, usize>::new();
        let mut stack = roots
            .into_iter()
            .map(|node| (node, 0usize))
            .collect::<Vec<_>>();
        while let Some((node, depth)) = stack.pop() {
            if depths.insert(node.id.as_str(), depth).is_some() {
                continue;
            }
            if let Some(values) = children.get(node.id.as_str()) {
                stack.extend(values.iter().map(|child| (*child, depth.saturating_add(1))));
            }
        }
        let mut nodes = self
            .nodes
            .iter()
            .map(|node| (depths.get(node.id.as_str()).copied().unwrap_or(0), node))
            .collect::<Vec<_>>();
        nodes.sort_by(|(left_depth, left), (right_depth, right)| {
            left_depth
                .cmp(right_depth)
                .then_with(|| left.id.cmp(&right.id))
        });
        if matches!(action, Action::Archive | Action::Delete) {
            nodes.reverse();
        }
        nodes
            .into_iter()
            .map(|(_, node)| node)
            .filter(|node| match action {
                Action::Archive => !node.archived,
                Action::Restore => node.archived,
                Action::Delete => node.archived,
                Action::Cleanup => false,
            })
            .map(|node| node.id.clone())
            .collect()
    }

    pub fn signature(&self) -> String {
        let mut values = self
            .nodes
            .iter()
            .map(|node| {
                format!(
                    "{}|{}|{}|{}|{}|{}|{}|{}",
                    node.id,
                    node.parent_id.as_deref().unwrap_or(""),
                    node.archived,
                    node.provider.as_deref().unwrap_or(""),
                    node.model.as_deref().unwrap_or(""),
                    node.pinned
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "unknown".into()),
                    node.last_activity
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "unknown".into()),
                    node.status.signature()
                )
            })
            .collect::<Vec<_>>();
        values.sort();
        values.join(";")
    }
}

pub fn build_trees(snapshot: &ScanSnapshot) -> Vec<SessionTree> {
    let roots = snapshot
        .nodes
        .iter()
        .filter(|node| node.parent_id.is_none() && node.source.is_interactive_root())
        .collect::<Vec<_>>();
    let mut children = BTreeMap::<&str, Vec<&SessionNode>>::new();
    for node in &snapshot.nodes {
        if let Some(parent) = node.parent_id.as_deref() {
            children.entry(parent).or_default().push(node);
        }
    }
    for values in children.values_mut() {
        values.sort_by(|left, right| right.id.cmp(&left.id));
    }
    let mut trees = Vec::new();

    for root in roots {
        let mut nodes = Vec::new();
        let mut stack = vec![root];
        let mut visited = BTreeSet::new();
        while let Some(node) = stack.pop() {
            if !visited.insert(node.id.as_str()) {
                continue;
            }
            nodes.push(node.clone());
            if let Some(values) = children.get(node.id.as_str()) {
                stack.extend(values.iter().copied());
            }
        }
        nodes.sort_by(|left, right| left.id.cmp(&right.id));
        let last_activity = nodes.iter().filter_map(|node| node.last_activity).max();
        let mut protection = nodes
            .iter()
            .filter_map(SessionNode::protection_reason)
            .collect::<Vec<_>>();
        if !snapshot.relation_complete {
            protection.push(ProtectionReason::IncompleteRelations);
        }
        if !snapshot.write_capable {
            protection.push(ProtectionReason::WriteCapabilityMissing);
        }
        trees.push(SessionTree {
            root_id: root.id.clone(),
            nodes,
            last_activity,
            protection,
        });
    }
    trees.sort_by_key(|tree| std::cmp::Reverse(tree.last_activity.unwrap_or(i64::MIN)));
    trees
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Filter {
    pub cutoff: Cutoff,
    pub project: Option<String>,
    pub query: Option<String>,
    pub archived: Option<bool>,
}

impl Filter {
    pub fn matches(&self, tree: &SessionTree, now: i64) -> bool {
        let Some(last_activity) = tree.last_activity else {
            return false;
        };
        if last_activity > self.cutoff.epoch_seconds(now) {
            return false;
        }
        if let Some(archived) = self.archived {
            if tree.is_fully_archived() != archived {
                return false;
            }
        }
        if let Some(project) = self.project.as_deref() {
            if !tree
                .nodes
                .iter()
                .any(|node| node.project.as_deref() == Some(project) || node.cwd == project)
            {
                return false;
            }
        }
        if let Some(query) = self.query.as_deref() {
            if !query.split_whitespace().all(|token| {
                tree.nodes
                    .iter()
                    .any(|node| node_matches_token(node, token))
            }) {
                return false;
            }
        }
        true
    }
}

fn node_matches_token(node: &SessionNode, token: &str) -> bool {
    let (field, value) = token
        .split_once(':')
        .map_or((None, token), |(field, value)| (Some(field), value));
    let value = value.to_lowercase();
    let contains = |candidate: &str| candidate.to_lowercase().contains(&value);
    match field {
        Some("id") => contains(&node.id),
        Some("title") => contains(&node.title),
        Some("project") => contains(node.project.as_deref().unwrap_or("")) || contains(&node.cwd),
        Some("provider") => contains(node.provider.as_deref().unwrap_or("")),
        Some("model") => contains(node.model.as_deref().unwrap_or("")),
        Some("cwd") => contains(&node.cwd),
        Some(_) => false,
        None => [
            node.id.as_str(),
            node.title.as_str(),
            node.cwd.as_str(),
            node.project.as_deref().unwrap_or(""),
            node.provider.as_deref().unwrap_or(""),
            node.model.as_deref().unwrap_or(""),
        ]
        .iter()
        .any(|candidate| contains(candidate)),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Archive,
    Restore,
    Delete,
    Cleanup,
}

impl Action {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Archive => "archive",
            Self::Restore => "restore",
            Self::Delete => "delete",
            Self::Cleanup => "cleanup",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "archive" => Some(Self::Archive),
            "restore" => Some(Self::Restore),
            "delete" => Some(Self::Delete),
            "cleanup" => Some(Self::Cleanup),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MaintenanceKind {
    UnreferencedRollout,
    StaleSpawnEdge,
    MissingRolloutThread,
}

impl MaintenanceKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::UnreferencedRollout => "unreferenced_rollout",
            Self::StaleSpawnEdge => "stale_spawn_edge",
            Self::MissingRolloutThread => "missing_rollout_thread",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceCandidate {
    pub key: String,
    pub kind: MaintenanceKind,
    pub label: String,
    pub detail: String,
    pub project: Option<String>,
    pub bytes: u64,
    pub fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenancePlan {
    pub candidates: Vec<MaintenanceCandidate>,
    pub total_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceAck {
    pub backup_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedTree {
    pub(crate) root_id: String,
    pub(crate) signature: String,
    pub(crate) node_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationPreview {
    pub(crate) action: Action,
    pub(crate) trees: Vec<PlannedTree>,
    pub(crate) impacted_count: usize,
    pub(crate) blocked: Vec<(String, Vec<ProtectionReason>)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutationAck {
    pub response_received: bool,
    pub notification_seen: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ItemResult {
    Success,
    Skipped,
    Failed(String),
    Interrupted,
}

impl ItemResult {
    pub fn as_storage_value(&self) -> String {
        match self {
            Self::Success => "success".into(),
            Self::Skipped => "skipped".into(),
            Self::Failed(message) => format!("failed:{message}"),
            Self::Interrupted => "interrupted".into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationResult {
    pub batch_id: String,
    pub action: Action,
    pub items: BTreeMap<String, ItemResult>,
    pub warnings: Vec<String>,
}

impl OperationResult {
    pub fn is_complete_success(&self) -> bool {
        self.items
            .values()
            .all(|value| matches!(value, ItemResult::Success | ItemResult::Skipped))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryEntry {
    pub batch_id: String,
    pub created_at: i64,
    pub action: String,
    pub planned_count: usize,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingBatch {
    pub batch_id: String,
    pub action: Action,
    pub node_ids: Vec<String>,
}

pub trait SessionGateway {
    fn target_summary(&self) -> Option<&str> {
        None
    }

    fn scan(&mut self) -> PortFuture<'_, ScanSnapshot>;
    fn mutate<'a>(&'a mut self, action: Action, thread_id: &'a str) -> PortFuture<'a, MutationAck>;

    fn maintenance_candidates(&self) -> &[MaintenanceCandidate] {
        &[]
    }

    fn cleanup<'a>(
        &'a mut self,
        _batch_id: &'a str,
        _candidate: &'a MaintenanceCandidate,
    ) -> PortFuture<'a, MaintenanceAck> {
        Box::pin(async {
            Err(VaultError::Blocked(
                "dirty-data maintenance is unavailable".into(),
            ))
        })
    }
}

pub trait OperationStore {
    fn write_ready(&self) -> Result<(), VaultError>;
    fn load_preferences(&self) -> Result<Preferences, VaultError>;
    fn save_preferences(&mut self, value: &Preferences) -> Result<(), VaultError>;
    fn begin_batch(
        &mut self,
        plan_key: &str,
        action: Action,
        created_at: i64,
        node_ids: &[String],
    ) -> Result<String, VaultError>;
    fn complete_batch(
        &mut self,
        batch_id: &str,
        results: &BTreeMap<String, ItemResult>,
        status: &str,
        completed_at: i64,
    ) -> Result<(), VaultError>;
    fn pending_batches(&self) -> Result<Vec<PendingBatch>, VaultError>;
    fn prune(&mut self, now: i64, retention_days: u32) -> Result<usize, VaultError>;
    fn history(&self, limit: usize) -> Result<Vec<HistoryEntry>, VaultError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: &str, parent: Option<&str>, source: SessionSource, archived: bool) -> SessionNode {
        SessionNode {
            id: id.into(),
            title: id.into(),
            project: Some("p".into()),
            provider: Some("free".into()),
            model: Some("gpt-test".into()),
            cwd: "/p".into(),
            rollout_bytes: Some(512),
            last_activity: Some(100),
            archived,
            pinned: Some(false),
            status: RuntimeStatus::Idle,
            parent_id: parent.map(str::to_owned),
            source,
        }
    }

    #[test]
    fn builds_only_interactive_roots_with_all_descendants() {
        let snapshot = ScanSnapshot {
            nodes: vec![
                node("root", None, SessionSource::Cli, false),
                node("child", Some("root"), SessionSource::Descendant, false),
                node("exec", None, SessionSource::Exec, false),
            ],
            write_capable: true,
            relation_complete: true,
            diagnostics: vec![],
        };
        let trees = build_trees(&snapshot);
        assert_eq!(trees.len(), 1);
        assert_eq!(trees[0].nodes.len(), 2);
        assert_eq!(trees[0].root_id, "root");
    }

    #[test]
    fn tree_storage_sums_descendants_without_claiming_incomplete_sizes() {
        let mut tree = SessionTree {
            root_id: "root".into(),
            nodes: vec![
                node("root", None, SessionSource::Cli, false),
                node("child", Some("root"), SessionSource::Descendant, false),
            ],
            last_activity: Some(100),
            protection: vec![],
        };
        assert_eq!(tree.storage_bytes(), Some(1024));
        tree.nodes[1].rollout_bytes = None;
        assert_eq!(tree.storage_bytes(), None);
        tree.nodes[1].rollout_bytes = Some(u64::MAX);
        assert_eq!(tree.storage_bytes(), None);
    }

    #[test]
    fn one_day_is_a_rolling_twenty_four_hours() {
        let tree = SessionTree {
            root_id: "root".into(),
            nodes: vec![node("root", None, SessionSource::Cli, false)],
            last_activity: Some(86_400),
            protection: vec![],
        };
        let filter = Filter {
            cutoff: Cutoff::RollingDays(1),
            project: None,
            query: None,
            archived: None,
        };
        assert!(filter.matches(&tree, 172_800));
        assert!(!filter.matches(&tree, 172_799));
    }

    #[test]
    fn all_cutoff_includes_recent_activity() {
        let tree = SessionTree {
            root_id: "root".into(),
            nodes: vec![node("root", None, SessionSource::Cli, false)],
            last_activity: Some(9_999),
            protection: vec![],
        };
        let filter = Filter {
            cutoff: Cutoff::All,
            project: None,
            query: None,
            archived: None,
        };
        assert!(filter.matches(&tree, 100));
    }

    #[test]
    fn archive_and_project_filters_keep_their_and_semantics() {
        let tree = SessionTree {
            root_id: "root".into(),
            nodes: vec![node("root", None, SessionSource::Cli, true)],
            last_activity: Some(86_400),
            protection: vec![],
        };
        let matching = Filter {
            cutoff: Cutoff::RollingDays(1),
            project: Some("p".into()),
            query: None,
            archived: Some(true),
        };
        assert!(matching.matches(&tree, 172_800));

        let wrong_archive = Filter {
            archived: Some(false),
            ..matching.clone()
        };
        assert!(!wrong_archive.matches(&tree, 172_800));

        let wrong_project = Filter {
            project: Some("other".into()),
            ..matching
        };
        assert!(!wrong_project.matches(&tree, 172_800));
    }

    #[test]
    fn protected_descendant_blocks_the_tree() {
        let mut child = node("child", Some("root"), SessionSource::Descendant, false);
        child.status = RuntimeStatus::Active(vec!["waitingOnApproval".into()]);
        let trees = build_trees(&ScanSnapshot {
            nodes: vec![node("root", None, SessionSource::Cli, false), child],
            write_capable: true,
            relation_complete: true,
            diagnostics: vec![],
        });
        assert_eq!(trees[0].protection, vec![ProtectionReason::WaitingApproval]);
    }

    #[test]
    fn unverifiable_source_blocks_only_its_known_tree() {
        let trees = build_trees(&ScanSnapshot {
            nodes: vec![
                node("root-a", None, SessionSource::Cli, false),
                node(
                    "future-child",
                    Some("root-a"),
                    SessionSource::Other("futureSource".into()),
                    false,
                ),
                node("root-b", None, SessionSource::Vscode, false),
            ],
            write_capable: true,
            relation_complete: true,
            diagnostics: vec![],
        });
        let protected = trees.iter().find(|tree| tree.root_id == "root-a").unwrap();
        let eligible = trees.iter().find(|tree| tree.root_id == "root-b").unwrap();
        assert_eq!(
            protected.protection,
            vec![ProtectionReason::UnverifiableSource]
        );
        assert!(eligible.protection.is_empty());
    }

    #[test]
    fn archive_and_delete_are_descendant_first() {
        let tree = SessionTree {
            root_id: "root".into(),
            nodes: vec![
                node("root", None, SessionSource::Cli, false),
                node("child", Some("root"), SessionSource::Descendant, false),
            ],
            last_activity: Some(100),
            protection: vec![],
        };
        assert_eq!(tree.impacted_ids(Action::Archive), vec!["child", "root"]);
    }

    #[test]
    fn thousands_of_roots_and_a_deep_tree_are_built_iteratively() {
        let many_roots = ScanSnapshot {
            nodes: (0..5_000)
                .map(|index| node(&format!("root-{index}"), None, SessionSource::Cli, false))
                .collect(),
            write_capable: true,
            relation_complete: true,
            diagnostics: vec![],
        };
        let trees = build_trees(&many_roots);
        assert_eq!(trees.len(), 5_000);
        assert!(trees.iter().all(|tree| tree.nodes.len() == 1));

        let mut nodes = Vec::with_capacity(5_000);
        nodes.push(node("node-0", None, SessionSource::Cli, false));
        for index in 1..5_000 {
            nodes.push(node(
                &format!("node-{index}"),
                Some(&format!("node-{}", index - 1)),
                SessionSource::Descendant,
                false,
            ));
        }
        let mut deep = build_trees(&ScanSnapshot {
            nodes,
            write_capable: true,
            relation_complete: true,
            diagnostics: vec![],
        });
        assert_eq!(deep.len(), 1);
        let impacted = deep.remove(0).impacted_ids(Action::Archive);
        assert_eq!(impacted.len(), 5_000);
        assert_eq!(impacted.first().map(String::as_str), Some("node-4999"));
        assert_eq!(impacted.last().map(String::as_str), Some("node-0"));
    }

    #[test]
    fn field_filters_compose_with_and_semantics() {
        let tree = SessionTree {
            root_id: "root".into(),
            nodes: vec![SessionNode {
                id: "root".into(),
                title: "Release cleanup".into(),
                project: Some("vault".into()),
                provider: Some("custom".into()),
                model: Some("gpt-5.5".into()),
                cwd: "/work/vault".into(),
                rollout_bytes: Some(512),
                last_activity: Some(1),
                archived: false,
                pinned: Some(false),
                status: RuntimeStatus::Idle,
                parent_id: None,
                source: SessionSource::Cli,
            }],
            last_activity: Some(1),
            protection: vec![],
        };
        let filter = Filter {
            cutoff: Cutoff::RollingDays(1),
            project: None,
            query: Some(
                "project:/work title:cleanup cwd:/work provider:custom model:gpt-5.5".into(),
            ),
            archived: None,
        };
        assert!(filter.matches(&tree, 200_000));
    }
}
