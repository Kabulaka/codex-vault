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
    Descendant,
    Other(String),
    Unknown,
}

impl SessionSource {
    pub fn is_interactive_root(&self) -> bool {
        matches!(self, Self::Cli | Self::Vscode)
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
    pub cwd: String,
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
    pub fn is_fully_archived(&self) -> bool {
        self.nodes.iter().all(|node| node.archived)
    }

    pub fn impacted_ids(&self, action: Action) -> Vec<String> {
        let mut nodes = self.nodes.clone();
        nodes.sort_by_key(|node| depth(node, &self.nodes));
        if matches!(action, Action::Archive | Action::Delete) {
            nodes.reverse();
        }
        nodes
            .into_iter()
            .filter(|node| match action {
                Action::Archive => !node.archived,
                Action::Restore => node.archived,
                Action::Delete => node.archived,
            })
            .map(|node| node.id)
            .collect()
    }

    pub fn signature(&self) -> String {
        let mut values = self
            .nodes
            .iter()
            .map(|node| {
                format!(
                    "{}|{}|{}|{}|{}|{}",
                    node.id,
                    node.parent_id.as_deref().unwrap_or(""),
                    node.archived,
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

fn depth(node: &SessionNode, all: &[SessionNode]) -> usize {
    let by_id = all
        .iter()
        .map(|value| (value.id.as_str(), value))
        .collect::<BTreeMap<_, _>>();
    let mut current = node.parent_id.as_deref();
    let mut seen = BTreeSet::new();
    let mut value = 0;
    while let Some(parent) = current {
        if !seen.insert(parent) {
            break;
        }
        value += 1;
        current = by_id.get(parent).and_then(|item| item.parent_id.as_deref());
    }
    value
}

pub fn build_trees(snapshot: &ScanSnapshot) -> Vec<SessionTree> {
    let by_id = snapshot
        .nodes
        .iter()
        .map(|node| (node.id.as_str(), node))
        .collect::<BTreeMap<_, _>>();
    let roots = snapshot
        .nodes
        .iter()
        .filter(|node| node.parent_id.is_none() && node.source.is_interactive_root());
    let mut trees = Vec::new();

    for root in roots {
        let mut nodes = snapshot
            .nodes
            .iter()
            .filter(|candidate| belongs_to_root(candidate, root.id.as_str(), &by_id))
            .cloned()
            .collect::<Vec<_>>();
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

fn belongs_to_root<'a>(
    candidate: &'a SessionNode,
    root: &str,
    by_id: &BTreeMap<&'a str, &'a SessionNode>,
) -> bool {
    if candidate.id == root {
        return true;
    }
    let mut current = candidate.parent_id.as_deref();
    let mut seen = BTreeSet::new();
    while let Some(parent) = current {
        if parent == root {
            return true;
        }
        if !seen.insert(parent) {
            return false;
        }
        current = by_id.get(parent).and_then(|node| node.parent_id.as_deref());
    }
    false
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
        if let Some(archived) = self.archived
            && tree.is_fully_archived() != archived
        {
            return false;
        }
        if let Some(project) = self.project.as_deref()
            && !tree
                .nodes
                .iter()
                .any(|node| node.project.as_deref() == Some(project) || node.cwd == project)
        {
            return false;
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
        Some("project") => contains(node.project.as_deref().unwrap_or("")),
        Some("cwd") => contains(&node.cwd),
        Some(_) => false,
        None => [
            node.id.as_str(),
            node.title.as_str(),
            node.cwd.as_str(),
            node.project.as_deref().unwrap_or(""),
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
}

impl Action {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Archive => "archive",
            Self::Restore => "restore",
            Self::Delete => "delete",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedTree {
    pub root_id: String,
    pub signature: String,
    pub node_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationPreview {
    pub action: Action,
    pub trees: Vec<PlannedTree>,
    pub impacted_count: usize,
    pub blocked: Vec<(String, Vec<ProtectionReason>)>,
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

pub trait SessionGateway {
    fn scan(&mut self) -> PortFuture<'_, ScanSnapshot>;
    fn mutate<'a>(&'a mut self, action: Action, thread_id: &'a str) -> PortFuture<'a, MutationAck>;
}

pub trait OperationStore {
    fn write_ready(&self) -> Result<(), VaultError>;
    fn load_preferences(&self) -> Result<Preferences, VaultError>;
    fn save_preferences(&mut self, value: &Preferences) -> Result<(), VaultError>;
    fn begin_batch(
        &mut self,
        batch_id: &str,
        action: Action,
        created_at: i64,
        node_ids: &[String],
    ) -> Result<(), VaultError>;
    fn record_result(
        &mut self,
        batch_id: &str,
        node_id: &str,
        result: &ItemResult,
        completed_at: i64,
    ) -> Result<(), VaultError>;
    fn finish_batch(&mut self, batch_id: &str, status: &str) -> Result<(), VaultError>;
    fn recover_interrupted(&mut self) -> Result<usize, VaultError>;
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
            cwd: "/p".into(),
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
    fn field_filters_compose_with_and_semantics() {
        let tree = SessionTree {
            root_id: "root".into(),
            nodes: vec![SessionNode {
                id: "root".into(),
                title: "Release cleanup".into(),
                project: Some("vault".into()),
                cwd: "/work/vault".into(),
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
            query: Some("project:vault title:cleanup cwd:/work".into()),
            archived: None,
        };
        assert!(filter.matches(&tree, 200_000));
    }
}
