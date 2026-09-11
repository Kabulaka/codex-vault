use std::{
    collections::{BTreeMap, BTreeSet},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::domain::{
    Action, Filter, ItemResult, OperationPreview, OperationResult, OperationStore, PlannedTree,
    Preferences, ScanSnapshot, SessionGateway, SessionTree, VaultError, build_trees,
};

pub struct VaultService<G, S> {
    gateway: G,
    store: S,
    snapshot: Option<ScanSnapshot>,
    trees: Vec<SessionTree>,
}

impl<G, S> VaultService<G, S>
where
    G: SessionGateway,
    S: OperationStore,
{
    pub fn new(gateway: G, store: S) -> Self {
        Self {
            gateway,
            store,
            snapshot: None,
            trees: Vec::new(),
        }
    }

    pub fn load_preferences(&self) -> Result<Preferences, VaultError> {
        self.store.load_preferences()
    }

    pub fn save_preferences(&mut self, value: &Preferences) -> Result<(), VaultError> {
        self.store.save_preferences(value)
    }

    pub fn recover_and_prune(&mut self) -> Result<(usize, usize), VaultError> {
        let now = now_epoch();
        let recovered = self.store.recover_interrupted()?;
        let pruned = self.store.prune(now, 90)?;
        Ok((recovered, pruned))
    }

    pub async fn refresh(&mut self) -> Result<&[SessionTree], VaultError> {
        let snapshot = self.gateway.scan().await?;
        self.trees = build_trees(&snapshot);
        self.snapshot = Some(snapshot);
        Ok(&self.trees)
    }

    pub fn trees(&self) -> &[SessionTree] {
        &self.trees
    }

    pub fn diagnostics(&self) -> &[String] {
        self.snapshot
            .as_ref()
            .map(|value| value.diagnostics.as_slice())
            .unwrap_or(&[])
    }

    pub fn filtered(&self, filter: &Filter) -> Vec<&SessionTree> {
        let now = now_epoch();
        self.trees
            .iter()
            .filter(|tree| filter.matches(tree, now))
            .collect()
    }

    pub fn preview(&self, roots: &BTreeSet<String>, action: Action) -> OperationPreview {
        let mut trees = Vec::new();
        let mut blocked = Vec::new();
        for tree in self
            .trees
            .iter()
            .filter(|tree| roots.contains(&tree.root_id))
        {
            let mut reasons = tree.protection.clone();
            if self.store.write_ready().is_err() {
                reasons.push(crate::domain::ProtectionReason::StorageUnavailable);
            }
            if action == Action::Delete && !tree.is_fully_archived() {
                reasons.push(crate::domain::ProtectionReason::UnsafeStatus(
                    "delete requires a fully archived tree".into(),
                ));
            }
            if reasons.is_empty() {
                trees.push(PlannedTree {
                    root_id: tree.root_id.clone(),
                    signature: tree.signature(),
                    node_ids: tree.impacted_ids(action),
                });
            } else {
                blocked.push((tree.root_id.clone(), reasons));
            }
        }
        let impacted_count = trees.iter().map(|tree| tree.node_ids.len()).sum();
        OperationPreview {
            action,
            trees,
            impacted_count,
            blocked,
        }
    }

    pub async fn execute(
        &mut self,
        preview: &OperationPreview,
        delete_confirmation: Option<usize>,
    ) -> Result<OperationResult, VaultError> {
        if preview.action == Action::Delete && delete_confirmation != Some(preview.impacted_count) {
            return Err(VaultError::ConfirmationMismatch);
        }
        if preview.trees.is_empty() || preview.impacted_count == 0 {
            return Err(VaultError::Blocked("nothing eligible is selected".into()));
        }

        self.refresh().await?;
        let current = self
            .trees
            .iter()
            .map(|tree| (tree.root_id.as_str(), tree))
            .collect::<BTreeMap<_, _>>();
        for planned in &preview.trees {
            let Some(tree) = current.get(planned.root_id.as_str()) else {
                return Err(VaultError::StalePreview);
            };
            if tree.signature() != planned.signature || !tree.protection.is_empty() {
                return Err(VaultError::StalePreview);
            }
            if preview.action == Action::Delete && !tree.is_fully_archived() {
                return Err(VaultError::StalePreview);
            }
        }

        let batch_id = batch_id();
        let node_ids = preview
            .trees
            .iter()
            .flat_map(|tree| tree.node_ids.iter().cloned())
            .collect::<Vec<_>>();
        self.store
            .begin_batch(&batch_id, preview.action, now_epoch(), node_ids.as_slice())?;

        let mut calls = BTreeMap::new();
        for node_id in &node_ids {
            let result = match self.gateway.mutate(preview.action, node_id).await {
                Ok(_) => ItemResult::Interrupted,
                Err(error) => ItemResult::Failed(error.to_string()),
            };
            calls.insert(node_id.clone(), result);
        }

        let rescan = self.gateway.scan().await;
        let mut items = BTreeMap::new();
        let mut warnings = Vec::new();
        match rescan {
            Ok(snapshot) => {
                let actual = snapshot
                    .nodes
                    .iter()
                    .map(|node| (node.id.as_str(), node))
                    .collect::<BTreeMap<_, _>>();
                for node_id in &node_ids {
                    let observed = match (preview.action, actual.get(node_id.as_str())) {
                        (Action::Archive, Some(node)) if node.archived => ItemResult::Success,
                        (Action::Restore, Some(node)) if !node.archived => ItemResult::Success,
                        (Action::Delete, None) => ItemResult::Success,
                        (_, _) => calls
                            .remove(node_id)
                            .unwrap_or_else(|| ItemResult::Failed("final state mismatch".into())),
                    };
                    items.insert(node_id.clone(), observed);
                }
                self.trees = build_trees(&snapshot);
                self.snapshot = Some(snapshot);
                for (node_id, observed) in &items {
                    if let Err(error) =
                        self.store
                            .record_result(&batch_id, node_id, observed, now_epoch())
                    {
                        warnings.push(format!(
                            "actual state was rescanned but could not be recorded for {node_id}: {error}"
                        ));
                    }
                }
            }
            Err(error) => {
                warnings.push(format!("final app-server rescan failed: {error}"));
                for node_id in &node_ids {
                    let result = match calls.remove(node_id) {
                        Some(ItemResult::Failed(message)) => ItemResult::Failed(message),
                        _ => ItemResult::Interrupted,
                    };
                    if let Err(error) =
                        self.store
                            .record_result(&batch_id, node_id, &result, now_epoch())
                    {
                        warnings.push(format!(
                            "interrupted result could not be recorded for {node_id}: {error}"
                        ));
                    }
                    items.insert(node_id.clone(), result);
                }
                if let Err(error) = self.store.finish_batch(&batch_id, "needs_verification") {
                    warnings.push(format!("batch status could not be recorded: {error}"));
                }
                return Ok(OperationResult {
                    batch_id,
                    action: preview.action,
                    items,
                    warnings,
                });
            }
        }

        let status = if items
            .values()
            .all(|value| matches!(value, ItemResult::Success | ItemResult::Skipped))
        {
            "completed"
        } else {
            "partial"
        };
        if let Err(error) = self.store.finish_batch(&batch_id, status) {
            warnings.push(format!("batch status could not be recorded: {error}"));
        }
        Ok(OperationResult {
            batch_id,
            action: preview.action,
            items,
            warnings,
        })
    }

    pub fn history(&self, limit: usize) -> Result<Vec<crate::domain::HistoryEntry>, VaultError> {
        self.store.history(limit)
    }
}

pub fn now_epoch() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .try_into()
        .unwrap_or(i64::MAX)
}

fn batch_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("batch-{nanos:x}-{:x}", std::process::id())
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex},
    };

    use super::*;
    use crate::domain::{
        HistoryEntry, MutationAck, PortFuture, RuntimeStatus, SessionNode, SessionSource,
    };

    #[derive(Clone)]
    struct Gateway {
        scans: Arc<Mutex<VecDeque<ScanSnapshot>>>,
        calls: Arc<Mutex<Vec<String>>>,
    }

    impl SessionGateway for Gateway {
        fn scan(&mut self) -> PortFuture<'_, ScanSnapshot> {
            let value = self.scans.lock().unwrap().pop_front().unwrap();
            Box::pin(async move { Ok(value) })
        }

        fn mutate<'a>(&'a mut self, _: Action, thread_id: &'a str) -> PortFuture<'a, MutationAck> {
            self.calls.lock().unwrap().push(thread_id.to_owned());
            Box::pin(async {
                Ok(MutationAck {
                    response_received: true,
                    notification_seen: true,
                })
            })
        }
    }

    #[derive(Default)]
    struct Store {
        began: bool,
        fail_begin: bool,
        results: Vec<(String, String)>,
    }

    impl OperationStore for Store {
        fn write_ready(&self) -> Result<(), VaultError> {
            Ok(())
        }
        fn load_preferences(&self) -> Result<Preferences, VaultError> {
            Ok(Preferences::default())
        }
        fn save_preferences(&mut self, _: &Preferences) -> Result<(), VaultError> {
            Ok(())
        }
        fn begin_batch(
            &mut self,
            _: &str,
            _: Action,
            _: i64,
            _: &[String],
        ) -> Result<(), VaultError> {
            if self.fail_begin {
                return Err(VaultError::Storage("read only".into()));
            }
            self.began = true;
            Ok(())
        }
        fn record_result(
            &mut self,
            _: &str,
            id: &str,
            value: &ItemResult,
            _: i64,
        ) -> Result<(), VaultError> {
            self.results.push((id.into(), value.as_storage_value()));
            Ok(())
        }
        fn finish_batch(&mut self, _: &str, _: &str) -> Result<(), VaultError> {
            Ok(())
        }
        fn recover_interrupted(&mut self) -> Result<usize, VaultError> {
            Ok(0)
        }
        fn prune(&mut self, _: i64, _: u32) -> Result<usize, VaultError> {
            Ok(0)
        }
        fn history(&self, _: usize) -> Result<Vec<HistoryEntry>, VaultError> {
            Ok(vec![])
        }
    }

    fn snapshot(archived: bool) -> ScanSnapshot {
        ScanSnapshot {
            nodes: vec![
                SessionNode {
                    id: "root".into(),
                    title: "root".into(),
                    project: None,
                    cwd: "/tmp".into(),
                    last_activity: Some(1),
                    archived,
                    pinned: Some(false),
                    status: RuntimeStatus::Idle,
                    parent_id: None,
                    source: SessionSource::Cli,
                },
                SessionNode {
                    id: "child".into(),
                    title: "child".into(),
                    project: None,
                    cwd: "/tmp".into(),
                    last_activity: Some(1),
                    archived,
                    pinned: Some(false),
                    status: RuntimeStatus::Idle,
                    parent_id: Some("root".into()),
                    source: SessionSource::Descendant,
                },
            ],
            write_capable: true,
            relation_complete: true,
            diagnostics: vec![],
        }
    }

    #[tokio::test]
    async fn persists_plan_before_descendant_first_mutation_and_verifies() {
        let calls = Arc::new(Mutex::new(vec![]));
        let gateway = Gateway {
            scans: Arc::new(Mutex::new(VecDeque::from([
                snapshot(false),
                snapshot(false),
                snapshot(true),
            ]))),
            calls: calls.clone(),
        };
        let mut service = VaultService::new(gateway, Store::default());
        service.refresh().await.unwrap();
        let selected = BTreeSet::from(["root".to_owned()]);
        let preview = service.preview(&selected, Action::Archive);
        let result = service.execute(&preview, None).await.unwrap();
        assert!(result.is_complete_success());
        assert_eq!(*calls.lock().unwrap(), vec!["child", "root"]);
        assert!(service.store.began);
    }

    #[tokio::test]
    async fn refuses_changed_tree_after_preview() {
        let mut changed = snapshot(false);
        changed.nodes[0].pinned = Some(true);
        let gateway = Gateway {
            scans: Arc::new(Mutex::new(VecDeque::from([snapshot(false), changed]))),
            calls: Arc::new(Mutex::new(vec![])),
        };
        let mut service = VaultService::new(gateway, Store::default());
        service.refresh().await.unwrap();
        let preview = service.preview(&BTreeSet::from(["root".into()]), Action::Archive);
        assert_eq!(
            service.execute(&preview, None).await,
            Err(VaultError::StalePreview)
        );
    }

    #[tokio::test]
    async fn exact_delete_count_is_required_before_rescan_or_write() {
        let calls = Arc::new(Mutex::new(vec![]));
        let gateway = Gateway {
            scans: Arc::new(Mutex::new(VecDeque::from([snapshot(true)]))),
            calls: calls.clone(),
        };
        let mut service = VaultService::new(gateway, Store::default());
        service.refresh().await.unwrap();
        let preview = service.preview(&BTreeSet::from(["root".into()]), Action::Delete);
        assert_eq!(
            service.execute(&preview, Some(1)).await,
            Err(VaultError::ConfirmationMismatch)
        );
        assert!(calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn failed_plan_persistence_prevents_every_codex_write() {
        let calls = Arc::new(Mutex::new(vec![]));
        let gateway = Gateway {
            scans: Arc::new(Mutex::new(VecDeque::from([
                snapshot(false),
                snapshot(false),
            ]))),
            calls: calls.clone(),
        };
        let store = Store {
            fail_begin: true,
            ..Store::default()
        };
        let mut service = VaultService::new(gateway, store);
        service.refresh().await.unwrap();
        let preview = service.preview(&BTreeSet::from(["root".into()]), Action::Archive);
        assert!(matches!(
            service.execute(&preview, None).await,
            Err(VaultError::Storage(_))
        ));
        assert!(calls.lock().unwrap().is_empty());
    }
}
