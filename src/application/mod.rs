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
        let pending = self.store.pending_batches()?;
        let snapshot = self
            .snapshot
            .as_ref()
            .ok_or_else(|| VaultError::Blocked("scan before recovering batches".into()))?;
        let actual = snapshot
            .nodes
            .iter()
            .map(|node| (node.id.as_str(), node))
            .collect::<BTreeMap<_, _>>();
        let reliable = snapshot.write_capable && snapshot.relation_complete;
        for batch in &pending {
            let items = batch
                .node_ids
                .iter()
                .map(|node_id| {
                    let success = reliable
                        && match (batch.action, actual.get(node_id.as_str())) {
                            (Action::Archive, Some(node)) => node.archived,
                            (Action::Restore, Some(node)) => !node.archived,
                            (Action::Delete, None) => true,
                            _ => false,
                        };
                    (
                        node_id.clone(),
                        if success {
                            ItemResult::Success
                        } else {
                            ItemResult::Interrupted
                        },
                    )
                })
                .collect::<BTreeMap<_, _>>();
            let status = if items.values().all(|value| *value == ItemResult::Success) {
                "completed"
            } else {
                "interrupted"
            };
            self.store
                .complete_batch(&batch.batch_id, &items, status, now)?;
        }
        let pruned = self.store.prune(now, 90)?;
        Ok((pending.len(), pruned))
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
        let storage_ready = self.store.write_ready().is_ok();
        for tree in self
            .trees
            .iter()
            .filter(|tree| roots.contains(&tree.root_id))
        {
            let mut reasons = tree.protection.clone();
            if !storage_ready {
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
                    signature: plan_signature(tree, action),
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
        if preview.trees.is_empty() || preview.impacted_count == 0 {
            return Err(VaultError::Blocked("nothing eligible is selected".into()));
        }

        self.refresh().await?;
        let current = self
            .trees
            .iter()
            .map(|tree| (tree.root_id.as_str(), tree))
            .collect::<BTreeMap<_, _>>();
        let mut roots = BTreeSet::new();
        let mut all_nodes = BTreeSet::<String>::new();
        let mut canonical = Vec::new();
        let mut impacted_count = 0usize;
        for planned in &preview.trees {
            if !roots.insert(planned.root_id.as_str()) {
                return Err(VaultError::StalePreview);
            }
            let Some(tree) = current.get(planned.root_id.as_str()) else {
                return Err(VaultError::StalePreview);
            };
            let node_ids = tree.impacted_ids(preview.action);
            if plan_signature(tree, preview.action) != planned.signature
                || node_ids != planned.node_ids
                || node_ids.is_empty()
                || !tree.protection.is_empty()
            {
                return Err(VaultError::StalePreview);
            }
            if preview.action == Action::Delete && !tree.is_fully_archived() {
                return Err(VaultError::StalePreview);
            }
            for node_id in &node_ids {
                if !all_nodes.insert(node_id.clone()) {
                    return Err(VaultError::StalePreview);
                }
            }
            impacted_count = impacted_count
                .checked_add(node_ids.len())
                .ok_or(VaultError::StalePreview)?;
            canonical.push(PlannedTree {
                root_id: planned.root_id.clone(),
                signature: planned.signature.clone(),
                node_ids,
            });
        }
        if impacted_count != preview.impacted_count {
            return Err(VaultError::StalePreview);
        }
        if preview.action == Action::Delete && delete_confirmation != Some(impacted_count) {
            return Err(VaultError::ConfirmationMismatch);
        }

        let node_ids = canonical
            .iter()
            .flat_map(|tree| tree.node_ids.iter().cloned())
            .collect::<Vec<_>>();
        let plan_key = plan_key(preview.action, &canonical);
        let batch_id =
            self.store
                .begin_batch(&plan_key, preview.action, now_epoch(), node_ids.as_slice())?;

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
            Ok(snapshot) if snapshot.write_capable && snapshot.relation_complete => {
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
            }
            Ok(snapshot) => {
                warnings.push(
                    "final app-server rescan was incomplete; results require verification".into(),
                );
                self.trees = build_trees(&snapshot);
                self.snapshot = Some(snapshot);
                for node_id in &node_ids {
                    let result = match calls.remove(node_id) {
                        Some(ItemResult::Failed(message)) => ItemResult::Failed(message),
                        _ => ItemResult::Interrupted,
                    };
                    items.insert(node_id.clone(), result);
                }
                if let Err(error) =
                    self.store
                        .complete_batch(&batch_id, &items, "needs_verification", now_epoch())
                {
                    warnings.push(format!("unverified batch could not be recorded: {error}"));
                }
                return Ok(OperationResult {
                    batch_id,
                    action: preview.action,
                    items,
                    warnings,
                });
            }
            Err(error) => {
                warnings.push(format!("final app-server rescan failed: {error}"));
                for node_id in &node_ids {
                    let result = match calls.remove(node_id) {
                        Some(ItemResult::Failed(message)) => ItemResult::Failed(message),
                        _ => ItemResult::Interrupted,
                    };
                    items.insert(node_id.clone(), result);
                }
                if let Err(error) =
                    self.store
                        .complete_batch(&batch_id, &items, "needs_verification", now_epoch())
                {
                    warnings.push(format!("unverified batch could not be recorded: {error}"));
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
        if let Err(error) = self
            .store
            .complete_batch(&batch_id, &items, status, now_epoch())
        {
            warnings.push(format!(
                "actual state was rescanned but the transactional result could not be recorded: {error}"
            ));
            for value in items.values_mut() {
                *value = ItemResult::Interrupted;
            }
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

fn plan_signature(tree: &SessionTree, action: Action) -> String {
    let nodes = tree.impacted_ids(action);
    format!(
        "{}|{}|{}",
        action.as_str(),
        tree.signature(),
        plan_key_nodes(&nodes)
    )
}

fn plan_key(action: Action, trees: &[PlannedTree]) -> String {
    let mut trees = trees.iter().collect::<Vec<_>>();
    trees.sort_by(|left, right| left.root_id.cmp(&right.root_id));
    let mut value = format!("{}|", action.as_str());
    for tree in trees {
        push_len(&mut value, &tree.root_id);
        push_len(&mut value, &tree.signature);
        value.push_str(&plan_key_nodes(&tree.node_ids));
    }
    value
}

fn plan_key_nodes(nodes: &[String]) -> String {
    let mut value = String::new();
    for node in nodes {
        push_len(&mut value, node);
    }
    value
}

fn push_len(target: &mut String, value: &str) {
    target.push_str(&value.len().to_string());
    target.push(':');
    target.push_str(value);
    target.push('|');
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex},
    };

    use super::*;
    use crate::domain::{
        HistoryEntry, MutationAck, PendingBatch, PortFuture, RuntimeStatus, SessionNode,
        SessionSource,
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
        fail_complete: bool,
        results: Vec<(String, String)>,
        pending: Vec<PendingBatch>,
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
        ) -> Result<String, VaultError> {
            if self.fail_begin {
                return Err(VaultError::Storage("read only".into()));
            }
            self.began = true;
            Ok("batch".into())
        }
        fn complete_batch(
            &mut self,
            _: &str,
            results: &BTreeMap<String, ItemResult>,
            _: &str,
            _: i64,
        ) -> Result<(), VaultError> {
            if self.fail_complete {
                return Err(VaultError::Storage("result transaction failed".into()));
            }
            self.results.extend(
                results
                    .iter()
                    .map(|(id, value)| (id.clone(), value.as_storage_value())),
            );
            Ok(())
        }
        fn pending_batches(&self) -> Result<Vec<PendingBatch>, VaultError> {
            Ok(self.pending.clone())
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
    async fn exact_delete_count_is_required_before_write() {
        let calls = Arc::new(Mutex::new(vec![]));
        let gateway = Gateway {
            scans: Arc::new(Mutex::new(VecDeque::from([snapshot(true), snapshot(true)]))),
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

    #[tokio::test]
    async fn rejects_forged_action_nodes_count_and_duplicate_roots() {
        for forge in 0..4 {
            let gateway = Gateway {
                scans: Arc::new(Mutex::new(VecDeque::from([
                    snapshot(false),
                    snapshot(false),
                ]))),
                calls: Arc::new(Mutex::new(vec![])),
            };
            let mut service = VaultService::new(gateway, Store::default());
            service.refresh().await.unwrap();
            let mut preview = service.preview(&BTreeSet::from(["root".into()]), Action::Archive);
            match forge {
                0 => preview.action = Action::Delete,
                1 => {
                    preview.trees[0].node_ids.pop();
                }
                2 => preview.impacted_count += 1,
                3 => {
                    preview.trees.push(preview.trees[0].clone());
                    preview.impacted_count *= 2;
                }
                _ => unreachable!(),
            }
            assert_eq!(
                service
                    .execute(&preview, Some(preview.impacted_count))
                    .await,
                Err(VaultError::StalePreview)
            );
        }
    }

    #[tokio::test]
    async fn transactional_result_failure_is_reported_as_interrupted() {
        let gateway = Gateway {
            scans: Arc::new(Mutex::new(VecDeque::from([
                snapshot(false),
                snapshot(false),
                snapshot(true),
            ]))),
            calls: Arc::new(Mutex::new(vec![])),
        };
        let mut service = VaultService::new(
            gateway,
            Store {
                fail_complete: true,
                ..Store::default()
            },
        );
        service.refresh().await.unwrap();
        let preview = service.preview(&BTreeSet::from(["root".into()]), Action::Archive);
        let result = service.execute(&preview, None).await.unwrap();
        assert!(!result.is_complete_success());
        assert!(
            result
                .items
                .values()
                .all(|value| *value == ItemResult::Interrupted)
        );
        assert_eq!(result.warnings.len(), 1);
    }

    #[tokio::test]
    async fn incomplete_final_scan_never_confirms_success() {
        let mut incomplete = snapshot(true);
        incomplete.relation_complete = false;
        incomplete.write_capable = false;
        let gateway = Gateway {
            scans: Arc::new(Mutex::new(VecDeque::from([
                snapshot(false),
                snapshot(false),
                incomplete,
            ]))),
            calls: Arc::new(Mutex::new(vec![])),
        };
        let mut service = VaultService::new(gateway, Store::default());
        service.refresh().await.unwrap();
        let preview = service.preview(&BTreeSet::from(["root".into()]), Action::Archive);
        let result = service.execute(&preview, None).await.unwrap();
        assert!(!result.is_complete_success());
        assert!(
            result
                .items
                .values()
                .all(|value| *value == ItemResult::Interrupted)
        );
        assert!(result.warnings[0].contains("incomplete"));
    }

    #[tokio::test]
    async fn recovery_reconciles_every_action_against_current_snapshot() {
        let deleted = ScanSnapshot {
            nodes: vec![],
            write_capable: true,
            relation_complete: true,
            diagnostics: vec![],
        };
        for (action, current) in [
            (Action::Archive, snapshot(true)),
            (Action::Restore, snapshot(false)),
            (Action::Delete, deleted),
        ] {
            let gateway = Gateway {
                scans: Arc::new(Mutex::new(VecDeque::from([current]))),
                calls: Arc::new(Mutex::new(vec![])),
            };
            let pending = PendingBatch {
                batch_id: "old".into(),
                action,
                node_ids: vec!["child".into(), "root".into()],
            };
            let mut service = VaultService::new(
                gateway,
                Store {
                    pending: vec![pending],
                    ..Store::default()
                },
            );
            service.refresh().await.unwrap();
            assert_eq!(service.recover_and_prune().unwrap(), (1, 0));
            assert_eq!(service.store.results.len(), 2);
            assert!(
                service
                    .store
                    .results
                    .iter()
                    .all(|(_, value)| value == "success")
            );
        }
    }
}
