use std::{
    cell::Cell,
    collections::{BTreeMap, BTreeSet},
    io,
    time::Duration,
};

use chrono::{DateTime, Local};
use crossterm::event::{Event, EventStream, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use futures_util::StreamExt;
use ratatui::{
    Frame, Terminal,
    backend::{Backend, CrosstermBackend},
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{
        Block, Borders, Cell as TableCell, Gauge, List, ListItem, Paragraph, Row, Table, Wrap,
    },
};

use crate::{
    application::{ExecutionPhase, ExecutionProgress, VaultService},
    domain::{
        Action, Cutoff, Filter, HistoryEntry, ItemResult, MaintenanceKind, MaintenancePlan,
        OperationPreview, OperationResult, OperationStore, Preferences, ProtectionReason,
        SessionGateway, SessionTree, VaultError,
    },
    i18n::Catalog,
};

const MIN_TERMINAL_WIDTH: u16 = 80;
const MIN_TERMINAL_HEIGHT: u16 = 20;
const EXECUTION_REFRESH_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Screen {
    Scan,
    Filter,
    Select,
    Preview,
    Result,
    History,
    Help,
    Search,
    CustomCutoff,
    Maintenance,
    MaintenancePreview,
}

#[derive(Debug, Default)]
struct ScopeSummary {
    matched: usize,
    eligible: BTreeSet<String>,
    impacted: usize,
    blocked: BTreeMap<String, Vec<ProtectionReason>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProjectTab {
    value: Option<String>,
    count: usize,
    storage_bytes: Option<u64>,
}

struct UiState {
    screen: Screen,
    return_screen: Screen,
    index: usize,
    selected: BTreeSet<String>,
    action: Option<Action>,
    filter: Filter,
    preferences: Preferences,
    catalog: Catalog,
    preview: Option<OperationPreview>,
    preview_offset: usize,
    input: String,
    message: String,
    history: Vec<HistoryEntry>,
    result: Vec<String>,
    result_offset: usize,
    pending_execution: Option<(OperationPreview, Option<usize>)>,
    pending_scan: bool,
    filter_focus: usize,
    maintenance_index: usize,
    maintenance_selected: BTreeSet<String>,
    maintenance_preview: Option<MaintenancePlan>,
    pending_maintenance: Option<MaintenancePlan>,
}

impl UiState {
    fn new(preferences: Preferences) -> Self {
        Self {
            screen: Screen::Scan,
            return_screen: Screen::Scan,
            index: 0,
            selected: BTreeSet::new(),
            action: None,
            filter: Filter {
                cutoff: preferences.cutoff.clone(),
                project: preferences.project.clone(),
                query: None,
                archived: preferences.archived,
            },
            catalog: Catalog::new(preferences.language),
            preferences,
            preview: None,
            preview_offset: 0,
            input: String::new(),
            message: String::new(),
            history: Vec::new(),
            result: Vec::new(),
            result_offset: 0,
            pending_execution: None,
            pending_scan: false,
            filter_focus: 0,
            maintenance_index: 0,
            maintenance_selected: BTreeSet::new(),
            maintenance_preview: None,
            pending_maintenance: None,
        }
    }

    fn sync_preferences(&mut self) {
        self.preferences.language = self.catalog.language();
        self.preferences.cutoff = self.filter.cutoff.clone();
        self.preferences.project = self.filter.project.clone();
        self.preferences.archived = self.filter.archived;
    }

    fn reset_selection(&mut self) {
        self.selected.clear();
        self.index = 0;
        self.preview = None;
        self.preview_offset = 0;
    }

    fn choose_action(&mut self, action: Action) {
        if self.action != Some(action) {
            self.selected.clear();
            self.preview = None;
        }
        self.action = Some(action);
        self.message.clear();
    }
}

pub async fn run<G, S>(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    service: &mut VaultService<G, S>,
    preferences: Preferences,
) -> Result<(), VaultError>
where
    G: SessionGateway,
    S: OperationStore,
{
    let mut state = UiState::new(preferences);
    execute_scan(terminal, &mut state, service, true).await?;
    service.recover_and_prune()?;
    let mut events = EventStream::new();
    loop {
        terminal
            .draw(|frame| draw(frame, &state, service))
            .map_err(|error| VaultError::Unavailable(error.to_string()))?;
        let event = events
            .next()
            .await
            .ok_or_else(|| VaultError::Unavailable("terminal event stream closed".into()))?
            .map_err(|error| VaultError::Unavailable(error.to_string()))?;
        let Event::Key(key) = event else {
            continue;
        };
        if key.kind != KeyEventKind::Press {
            continue;
        }
        let area = terminal
            .size()
            .map_err(|error| VaultError::Unavailable(error.to_string()))?;
        if !event_allowed(area.width, area.height, key) {
            continue;
        }
        if handle_key(key, area.height, &mut state, service).await? {
            break;
        }
        if state.pending_scan {
            state.pending_scan = false;
            execute_scan(terminal, &mut state, service, false).await?;
            events = EventStream::new();
        }
        if let Some((preview, confirmation)) = state.pending_execution.take() {
            execute_preview(terminal, &mut state, service, preview, confirmation).await?;
            // Drop the old reader together with any keys buffered while execution owned the UI.
            // Polling `next()` once and abandoning the pending future can leave EventStream's
            // readiness state disarmed, after which the result screen no longer receives keys.
            events = EventStream::new();
        }
        if let Some(plan) = state.pending_maintenance.take() {
            execute_maintenance(terminal, &mut state, service, plan).await?;
            events = EventStream::new();
        }
    }
    state.sync_preferences();
    service.save_preferences(&state.preferences)?;
    Ok(())
}

async fn handle_key<G, S>(
    key: KeyEvent,
    terminal_height: u16,
    state: &mut UiState,
    service: &mut VaultService<G, S>,
) -> Result<bool, VaultError>
where
    G: SessionGateway,
    S: OperationStore,
{
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
        return Ok(true);
    }
    if matches!(state.screen, Screen::Search | Screen::CustomCutoff) {
        return handle_text_input(key, state, service).await;
    }
    if key.code == KeyCode::Char('l') {
        let language = match state.catalog.language() {
            crate::domain::Language::En => crate::domain::Language::ZhCn,
            crate::domain::Language::ZhCn => crate::domain::Language::En,
        };
        state.catalog = Catalog::new(language);
        persist_preferences(state, service);
        return Ok(false);
    }
    if key.code == KeyCode::Char('?') {
        if state.screen != Screen::Help {
            state.return_screen = state.screen;
            state.screen = Screen::Help;
        }
        return Ok(false);
    }
    if key.code == KeyCode::Char('q') {
        return Ok(true);
    }
    if key.code == KeyCode::Esc {
        go_back(state);
        return Ok(false);
    }
    if key.code == KeyCode::Char('h') && !matches!(state.screen, Screen::History | Screen::Help) {
        state.return_screen = state.screen;
        match service.history(50) {
            Ok(history) => {
                state.history = history;
                state.screen = Screen::History;
            }
            Err(error) => state.message = error.to_string(),
        }
        return Ok(false);
    }

    match state.screen {
        Screen::Scan => handle_scan(key, state, service).await,
        Screen::Filter => handle_filter(key, state, service).await,
        Screen::Select => handle_select(key, state, service),
        Screen::Preview => handle_preview(key, state),
        Screen::Maintenance => handle_maintenance(key, state, service),
        Screen::MaintenancePreview => handle_maintenance_preview(key, state),
        Screen::Result => {
            let max_start = state
                .result
                .len()
                .saturating_sub(result_view_height(terminal_height));
            state.result_offset = state.result_offset.min(max_start);
            match key.code {
                KeyCode::Up => {
                    state.result_offset = state.result_offset.saturating_sub(1);
                }
                KeyCode::Down => {
                    state.result_offset = state.result_offset.saturating_add(1).min(max_start);
                }
                KeyCode::Enter => {
                    state.screen = Screen::Scan;
                    state.action = None;
                    state.preview = None;
                    state.result_offset = 0;
                }
                _ => {}
            }
        }
        Screen::History | Screen::Help => {
            if key.code == KeyCode::Enter {
                state.screen = state.return_screen;
            }
        }
        Screen::Search | Screen::CustomCutoff => unreachable!(),
    }
    Ok(false)
}

fn go_back(state: &mut UiState) {
    state.input.clear();
    match state.screen {
        Screen::Scan => {}
        Screen::Filter => state.screen = Screen::Scan,
        Screen::Select => state.screen = Screen::Filter,
        Screen::Preview => {
            state.preview = None;
            state.preview_offset = 0;
            state.screen = Screen::Select;
        }
        Screen::Result => {
            state.preview = None;
            state.screen = Screen::Select;
        }
        Screen::Maintenance => {
            state.maintenance_selected.clear();
            state.maintenance_preview = None;
            state.screen = Screen::Scan;
        }
        Screen::MaintenancePreview => {
            state.maintenance_preview = None;
            state.screen = Screen::Maintenance;
        }
        Screen::History | Screen::Help => state.screen = state.return_screen,
        Screen::Search | Screen::CustomCutoff => state.screen = Screen::Filter,
    }
}

async fn handle_scan<G, S>(key: KeyEvent, state: &mut UiState, service: &mut VaultService<G, S>)
where
    G: SessionGateway,
    S: OperationStore,
{
    match key.code {
        KeyCode::Enter => state.screen = Screen::Filter,
        KeyCode::Char('r') => state.pending_scan = true,
        KeyCode::Char('g') if !service.maintenance_candidates().is_empty() => {
            state.maintenance_index = 0;
            state.maintenance_selected.clear();
            state.maintenance_preview = None;
            state.message.clear();
            state.screen = Screen::Maintenance;
        }
        _ => {}
    }
}

async fn handle_filter<G, S>(key: KeyEvent, state: &mut UiState, service: &mut VaultService<G, S>)
where
    G: SessionGateway,
    S: OperationStore,
{
    match key.code {
        KeyCode::Enter => {
            state.index = 0;
            state.screen = Screen::Select;
        }
        KeyCode::Char('t') => {
            state.filter.cutoff = match state.filter.cutoff {
                Cutoff::All => Cutoff::RollingDays(1),
                Cutoff::RollingDays(1) => Cutoff::RollingDays(7),
                Cutoff::RollingDays(7) => Cutoff::RollingDays(30),
                _ => Cutoff::All,
            };
            state.reset_selection();
            persist_preferences(state, service);
        }
        KeyCode::Char('c') => {
            state.input.clear();
            state.screen = Screen::CustomCutoff;
        }
        KeyCode::Char('/') => {
            state.input = state.filter.query.clone().unwrap_or_default();
            state.screen = Screen::Search;
        }
        KeyCode::Char('v') => {
            state.filter.archived = match state.filter.archived {
                None => Some(false),
                Some(false) => Some(true),
                Some(true) => None,
            };
            state.reset_selection();
            persist_preferences(state, service);
        }
        KeyCode::Up => state.filter_focus = state.filter_focus.saturating_sub(1),
        KeyCode::Down => state.filter_focus = (state.filter_focus + 1).min(2),
        KeyCode::Left => cycle_filter_control(state, service, false),
        KeyCode::Right => cycle_filter_control(state, service, true),
        KeyCode::Char('r') => state.pending_scan = true,
        _ => {}
    }
}

fn cycle_filter_control<G, S>(state: &mut UiState, service: &mut VaultService<G, S>, forward: bool)
where
    G: SessionGateway,
    S: OperationStore,
{
    match state.filter_focus {
        0 => {
            let values = [
                Cutoff::All,
                Cutoff::RollingDays(1),
                Cutoff::RollingDays(7),
                Cutoff::RollingDays(30),
            ];
            let current = values
                .iter()
                .position(|value| value == &state.filter.cutoff)
                .unwrap_or(0);
            let next = cycle_index(current, values.len(), forward);
            state.filter.cutoff = values[next].clone();
        }
        1 => {
            let values = [None, Some(false), Some(true)];
            let current = values
                .iter()
                .position(|value| value == &state.filter.archived)
                .unwrap_or(0);
            state.filter.archived = values[cycle_index(current, values.len(), forward)];
        }
        2 => {
            let values = project_options(service);
            let current = values
                .iter()
                .position(|value| value == &state.filter.project)
                .unwrap_or(0);
            state.filter.project = values[cycle_index(current, values.len(), forward)].clone();
        }
        _ => return,
    }
    state.reset_selection();
    persist_preferences(state, service);
}

fn cycle_index(current: usize, len: usize, forward: bool) -> usize {
    if len == 0 {
        return 0;
    }
    if forward {
        (current + 1) % len
    } else {
        current.checked_sub(1).unwrap_or(len - 1)
    }
}

fn project_options<G, S>(service: &VaultService<G, S>) -> Vec<Option<String>>
where
    G: SessionGateway,
    S: OperationStore,
{
    let mut projects = service
        .trees()
        .iter()
        .flat_map(|tree| tree.nodes.iter())
        .map(|node| node.project.clone().unwrap_or_else(|| node.cwd.clone()))
        .filter(|value| !value.trim().is_empty())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(Some)
        .collect::<Vec<_>>();
    projects.insert(0, None);
    projects
}

fn project_tabs<G, S>(service: &VaultService<G, S>, filter: &Filter) -> Vec<ProjectTab>
where
    G: SessionGateway,
    S: OperationStore,
{
    let mut base_filter = filter.clone();
    base_filter.project = None;
    project_options(service)
        .into_iter()
        .map(|value| {
            let mut project_filter = base_filter.clone();
            project_filter.project.clone_from(&value);
            let trees = service.filtered(&project_filter);
            ProjectTab {
                value,
                count: trees.len(),
                storage_bytes: trees
                    .iter()
                    .try_fold(0u64, |sum, tree| sum.checked_add(tree.storage_bytes()?)),
            }
        })
        .collect()
}

fn cycle_project_tab<G, S>(state: &mut UiState, service: &mut VaultService<G, S>, forward: bool)
where
    G: SessionGateway,
    S: OperationStore,
{
    let values = project_options(service);
    let current = values
        .iter()
        .position(|value| value == &state.filter.project)
        .unwrap_or(0);
    state.filter.project = values[cycle_index(current, values.len(), forward)].clone();
    state.reset_selection();
    state.message.clear();
    persist_preferences(state, service);
}

fn handle_maintenance<G, S>(key: KeyEvent, state: &mut UiState, service: &VaultService<G, S>)
where
    G: SessionGateway,
    S: OperationStore,
{
    let candidates = service.maintenance_candidates();
    state.maintenance_index = clamp_index(state.maintenance_index, candidates.len());
    match key.code {
        KeyCode::Up => {
            state.maintenance_index = state.maintenance_index.saturating_sub(1);
        }
        KeyCode::Down => {
            state.maintenance_index =
                clamp_index(state.maintenance_index.saturating_add(1), candidates.len());
        }
        KeyCode::Char(' ') => {
            if let Some(candidate) = candidates.get(state.maintenance_index) {
                toggle_selection(&mut state.maintenance_selected, &candidate.key, true);
            }
        }
        KeyCode::Char('A') => {
            state.maintenance_selected = candidates
                .iter()
                .map(|candidate| candidate.key.clone())
                .collect();
        }
        KeyCode::Char('n') => state.maintenance_selected.clear(),
        KeyCode::Char('1') => toggle_maintenance_kind(
            &mut state.maintenance_selected,
            candidates,
            MaintenanceKind::UnreferencedRollout,
        ),
        KeyCode::Char('2') => toggle_maintenance_kind(
            &mut state.maintenance_selected,
            candidates,
            MaintenanceKind::StaleSpawnEdge,
        ),
        KeyCode::Char('3') => toggle_maintenance_kind(
            &mut state.maintenance_selected,
            candidates,
            MaintenanceKind::MissingRolloutThread,
        ),
        KeyCode::Enter => {
            let plan = service.maintenance_preview(&state.maintenance_selected);
            if plan.candidates.is_empty() {
                state.message = state.catalog.text("maintenance_select_first").into();
            } else {
                state.maintenance_preview = Some(plan);
                state.message.clear();
                state.screen = Screen::MaintenancePreview;
            }
        }
        _ => {}
    }
}

fn toggle_maintenance_kind(
    selected: &mut BTreeSet<String>,
    candidates: &[crate::domain::MaintenanceCandidate],
    kind: MaintenanceKind,
) {
    let keys = candidates
        .iter()
        .filter(|candidate| candidate.kind == kind)
        .map(|candidate| candidate.key.clone())
        .collect::<Vec<_>>();
    let all_selected = !keys.is_empty() && keys.iter().all(|key| selected.contains(key));
    for key in keys {
        if all_selected {
            selected.remove(&key);
        } else {
            selected.insert(key);
        }
    }
}

fn handle_maintenance_preview(key: KeyEvent, state: &mut UiState) {
    if key.code == KeyCode::Enter {
        if let Some(plan) = state.maintenance_preview.clone() {
            state.pending_maintenance = Some(plan);
        }
    }
}

fn handle_select<G, S>(key: KeyEvent, state: &mut UiState, service: &mut VaultService<G, S>)
where
    G: SessionGateway,
    S: OperationStore,
{
    match key.code {
        KeyCode::Tab => {
            cycle_project_tab(state, service, true);
            return;
        }
        KeyCode::BackTab => {
            cycle_project_tab(state, service, false);
            return;
        }
        _ => {}
    }
    let trees = service.filtered(&state.filter);
    state.index = clamp_index(state.index, trees.len());
    if is_select_all_key(key) {
        if let Some(action) = state.action {
            let scope = scope_summary(service, &state.filter, action);
            select_all_eligible(&mut state.selected, &scope);
            state.message = if state.selected.is_empty() {
                state.catalog.text("nothing_eligible").into()
            } else {
                format!(
                    "{} {} · {}={}",
                    state.selected.len(),
                    state.catalog.text("trees_selected"),
                    state.catalog.text("impacted"),
                    scope.impacted
                )
            };
        } else {
            state.message = state.catalog.text("choose_action_first").into();
        }
        return;
    }
    match key.code {
        KeyCode::Up => state.index = state.index.saturating_sub(1),
        KeyCode::Down => state.index = clamp_index(state.index.saturating_add(1), trees.len()),
        KeyCode::Char('a') => state.choose_action(Action::Archive),
        KeyCode::Char('u') => state.choose_action(Action::Restore),
        KeyCode::Char('d') => state.choose_action(Action::Delete),
        KeyCode::Char('n') => {
            state.selected.clear();
            state.message = state.catalog.text("selection_cleared").into();
        }
        KeyCode::Char(' ') => {
            let Some(action) = state.action else {
                state.message = state.catalog.text("choose_action_first").into();
                return;
            };
            let scope = scope_summary(service, &state.filter, action);
            if let Some(tree) = trees.get(state.index) {
                if !scope.eligible.contains(&tree.root_id) {
                    state.message = blocked_message(&state.catalog, tree, action, &scope);
                } else {
                    let selected = toggle_selection(&mut state.selected, &tree.root_id, true);
                    let impacted = service.preview(&state.selected, action).impacted_count;
                    state.message = format!(
                        "{} · {}={} · {}={}",
                        state.catalog.text(if selected {
                            "tree_selected"
                        } else {
                            "tree_unselected"
                        }),
                        state.catalog.text("selected"),
                        state.selected.len(),
                        state.catalog.text("impacted"),
                        impacted
                    );
                }
            }
        }
        KeyCode::Enter => {
            let Some(action) = state.action else {
                state.message = state.catalog.text("choose_action_first").into();
                return;
            };
            let scope = scope_summary(service, &state.filter, action);
            state.selected.retain(|root| scope.eligible.contains(root));
            if state.selected.is_empty() {
                state.message = state.catalog.text("select_one_first").into();
                return;
            }
            let preview = service.preview(&state.selected, action);
            if preview.trees.is_empty() || preview.impacted_count == 0 {
                state.message = state.catalog.text("nothing_eligible").into();
                return;
            }
            state.preview = Some(preview);
            state.preview_offset = 0;
            state.input.clear();
            state.message.clear();
            state.screen = Screen::Preview;
        }
        _ => {}
    }
}

fn handle_preview(key: KeyEvent, state: &mut UiState) {
    let Some(preview) = state.preview.clone() else {
        state.screen = Screen::Select;
        return;
    };
    match key.code {
        KeyCode::Up => {
            state.preview_offset = state.preview_offset.saturating_sub(1);
            return;
        }
        KeyCode::Down => {
            let max = preview.trees.len() + preview.blocked.len() + 4;
            state.preview_offset = state.preview_offset.saturating_add(1).min(max);
            return;
        }
        _ => {}
    }
    if preview.action == Action::Delete {
        match key.code {
            KeyCode::Char(value) if value.is_ascii_digit() => {
                state.input.push(value);
                state.message.clear();
            }
            KeyCode::Backspace => {
                state.input.pop();
                state.message.clear();
            }
            KeyCode::Enter => {
                let confirmation = state.input.parse::<usize>().ok();
                if confirmation == Some(preview.impacted_count) {
                    state.message.clear();
                    state.pending_execution = Some((preview, confirmation));
                } else {
                    state.pending_execution = None;
                    state.message = format!(
                        "{}: {}",
                        state.catalog.text("delete_confirmation_mismatch"),
                        preview.impacted_count
                    );
                }
            }
            _ => {}
        }
    } else if key.code == KeyCode::Enter {
        state.pending_execution = Some((preview, None));
    }
}

async fn execute_scan<B, G, S>(
    terminal: &mut Terminal<B>,
    state: &mut UiState,
    service: &mut VaultService<G, S>,
    initial: bool,
) -> Result<(), VaultError>
where
    B: Backend,
    G: SessionGateway,
    S: OperationStore,
{
    let catalog = &state.catalog;
    let refresh = service.refresh();
    tokio::pin!(refresh);
    let mut ticker = tokio::time::interval(EXECUTION_REFRESH_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut animation_frame = 0usize;
    let mut draw_error = None;
    let result = loop {
        tokio::select! {
            result = &mut refresh => break result.map(|trees| trees.len()),
            _ = ticker.tick() => {
                if draw_error.is_none()
                    && let Err(error) = terminal.draw(|frame| {
                        draw_scan_progress(frame, catalog, animation_frame)
                    })
                {
                    draw_error = Some(error.to_string());
                }
                animation_frame = animation_frame.wrapping_add(1);
            }
        }
    };
    if let Some(error) = draw_error {
        return Err(VaultError::Unavailable(format!(
            "scan progress could not be rendered: {error}"
        )));
    }
    match result {
        Ok(count) => {
            state.reset_selection();
            state.maintenance_selected.clear();
            state.maintenance_preview = None;
            if !initial {
                state.message = format!("{count} {}", state.catalog.text("trees"));
            }
            Ok(())
        }
        Err(error) if initial => Err(error),
        Err(error) => {
            state.message = error.to_string();
            Ok(())
        }
    }
}

async fn execute_maintenance<B, G, S>(
    terminal: &mut Terminal<B>,
    state: &mut UiState,
    service: &mut VaultService<G, S>,
    plan: MaintenancePlan,
) -> Result<(), VaultError>
where
    B: Backend,
    G: SessionGateway,
    S: OperationStore,
{
    state.result.clear();
    state.result_offset = 0;
    let catalog = &state.catalog;
    let progress = Cell::new(ExecutionProgress {
        phase: ExecutionPhase::Validating,
        completed: 0,
        total: plan.candidates.len(),
    });
    let execution = service.cleanup_with_progress(&plan, |value| progress.set(value));
    tokio::pin!(execution);
    let mut ticker = tokio::time::interval(EXECUTION_REFRESH_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut animation_frame = 0usize;
    let mut draw_error = None;
    let execution = loop {
        tokio::select! {
            result = &mut execution => break result,
            _ = ticker.tick() => {
                if draw_error.is_none()
                    && let Err(error) = terminal.draw(|frame| {
                        draw_execution(
                            frame,
                            catalog,
                            Action::Cleanup,
                            progress.get(),
                            animation_frame,
                        )
                    })
                {
                    draw_error = Some(error.to_string());
                }
                animation_frame = animation_frame.wrapping_add(1);
            }
        }
    };
    match execution {
        Ok(result) => {
            append_operation_result(state, result);
            state.maintenance_selected.clear();
            state.maintenance_preview = None;
        }
        Err(error) => state.result.push(error.to_string()),
    }
    state.screen = Screen::Result;
    if let Some(error) = draw_error {
        return Err(VaultError::Unavailable(format!(
            "maintenance progress could not be rendered: {error}"
        )));
    }
    Ok(())
}

async fn execute_preview<B, G, S>(
    terminal: &mut Terminal<B>,
    state: &mut UiState,
    service: &mut VaultService<G, S>,
    preview: OperationPreview,
    confirmation: Option<usize>,
) -> Result<(), VaultError>
where
    B: Backend,
    G: SessionGateway,
    S: OperationStore,
{
    state.result.clear();
    state.result_offset = 0;
    let catalog = &state.catalog;
    let action = preview.action;
    let mut draw_error = None;
    let progress = Cell::new(ExecutionProgress {
        phase: ExecutionPhase::Validating,
        completed: 0,
        total: preview.impacted_count,
    });
    let execution = service.execute_with_progress(&preview, confirmation, |value| {
        progress.set(value);
    });
    tokio::pin!(execution);
    let mut refresh = tokio::time::interval(EXECUTION_REFRESH_INTERVAL);
    refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut animation_frame = 0usize;
    let execution = loop {
        tokio::select! {
            result = &mut execution => break result,
            _ = refresh.tick() => {
                if draw_error.is_none() {
                    if let Err(error) = terminal.draw(|frame| {
                        draw_execution(frame, catalog, action, progress.get(), animation_frame)
                    }) {
                        draw_error = Some(error.to_string());
                    }
                }
                animation_frame = animation_frame.wrapping_add(1);
            }
        }
    };
    match execution {
        Ok(result) => {
            let success = result
                .items
                .values()
                .filter(|value| matches!(value, ItemResult::Success))
                .count();
            let skipped = result
                .items
                .values()
                .filter(|value| matches!(value, ItemResult::Skipped))
                .count();
            let failed = result
                .items
                .values()
                .filter(|value| matches!(value, ItemResult::Failed(_)))
                .count();
            let interrupted = result
                .items
                .values()
                .filter(|value| matches!(value, ItemResult::Interrupted))
                .count();
            state.result.push(format!(
                "{} · {}={} · {}={} · {}={} · {}={}",
                if result.is_complete_success() {
                    state.catalog.text("completed")
                } else {
                    state.catalog.text("partial")
                },
                state.catalog.text("success"),
                success,
                state.catalog.text("skipped"),
                skipped,
                state.catalog.text("failed"),
                failed,
                state.catalog.text("interrupted"),
                interrupted
            ));
            state.result.push(format!(
                "{}: {} · {}: {}",
                state.catalog.text("batch"),
                result.batch_id,
                state.catalog.text("action"),
                action_text(&state.catalog, result.action)
            ));
            state
                .result
                .extend(result.items.into_iter().map(|(id, value)| {
                    let value = match value {
                        ItemResult::Success => state.catalog.text("success").into(),
                        ItemResult::Skipped => state.catalog.text("skipped").into(),
                        ItemResult::Interrupted => state.catalog.text("interrupted").into(),
                        ItemResult::Failed(message) => {
                            format!("{}: {message}", state.catalog.text("failed"))
                        }
                    };
                    format!("{id}: {value}")
                }));
            state.result.extend(
                result
                    .warnings
                    .into_iter()
                    .map(|warning| format!("{}: {warning}", state.catalog.text("warning"))),
            );
            state.selected.clear();
        }
        Err(error) => state.result.push(error.to_string()),
    }
    state.screen = Screen::Result;
    if let Some(error) = draw_error {
        return Err(VaultError::Unavailable(format!(
            "execution progress could not be rendered: {error}"
        )));
    }
    Ok(())
}

fn append_operation_result(state: &mut UiState, result: OperationResult) {
    let success = result
        .items
        .values()
        .filter(|value| matches!(value, ItemResult::Success))
        .count();
    let skipped = result
        .items
        .values()
        .filter(|value| matches!(value, ItemResult::Skipped))
        .count();
    let failed = result
        .items
        .values()
        .filter(|value| matches!(value, ItemResult::Failed(_)))
        .count();
    let interrupted = result
        .items
        .values()
        .filter(|value| matches!(value, ItemResult::Interrupted))
        .count();
    state.result.push(format!(
        "{} · {}={} · {}={} · {}={} · {}={}",
        if result.is_complete_success() {
            state.catalog.text("completed")
        } else {
            state.catalog.text("partial")
        },
        state.catalog.text("success"),
        success,
        state.catalog.text("skipped"),
        skipped,
        state.catalog.text("failed"),
        failed,
        state.catalog.text("interrupted"),
        interrupted
    ));
    state.result.push(format!(
        "{}: {} · {}: {}",
        state.catalog.text("batch"),
        result.batch_id,
        state.catalog.text("action"),
        action_text(&state.catalog, result.action)
    ));
    state
        .result
        .extend(result.items.into_iter().map(|(id, value)| {
            let value = match value {
                ItemResult::Success => state.catalog.text("success").into(),
                ItemResult::Skipped => state.catalog.text("skipped").into(),
                ItemResult::Interrupted => state.catalog.text("interrupted").into(),
                ItemResult::Failed(message) => {
                    format!("{}: {message}", state.catalog.text("failed"))
                }
            };
            format!("{id}: {value}")
        }));
    state.result.extend(
        result
            .warnings
            .into_iter()
            .map(|warning| format!("{}: {warning}", state.catalog.text("warning"))),
    );
}

async fn handle_text_input<G, S>(
    key: KeyEvent,
    state: &mut UiState,
    service: &mut VaultService<G, S>,
) -> Result<bool, VaultError>
where
    G: SessionGateway,
    S: OperationStore,
{
    match key.code {
        KeyCode::Esc => {
            state.input.clear();
            state.screen = Screen::Filter;
        }
        KeyCode::Backspace => {
            state.input.pop();
        }
        KeyCode::Char(value) => state.input.push(value),
        KeyCode::Enter if state.screen == Screen::Search => {
            state.filter.query = if state.input.trim().is_empty() {
                None
            } else {
                Some(state.input.trim().to_owned())
            };
            state.input.clear();
            state.reset_selection();
            state.screen = Screen::Filter;
        }
        KeyCode::Enter => match DateTime::parse_from_rfc3339(state.input.trim()) {
            Ok(value) => {
                state.filter.cutoff = Cutoff::Absolute(value.timestamp());
                state.input.clear();
                state.reset_selection();
                state.screen = Screen::Filter;
                persist_preferences(state, service);
            }
            Err(error) => state.message = format!("invalid RFC3339: {error}"),
        },
        _ => {}
    }
    Ok(false)
}

fn persist_preferences<G, S>(state: &mut UiState, service: &mut VaultService<G, S>)
where
    G: SessionGateway,
    S: OperationStore,
{
    state.sync_preferences();
    if let Err(error) = service.save_preferences(&state.preferences) {
        state.message = error.to_string();
    }
}

fn scope_summary<G, S>(
    service: &VaultService<G, S>,
    filter: &Filter,
    action: Action,
) -> ScopeSummary
where
    G: SessionGateway,
    S: OperationStore,
{
    let trees = service.filtered(filter);
    let roots = trees
        .iter()
        .map(|tree| tree.root_id.clone())
        .collect::<BTreeSet<_>>();
    let preview = service.preview(&roots, action);
    summarize_scope(&trees, preview, action)
}

fn summarize_scope(
    trees: &[&SessionTree],
    preview: OperationPreview,
    action: Action,
) -> ScopeSummary {
    let candidates = trees
        .iter()
        .filter(|tree| action_candidate(tree, action))
        .map(|tree| tree.root_id.as_str())
        .collect::<BTreeSet<_>>();
    let mut summary = ScopeSummary {
        matched: trees.len(),
        blocked: preview.blocked.into_iter().collect(),
        ..ScopeSummary::default()
    };
    for planned in preview.trees {
        if candidates.contains(planned.root_id.as_str()) && !planned.node_ids.is_empty() {
            summary.impacted += planned.node_ids.len();
            summary.eligible.insert(planned.root_id);
        }
    }
    summary
}

fn action_candidate(tree: &SessionTree, action: Action) -> bool {
    match action {
        Action::Archive => !tree.is_fully_archived(),
        Action::Restore | Action::Delete => tree.is_fully_archived(),
        Action::Cleanup => false,
    }
}

fn select_all_eligible(selected: &mut BTreeSet<String>, scope: &ScopeSummary) {
    selected.clone_from(&scope.eligible);
}

fn toggle_selection(selected: &mut BTreeSet<String>, root: &str, eligible: bool) -> bool {
    if !eligible {
        return false;
    }
    if selected.remove(root) {
        false
    } else {
        selected.insert(root.to_owned());
        true
    }
}

fn is_select_all_key(key: KeyEvent) -> bool {
    key.code == KeyCode::Char('A')
        || (key.code == KeyCode::Char('a') && key.modifiers.contains(KeyModifiers::SHIFT))
}

fn selection_marker(selected: &BTreeSet<String>, root: &str) -> &'static str {
    if selected.contains(root) {
        "[x]"
    } else {
        "[ ]"
    }
}

fn blocked_message(
    catalog: &Catalog,
    tree: &SessionTree,
    action: Action,
    scope: &ScopeSummary,
) -> String {
    let reasons = scope
        .blocked
        .get(&tree.root_id)
        .cloned()
        .unwrap_or_else(|| tree.protection.clone());
    if !reasons.is_empty() {
        return reasons
            .iter()
            .map(|reason| protection_text(catalog, reason))
            .collect::<Vec<_>>()
            .join(", ");
    }
    match action {
        Action::Archive => catalog.text("reason_already_archived").into(),
        Action::Restore => catalog.text("reason_restore_requires_archived").into(),
        Action::Delete => catalog.text("reason_delete_requires_archived").into(),
        Action::Cleanup => catalog.text("maintenance_title").into(),
    }
}

fn row_blocked_message(
    catalog: &Catalog,
    tree: &SessionTree,
    action: Action,
    scope: &ScopeSummary,
) -> String {
    let reasons = scope
        .blocked
        .get(&tree.root_id)
        .cloned()
        .unwrap_or_else(|| tree.protection.clone());
    let specific = specific_protection_message(catalog, &reasons);
    if !specific.is_empty() {
        return specific;
    }
    if !reasons.is_empty() {
        return String::new();
    }
    match action {
        Action::Archive => catalog.text("reason_already_archived").into(),
        Action::Restore => catalog.text("reason_restore_requires_archived").into(),
        Action::Delete => catalog.text("reason_delete_requires_archived").into(),
        Action::Cleanup => catalog.text("maintenance_title").into(),
    }
}

fn specific_protection_message(catalog: &Catalog, reasons: &[ProtectionReason]) -> String {
    reasons
        .iter()
        .filter(|reason| {
            !matches!(
                reason,
                ProtectionReason::PinnedUnknown
                    | ProtectionReason::IncompleteRelations
                    | ProtectionReason::WriteCapabilityMissing
                    | ProtectionReason::StorageUnavailable
            )
        })
        .map(|reason| protection_text(catalog, reason))
        .collect::<Vec<_>>()
        .join(", ")
}

fn draw<G, S>(frame: &mut Frame, state: &UiState, service: &VaultService<G, S>)
where
    G: SessionGateway,
    S: OperationStore,
{
    if terminal_too_small(frame.area()) {
        draw_terminal_too_small(frame, state);
        return;
    }
    let areas = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(5),
            Constraint::Min(8),
            Constraint::Length(5),
        ])
        .split(frame.area());
    let scope = state
        .action
        .map(|action| scope_summary(service, &state.filter, action));
    draw_header(frame, areas[0], state, service, scope.as_ref());
    match state.screen {
        Screen::Scan => draw_scan(frame, areas[1], state, service),
        Screen::Filter => draw_filter(frame, areas[1], state, service),
        Screen::Select => draw_select(frame, areas[1], state, service, scope.as_ref()),
        Screen::Preview => draw_preview(frame, areas[1], state, service),
        Screen::Maintenance => draw_maintenance(frame, areas[1], state, service),
        Screen::MaintenancePreview => draw_maintenance_preview(frame, areas[1], state),
        Screen::Result => draw_result(frame, areas[1], state),
        Screen::History => draw_history(frame, areas[1], state),
        Screen::Help => draw_help(frame, areas[1], state),
        Screen::Search | Screen::CustomCutoff => draw_input(frame, areas[1], state),
    }
    draw_footer(frame, areas[2], state, service);
}

fn draw_header<G, S>(
    frame: &mut Frame,
    area: Rect,
    state: &UiState,
    service: &VaultService<G, S>,
    scope: Option<&ScopeSummary>,
) where
    G: SessionGateway,
    S: OperationStore,
{
    let step = screen_step(state.screen, state.return_screen);
    let labels = [
        "step_scan",
        "step_filter",
        "step_select",
        "step_preview",
        "step_result",
    ];
    let mut step_line = vec![Span::styled(
        format!("{}  ", state.catalog.text("title")),
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    )];
    for (index, key) in labels.iter().enumerate() {
        let style = if index == step {
            Style::default()
                .fg(Color::Black)
                .bg(Color::Cyan)
                .add_modifier(Modifier::BOLD)
        } else if index < step {
            Style::default().fg(Color::Green)
        } else {
            Style::default().fg(Color::DarkGray)
        };
        step_line.push(Span::styled(
            format!("{} {}", index + 1, state.catalog.text(key)),
            style,
        ));
        if index + 1 < labels.len() {
            step_line.push(Span::raw("  ›  "));
        }
    }
    let matched = service.filtered(&state.filter).len();
    let (eligible, blocked, impacted) = scope.map_or((0, 0, 0), |scope| {
        let selected_impacted = state
            .action
            .map(|action| service.preview(&state.selected, action).impacted_count)
            .unwrap_or_default();
        (
            scope.eligible.len(),
            scope.matched.saturating_sub(scope.eligible.len()),
            selected_impacted,
        )
    });
    let action = state
        .action
        .map(|value| action_text(&state.catalog, value))
        .unwrap_or(state.catalog.text("not_chosen"));
    let scope_stats = format!(
        "{}={}  {}={}  {}={}  {}={}",
        state.catalog.text("action"),
        action,
        state.catalog.text("matched"),
        matched,
        state.catalog.text("eligible"),
        eligible,
        state.catalog.text("blocked"),
        blocked
    );
    let selection_stats = format!(
        "{}={}  {}={}",
        state.catalog.text("selected"),
        state.selected.len(),
        state.catalog.text("impacted"),
        impacted
    );
    frame.render_widget(
        Paragraph::new(vec![
            Line::from(step_line),
            Line::from(scope_stats),
            Line::from(selection_stats),
        ])
        .block(Block::default().borders(Borders::ALL)),
        area,
    );
}

fn draw_scan<G, S>(frame: &mut Frame, area: Rect, state: &UiState, service: &VaultService<G, S>)
where
    G: SessionGateway,
    S: OperationStore,
{
    let protected = service
        .trees()
        .iter()
        .filter(|tree| !tree.protection.is_empty())
        .count();
    let archived = service
        .trees()
        .iter()
        .filter(|tree| tree.is_fully_archived())
        .count();
    let maintenance_count = service.maintenance_candidates().len();
    let maintenance_bytes = service
        .maintenance_candidates()
        .iter()
        .map(|candidate| candidate.bytes)
        .sum::<u64>();
    let mut lines = vec![
        Line::from(Span::styled(
            state.catalog.text("scan_ready"),
            Style::default().add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
        Line::from(format!(
            "{}: {}",
            state.catalog.text("target"),
            service.target_summary().unwrap_or("?")
        )),
        Line::from(format!(
            "{}: {}    {}: {}    {}: {}",
            state.catalog.text("trees"),
            service.trees().len(),
            state.catalog.text("archived"),
            archived,
            state.catalog.text("protected"),
            protected
        )),
        Line::from(format!(
            "{}: {} · {}",
            state.catalog.text("maintenance_candidates"),
            maintenance_count,
            human_bytes(maintenance_bytes)
        )),
    ];
    if maintenance_count > 0 {
        lines.push(Line::from(Span::styled(
            state.catalog.text("maintenance_open_hint"),
            Style::default().fg(Color::Cyan),
        )));
    }
    if !service.diagnostics().is_empty() {
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            state.catalog.text("readonly_banner"),
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )));
        lines.extend(
            service
                .diagnostics()
                .iter()
                .map(|diagnostic| Line::from(format!("• {diagnostic}"))),
        );
    }
    frame.render_widget(
        Paragraph::new(lines)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(state.catalog.text("step_scan")),
            )
            .wrap(Wrap { trim: false }),
        area,
    );
}

fn draw_filter<G, S>(frame: &mut Frame, area: Rect, state: &UiState, service: &VaultService<G, S>)
where
    G: SessionGateway,
    S: OperationStore,
{
    let cutoff_choices = [
        (
            state.catalog.text("all"),
            matches!(state.filter.cutoff, Cutoff::All),
        ),
        (
            "> 1d",
            matches!(state.filter.cutoff, Cutoff::RollingDays(1)),
        ),
        (
            "> 7d",
            matches!(state.filter.cutoff, Cutoff::RollingDays(7)),
        ),
        (
            "> 30d",
            matches!(state.filter.cutoff, Cutoff::RollingDays(30)),
        ),
    ];
    let view_choices = [
        (state.catalog.text("all"), state.filter.archived.is_none()),
        (
            state.catalog.text("active"),
            state.filter.archived == Some(false),
        ),
        (
            state.catalog.text("archived"),
            state.filter.archived == Some(true),
        ),
    ];
    let mut lines = vec![
        Line::from(Span::styled(
            state.catalog.text("filter_intro"),
            Style::default().add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
        filter_choice_line(
            state.catalog.text("cutoff"),
            &cutoff_choices,
            state.filter_focus == 0,
        ),
        filter_choice_line(
            state.catalog.text("view"),
            &view_choices,
            state.filter_focus == 1,
        ),
        filter_value_line(
            state.catalog.text("project"),
            state
                .filter
                .project
                .as_deref()
                .unwrap_or(state.catalog.text("all")),
            state.filter_focus == 2,
        ),
        Line::from(format!(
            "  {}: {}",
            state.catalog.text("query"),
            state
                .filter
                .query
                .as_deref()
                .unwrap_or(state.catalog.text("none"))
        )),
        Line::from(""),
        Line::from(Span::styled(
            format!(
                "  {}  {}  ",
                state.catalog.text("matched"),
                service.filtered(&state.filter).len()
            ),
            Style::default()
                .fg(Color::Black)
                .bg(Color::Green)
                .add_modifier(Modifier::BOLD),
        )),
    ];
    if matches!(state.filter.cutoff, Cutoff::Absolute(_)) {
        lines.insert(
            3,
            Line::from(format!(
                "  {}: {}",
                state.catalog.text("custom_cutoff"),
                cutoff_text(&state.filter.cutoff)
            )),
        );
    }
    frame.render_widget(
        Paragraph::new(lines)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(state.catalog.text("step_filter")),
            )
            .wrap(Wrap { trim: false }),
        area,
    );
}

fn filter_choice_line<'a>(label: &'a str, choices: &[(&'a str, bool)], focused: bool) -> Line<'a> {
    let mut spans = vec![Span::styled(
        if focused {
            format!("› {label}: ")
        } else {
            format!("  {label}: ")
        },
        if focused {
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default()
        },
    )];
    for (value, active) in choices {
        spans.push(Span::styled(
            format!(" {value} "),
            if *active {
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Cyan)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(Color::DarkGray)
            },
        ));
        spans.push(Span::raw(" "));
    }
    Line::from(spans)
}

fn filter_value_line<'a>(label: &'a str, value: &'a str, focused: bool) -> Line<'a> {
    Line::from(vec![
        Span::styled(
            if focused {
                format!("› {label}: ")
            } else {
                format!("  {label}: ")
            },
            if focused {
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            },
        ),
        Span::styled(
            format!(" {value} "),
            Style::default().fg(Color::Black).bg(Color::Cyan),
        ),
    ])
}

fn project_tab_label(catalog: &Catalog, value: Option<&str>) -> String {
    let Some(value) = value else {
        return catalog.text("all").to_owned();
    };
    let trimmed = value.trim_end_matches('/');
    trimmed
        .rsplit('/')
        .next()
        .filter(|label| !label.is_empty())
        .unwrap_or(value)
        .to_owned()
}

fn format_storage(bytes: Option<u64>) -> String {
    bytes.map(human_bytes).unwrap_or_else(|| "?".to_owned())
}

fn storage_background(bytes: Option<u64>) -> Color {
    match bytes {
        None => Color::Rgb(44, 44, 50),
        Some(bytes) if bytes >= 10 * 1024 * 1024 => Color::Rgb(89, 34, 39),
        Some(bytes) if bytes >= 1024 * 1024 => Color::Rgb(77, 59, 27),
        Some(_) => Color::Rgb(27, 58, 48),
    }
}

fn project_tab_line(
    catalog: &Catalog,
    tabs: &[ProjectTab],
    active: usize,
    max_width: usize,
) -> Line<'static> {
    let labels = tabs
        .iter()
        .map(|tab| {
            format!(
                " {} ({}, {}) ",
                fit_to_width(&project_tab_label(catalog, tab.value.as_deref()), 16),
                tab.count,
                format_storage(tab.storage_bytes)
            )
        })
        .collect::<Vec<_>>();
    if labels.is_empty() {
        return Line::default();
    }
    let active = active.min(labels.len() - 1);
    let range_width = |start: usize, end: usize| {
        labels[start..end]
            .iter()
            .map(|label| display_width(label))
            .sum::<usize>()
            + end.saturating_sub(start + 1)
    };
    let mut start = 0;
    while start < active && range_width(start, active + 1) > max_width {
        start += 1;
    }
    let mut end = active + 1;
    while end < labels.len() && range_width(start, end + 1) <= max_width {
        end += 1;
    }
    let mut spans = Vec::new();
    if start > 0 {
        spans.push(Span::styled("…", Style::default().fg(Color::DarkGray)));
        spans.push(Span::raw(" "));
    }
    for (offset, label) in labels[start..end].iter().enumerate() {
        let index = start + offset;
        if offset > 0 {
            spans.push(Span::styled("│", Style::default().fg(Color::DarkGray)));
        }
        spans.push(Span::styled(
            label.clone(),
            if index == active {
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Cyan)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(Color::Gray)
            },
        ));
    }
    if end < labels.len() {
        spans.push(Span::styled(" …", Style::default().fg(Color::DarkGray)));
    }
    Line::from(spans)
}

fn draw_project_tabs<G, S>(
    frame: &mut Frame,
    area: Rect,
    state: &UiState,
    service: &VaultService<G, S>,
) where
    G: SessionGateway,
    S: OperationStore,
{
    let tabs = project_tabs(service, &state.filter);
    let active = tabs
        .iter()
        .position(|tab| tab.value == state.filter.project)
        .unwrap_or(0);
    let line = project_tab_line(
        &state.catalog,
        &tabs,
        active,
        area.width.saturating_sub(2) as usize,
    );
    frame.render_widget(
        Paragraph::new(line).block(
            Block::default()
                .borders(Borders::ALL)
                .title(state.catalog.text("project")),
        ),
        area,
    );
}

fn draw_select<G, S>(
    frame: &mut Frame,
    area: Rect,
    state: &UiState,
    service: &VaultService<G, S>,
    scope: Option<&ScopeSummary>,
) where
    G: SessionGateway,
    S: OperationStore,
{
    let areas = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(3)])
        .split(area);
    draw_project_tabs(frame, areas[0], state, service);
    let list_area = areas[1];
    let trees = service.filtered(&state.filter);
    if trees.is_empty() {
        frame.render_widget(
            Paragraph::new(state.catalog.text("empty")).block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(state.catalog.text("step_select")),
            ),
            list_area,
        );
        return;
    }
    let visible = list_area.height.saturating_sub(3) as usize;
    let index = clamp_index(state.index, trees.len());
    let start = window_start(index, trees.len(), visible);
    let width = list_area.width.saturating_sub(2) as usize;
    let wide = width >= 125;
    let columns = if wide {
        vec![
            Constraint::Length(4),
            Constraint::Min(16),
            Constraint::Length(9),
            Constraint::Length(9),
            Constraint::Min(12),
            Constraint::Length(18),
            Constraint::Length(16),
            Constraint::Length(5),
            Constraint::Length(9),
            Constraint::Min(12),
        ]
    } else {
        vec![
            Constraint::Length(4),
            Constraint::Min(12),
            Constraint::Length(9),
            Constraint::Length(5),
            Constraint::Length(5),
            Constraint::Length(8),
            Constraint::Min(10),
        ]
    };
    let headers = if wide {
        vec![
            "",
            "column_title",
            "storage",
            "column_id",
            "project",
            "column_model",
            "column_updated",
            "nodes",
            "column_status",
            "column_reason",
        ]
    } else {
        vec![
            "",
            "column_title",
            "storage",
            "column_date",
            "nodes",
            "column_status",
            "column_reason",
        ]
    };
    let header = Row::new(headers.into_iter().map(|key| {
        if key.is_empty() {
            TableCell::from("")
        } else {
            TableCell::from(state.catalog.text(key))
        }
    }))
    .style(
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    );
    let rows = trees
        .iter()
        .enumerate()
        .skip(start)
        .take(visible)
        .map(|(position, tree)| {
            let selected = selection_marker(&state.selected, &tree.root_id);
            let marker = if position == index { "›" } else { " " };
            let eligible = scope.is_some_and(|value| value.eligible.contains(&tree.root_id));
            let reason = match (state.action, scope) {
                (Some(action), Some(scope)) if !eligible => {
                    row_blocked_message(&state.catalog, tree, action, scope)
                }
                _ if !tree.protection.is_empty() => {
                    specific_protection_message(&state.catalog, &tree.protection)
                }
                _ => String::new(),
            };
            let root = tree
                .nodes
                .iter()
                .find(|node| node.id == tree.root_id)
                .or_else(|| tree.nodes.first());
            let title = root.map_or("-", |node| node.title.as_str());
            let location = root.map_or("-", |node| {
                node.project.as_deref().unwrap_or(node.cwd.as_str())
            });
            let runtime = root.map_or_else(
                || "-".to_owned(),
                |node| match (node.provider.as_deref(), node.model.as_deref()) {
                    (Some(provider), Some(model)) => format!("{provider}/{model}"),
                    (Some(provider), None) => provider.to_owned(),
                    (None, Some(model)) => model.to_owned(),
                    (None, None) => "-".to_owned(),
                },
            );
            let status = if tree.is_fully_archived() {
                state.catalog.text("archived")
            } else {
                state.catalog.text("active")
            };
            let last = tree
                .last_activity
                .and_then(|epoch| DateTime::from_timestamp(epoch, 0))
                .map(|value| {
                    value
                        .with_timezone(&Local)
                        .format("%Y-%m-%d %H:%M")
                        .to_string()
                })
                .unwrap_or_else(|| "?".into());
            let last = if !wide {
                tree.last_activity
                    .and_then(|epoch| DateTime::from_timestamp(epoch, 0))
                    .map(|value| value.with_timezone(&Local).format("%m-%d").to_string())
                    .unwrap_or_else(|| "?".into())
            } else {
                last
            };
            let reason = if reason.is_empty() {
                "-".to_owned()
            } else {
                reason
            };
            let storage = tree.storage_bytes();
            let mut cells = vec![
                TableCell::from(format!("{marker}{selected}")),
                TableCell::from(title.to_owned()),
                TableCell::from(format_storage(storage)),
            ];
            if wide {
                cells.extend([
                    TableCell::from(short_id(&tree.root_id)),
                    TableCell::from(location.to_owned()),
                    TableCell::from(runtime),
                ]);
            }
            cells.extend([
                TableCell::from(last),
                TableCell::from(tree.nodes.len().to_string()),
                TableCell::from(status.to_owned()),
                TableCell::from(reason),
            ]);
            let style = if position == index {
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD)
            } else if eligible {
                Style::default().fg(Color::White)
            } else {
                Style::default().fg(Color::Gray)
            };
            Row::new(cells).style(style.bg(storage_background(storage)))
        })
        .collect::<Vec<_>>();
    frame.render_widget(
        Table::new(rows, columns)
            .header(header)
            .column_spacing(1)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(format!(
                        "{} · {} {}/{}",
                        state.catalog.text("step_select"),
                        state.catalog.text("selected"),
                        state.selected.len(),
                        trees.len()
                    ))
                    .title_bottom(state.catalog.text("storage_legend")),
            ),
        list_area,
    );
}

fn draw_maintenance<G, S>(
    frame: &mut Frame,
    area: Rect,
    state: &UiState,
    service: &VaultService<G, S>,
) where
    G: SessionGateway,
    S: OperationStore,
{
    let candidates = service.maintenance_candidates();
    if candidates.is_empty() {
        frame.render_widget(
            Paragraph::new(state.catalog.text("maintenance_empty")).block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(state.catalog.text("maintenance_title")),
            ),
            area,
        );
        return;
    }
    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(4), Constraint::Min(3)])
        .split(area);
    let selected_bytes = candidates
        .iter()
        .filter(|candidate| state.maintenance_selected.contains(&candidate.key))
        .map(|candidate| candidate.bytes)
        .sum::<u64>();
    let counts = [
        MaintenanceKind::UnreferencedRollout,
        MaintenanceKind::StaleSpawnEdge,
        MaintenanceKind::MissingRolloutThread,
    ]
    .map(|kind| {
        candidates
            .iter()
            .filter(|candidate| candidate.kind == kind)
            .count()
    });
    frame.render_widget(
        Paragraph::new(vec![
            Line::from(Span::styled(
                state.catalog.text("maintenance_intro"),
                Style::default().add_modifier(Modifier::BOLD),
            )),
            Line::from(format!(
                "1 {}={}  2 {}={}  3 {}={}  ·  {}={} · {}",
                state.catalog.text("maintenance_rollouts"),
                counts[0],
                state.catalog.text("maintenance_edges"),
                counts[1],
                state.catalog.text("maintenance_threads"),
                counts[2],
                state.catalog.text("selected"),
                state.maintenance_selected.len(),
                human_bytes(selected_bytes)
            )),
        ])
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(state.catalog.text("maintenance_title")),
        ),
        sections[0],
    );
    let visible = sections[1].height.saturating_sub(2) as usize;
    let index = clamp_index(state.maintenance_index, candidates.len());
    let start = window_start(index, candidates.len(), visible);
    let width = sections[1].width.saturating_sub(2) as usize;
    let items = candidates
        .iter()
        .enumerate()
        .skip(start)
        .take(visible)
        .map(|(position, candidate)| {
            let marker = if position == index { "›" } else { " " };
            let selected = selection_marker(&state.maintenance_selected, &candidate.key);
            let project = candidate.project.as_deref().unwrap_or("-");
            let row = format!(
                "{marker}{selected} {} · {} · {} · {}",
                maintenance_kind_text(&state.catalog, candidate.kind),
                human_bytes(candidate.bytes),
                fit_to_width(project, 26),
                candidate.label
            );
            ListItem::new(fit_to_width(&row, width)).style(if position == index {
                Style::default().fg(Color::Black).bg(Color::Cyan)
            } else {
                Style::default()
            })
        })
        .collect::<Vec<_>>();
    frame.render_widget(
        List::new(items).block(Block::default().borders(Borders::ALL)),
        sections[1],
    );
}

fn draw_maintenance_preview(frame: &mut Frame, area: Rect, state: &UiState) {
    let Some(plan) = &state.maintenance_preview else {
        return;
    };
    let height = area.height.saturating_sub(6) as usize;
    let mut lines = vec![
        Line::from(Span::styled(
            format!(
                "{}={} · {}={} · {}",
                state.catalog.text("maintenance_candidates"),
                plan.candidates.len(),
                state.catalog.text("maintenance_space"),
                human_bytes(plan.total_bytes),
                state.catalog.text("maintenance_backup_notice")
            ),
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
    ];
    lines.extend(plan.candidates.iter().take(height).map(|candidate| {
        Line::from(format!(
            "✓ {} · {} · {}",
            maintenance_kind_text(&state.catalog, candidate.kind),
            human_bytes(candidate.bytes),
            candidate.label
        ))
    }));
    if plan.candidates.len() > height {
        lines.push(Line::from(format!("… +{}", plan.candidates.len() - height)));
    }
    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        state.catalog.text("maintenance_confirm"),
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    )));
    frame.render_widget(
        Paragraph::new(lines)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(state.catalog.text("maintenance_preview")),
            )
            .wrap(Wrap { trim: false }),
        area,
    );
}

fn draw_preview<G, S>(frame: &mut Frame, area: Rect, state: &UiState, service: &VaultService<G, S>)
where
    G: SessionGateway,
    S: OperationStore,
{
    let Some(preview) = &state.preview else {
        return;
    };
    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(1), Constraint::Length(3)])
        .split(area);
    let list_area = sections[0];
    let confirmation_area = sections[1];
    let height = list_area.height.saturating_sub(2) as usize;
    let total = 3usize
        .saturating_add(preview.trees.len())
        .saturating_add(preview.blocked.len());
    let max_start = total.saturating_sub(height);
    let offset = state.preview_offset.min(max_start);
    let titles = service
        .trees()
        .iter()
        .filter_map(|tree| {
            tree.nodes
                .first()
                .map(|node| (tree.root_id.as_str(), node.title.as_str()))
        })
        .collect::<BTreeMap<_, _>>();
    let planned_start = 3;
    let blocked_start = planned_start + preview.trees.len();
    let lines = (offset..offset.saturating_add(height).min(total))
        .map(|index| match index {
            0 => Line::from(Span::styled(
                format!(
                    "{} · {}={} · {}={}",
                    action_text(&state.catalog, preview.action),
                    state.catalog.text("trees"),
                    preview.trees.len(),
                    state.catalog.text("impacted"),
                    preview.impacted_count
                ),
                Style::default().add_modifier(Modifier::BOLD),
            )),
            1 => Line::from(format!(
                "{}: {} · {}: {} · {}: {}",
                state.catalog.text("cutoff"),
                cutoff_text(&state.filter.cutoff),
                state.catalog.text("view"),
                view_text(&state.catalog, state.filter.archived),
                state.catalog.text("query"),
                state.filter.query.as_deref().unwrap_or("*")
            )),
            2 => Line::from(""),
            value if value < blocked_start => {
                let planned = &preview.trees[value - planned_start];
                let title = titles.get(planned.root_id.as_str()).copied().unwrap_or("-");
                Line::from(format!(
                    "✓ {} · {} · {}{}",
                    fit_to_width(title, 36),
                    short_id(&planned.root_id),
                    planned.node_ids.len(),
                    state.catalog.text("nodes")
                ))
            }
            value => {
                let (root, reasons) = &preview.blocked[value - blocked_start];
                Line::from(format!(
                    "✗ {}: {}",
                    short_id(root),
                    reasons
                        .iter()
                        .map(|reason| protection_text(&state.catalog, reason))
                        .collect::<Vec<_>>()
                        .join(", ")
                ))
            }
        })
        .collect::<Vec<_>>();
    frame.render_widget(
        Paragraph::new(lines)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(state.catalog.text("step_preview")),
            )
            .wrap(Wrap { trim: false }),
        list_area,
    );
    let (confirmation_title, confirmation) = if preview.action == Action::Delete {
        (
            state.catalog.text("delete_confirmation_title"),
            Line::from(vec![
                Span::raw(format!(
                    "{} {}: ",
                    state.catalog.text("delete_confirm_prefix"),
                    preview.impacted_count
                )),
                Span::styled(
                    format!("{}_", state.input),
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                ),
            ]),
        )
    } else {
        (
            state.catalog.text("confirmation_title"),
            Line::from(state.catalog.text("confirm")),
        )
    };
    frame.render_widget(
        Paragraph::new(confirmation)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(confirmation_title),
            )
            .style(Style::default().fg(Color::Yellow)),
        confirmation_area,
    );
}

fn draw_scan_progress(frame: &mut Frame, catalog: &Catalog, animation_frame: usize) {
    let area = frame.area();
    let outer = Block::default()
        .borders(Borders::ALL)
        .title(catalog.text("scan_progress_title"));
    let inner = outer.inner(area);
    frame.render_widget(outer, area);
    let sections = Layout::default()
        .direction(Direction::Vertical)
        .margin(2)
        .constraints([
            Constraint::Length(2),
            Constraint::Length(3),
            Constraint::Min(0),
        ])
        .split(inner);
    let spinner = ["|", "/", "-", "\\"][animation_frame % 4];
    frame.render_widget(
        Paragraph::new(format!(
            "{spinner} {}",
            catalog.text("scan_progress_detail")
        )),
        sections[0],
    );
    let pulse = ((animation_frame % 20) + 1) as f64 / 20.0;
    frame.render_widget(
        Gauge::default()
            .block(Block::default().borders(Borders::ALL))
            .gauge_style(Style::default().fg(Color::Cyan))
            .ratio(pulse)
            .label(catalog.text("scan_progress_indeterminate")),
        sections[1],
    );
}

fn draw_execution(
    frame: &mut Frame,
    catalog: &Catalog,
    action: Action,
    progress: ExecutionProgress,
    animation_frame: usize,
) {
    let area = frame.area();
    let outer = Block::default()
        .borders(Borders::ALL)
        .title(catalog.text("execution_title"));
    let inner = outer.inner(area);
    frame.render_widget(outer, area);
    let sections = Layout::default()
        .direction(Direction::Vertical)
        .margin(2)
        .constraints([
            Constraint::Length(2),
            Constraint::Length(3),
            Constraint::Length(2),
            Constraint::Min(0),
        ])
        .split(inner);
    let phase = match progress.phase {
        ExecutionPhase::Validating => catalog.text("execution_validating"),
        ExecutionPhase::Applying => catalog.text("execution_applying"),
        ExecutionPhase::Verifying => catalog.text("execution_verifying"),
    };
    let spinner = ["|", "/", "-", "\\"][animation_frame % 4];
    let completed = progress.completed.min(progress.total);
    let percent = completed
        .saturating_mul(100)
        .checked_div(progress.total)
        .unwrap_or(0) as u16;
    frame.render_widget(
        Paragraph::new(format!(
            "{spinner} {phase} · {}={} · {}={}",
            catalog.text("action"),
            action_text(catalog, action),
            catalog.text("nodes"),
            progress.total
        )),
        sections[0],
    );
    frame.render_widget(
        Gauge::default()
            .block(Block::default().borders(Borders::ALL))
            .gauge_style(
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            )
            .percent(percent)
            .label(format!("{completed}/{} · {percent}%", progress.total)),
        sections[1],
    );
    frame.render_widget(
        Paragraph::new(catalog.text("execution_wait"))
            .style(Style::default().fg(Color::Yellow))
            .wrap(Wrap { trim: false }),
        sections[2],
    );
}

fn draw_history(frame: &mut Frame, area: Rect, state: &UiState) {
    let lines = state
        .history
        .iter()
        .map(|entry| {
            Line::from(format!(
                "{} · {} · {} · {} {} · {}",
                entry.batch_id,
                entry.created_at,
                entry.action,
                entry.planned_count,
                state.catalog.text("nodes"),
                entry.status
            ))
        })
        .collect::<Vec<_>>();
    frame.render_widget(
        Paragraph::new(lines)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(state.catalog.text("history")),
            )
            .wrap(Wrap { trim: false }),
        area,
    );
}

fn draw_result(frame: &mut Frame, area: Rect, state: &UiState) {
    let height = area.height.saturating_sub(2) as usize;
    let max_start = state.result.len().saturating_sub(height);
    let offset = state.result_offset.min(max_start);
    frame.render_widget(
        Paragraph::new(
            state
                .result
                .iter()
                .skip(offset)
                .take(height)
                .map(|value| Line::from(value.as_str()))
                .collect::<Vec<_>>(),
        )
        .block(Block::default().borders(Borders::ALL).title(format!(
            "{} · {}/{}",
            state.catalog.text("step_result"),
            if state.result.is_empty() {
                0
            } else {
                offset + 1
            },
            state.result.len()
        )))
        .wrap(Wrap { trim: false }),
        area,
    );
}

fn draw_help(frame: &mut Frame, area: Rect, state: &UiState) {
    let lines = [
        "help_navigation",
        "help_scan",
        "help_filter",
        "help_select",
        "help_preview",
        "help_maintenance",
        "help_global",
    ]
    .into_iter()
    .map(|key| Line::from(state.catalog.text(key)))
    .collect::<Vec<_>>();
    frame.render_widget(
        Paragraph::new(lines)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(state.catalog.text("help_title")),
            )
            .wrap(Wrap { trim: false }),
        area,
    );
}

fn draw_input(frame: &mut Frame, area: Rect, state: &UiState) {
    let prompt = if state.screen == Screen::Search {
        state.catalog.text("search_prompt")
    } else {
        state.catalog.text("custom_prompt")
    };
    frame.render_widget(
        Paragraph::new(format!("{prompt}\n\n{}_", state.input))
            .block(Block::default().borders(Borders::ALL))
            .wrap(Wrap { trim: false }),
        area,
    );
}

fn draw_footer<G, S>(frame: &mut Frame, area: Rect, state: &UiState, service: &VaultService<G, S>)
where
    G: SessionGateway,
    S: OperationStore,
{
    let help = state.catalog.text(screen_help_key(state.screen));
    let mut lines = Vec::new();
    if !state.message.is_empty() {
        lines.push(Line::from(state.message.as_str()));
    }
    if let Some(diagnostic) = service.diagnostics().first() {
        lines.push(Line::from(Span::styled(
            format!("⚠ {} {diagnostic}", state.catalog.text("readonly_banner")),
            Style::default().fg(Color::Yellow),
        )));
    }
    lines.push(Line::from(help));
    frame.render_widget(
        Paragraph::new(lines)
            .block(Block::default().borders(Borders::ALL))
            .wrap(Wrap { trim: true }),
        area,
    );
}

fn draw_terminal_too_small(frame: &mut Frame, state: &UiState) {
    frame.render_widget(
        Paragraph::new(format!(
            "{}\n{}x{} · {}x{}",
            state.catalog.text("terminal_too_small"),
            frame.area().width,
            frame.area().height,
            MIN_TERMINAL_WIDTH,
            MIN_TERMINAL_HEIGHT
        ))
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(state.catalog.text("title")),
        )
        .wrap(Wrap { trim: false }),
        frame.area(),
    );
}

fn screen_step(screen: Screen, return_screen: Screen) -> usize {
    match screen {
        Screen::Scan => 0,
        Screen::Filter | Screen::Search | Screen::CustomCutoff | Screen::Maintenance => 1,
        Screen::Select => 2,
        Screen::Preview | Screen::MaintenancePreview => 3,
        Screen::Result => 4,
        Screen::History | Screen::Help => screen_step(return_screen, Screen::Scan),
    }
}

fn screen_help_key(screen: Screen) -> &'static str {
    match screen {
        Screen::Scan => "keys_scan",
        Screen::Filter => "keys_filter",
        Screen::Select => "keys_select",
        Screen::Preview => "keys_preview",
        Screen::Maintenance => "keys_maintenance",
        Screen::MaintenancePreview => "keys_maintenance_preview",
        Screen::Result => "keys_result",
        Screen::History => "keys_history",
        Screen::Help => "keys_help",
        Screen::Search | Screen::CustomCutoff => "keys_input",
    }
}

fn terminal_too_small(area: Rect) -> bool {
    area.width < MIN_TERMINAL_WIDTH || area.height < MIN_TERMINAL_HEIGHT
}

fn is_exit_key(key: KeyEvent) -> bool {
    key.code == KeyCode::Char('q')
        || (key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL))
}

fn event_allowed(width: u16, height: u16, key: KeyEvent) -> bool {
    (width >= MIN_TERMINAL_WIDTH && height >= MIN_TERMINAL_HEIGHT) || is_exit_key(key)
}

fn result_view_height(terminal_height: u16) -> usize {
    terminal_height.saturating_sub(5 + 5).saturating_sub(2) as usize
}

fn clamp_index(index: usize, len: usize) -> usize {
    if len == 0 { 0 } else { index.min(len - 1) }
}

fn window_start(index: usize, len: usize, visible: usize) -> usize {
    if len == 0 || visible == 0 {
        return 0;
    }
    let index = clamp_index(index, len);
    index
        .saturating_add(1)
        .saturating_sub(visible)
        .min(len.saturating_sub(visible))
}

fn cutoff_text(cutoff: &Cutoff) -> String {
    match cutoff {
        Cutoff::All => "*".into(),
        Cutoff::RollingDays(days) => format!("{days}d"),
        Cutoff::Absolute(epoch) => DateTime::from_timestamp(*epoch, 0)
            .map(|value| value.with_timezone(&Local).to_rfc3339())
            .unwrap_or_else(|| epoch.to_string()),
    }
}

fn view_text(catalog: &Catalog, archived: Option<bool>) -> &str {
    match archived {
        None => catalog.text("all"),
        Some(false) => catalog.text("active"),
        Some(true) => catalog.text("archived"),
    }
}

fn short_id(value: &str) -> String {
    fit_to_width(value, 8)
}

fn fit_to_width(value: &str, max_width: usize) -> String {
    if display_width(value) <= max_width {
        return value.to_owned();
    }
    if max_width == 0 {
        return String::new();
    }
    let content_width = max_width.saturating_sub(1);
    let mut result = String::new();
    let mut width = 0;
    for character in value.chars() {
        let character_width = display_char_width(character);
        if width + character_width > content_width {
            break;
        }
        result.push(character);
        width += character_width;
    }
    result.push('…');
    result
}

fn display_width(value: &str) -> usize {
    value.chars().map(display_char_width).sum()
}

fn display_char_width(character: char) -> usize {
    match character as u32 {
        0x1100..=0x115f
        | 0x2329..=0x232a
        | 0x2e80..=0xa4cf
        | 0xac00..=0xd7a3
        | 0xf900..=0xfaff
        | 0xfe10..=0xfe19
        | 0xfe30..=0xfe6f
        | 0xff00..=0xff60
        | 0xffe0..=0xffe6
        | 0x1f300..=0x1faff
        | 0x20000..=0x3fffd => 2,
        0x0000..=0x001f | 0x007f..=0x009f => 0,
        _ => 1,
    }
}

pub async fn brief_pause() {
    tokio::time::sleep(Duration::from_millis(30)).await;
}

fn action_text(catalog: &Catalog, action: Action) -> &str {
    catalog.text(action.as_str())
}

fn maintenance_kind_text(catalog: &Catalog, kind: MaintenanceKind) -> &str {
    match kind {
        MaintenanceKind::UnreferencedRollout => catalog.text("maintenance_rollout"),
        MaintenanceKind::StaleSpawnEdge => catalog.text("maintenance_edge"),
        MaintenanceKind::MissingRolloutThread => catalog.text("maintenance_thread"),
    }
}

fn human_bytes(value: u64) -> String {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB"];
    let mut amount = value as f64;
    let mut unit = 0usize;
    while amount >= 1024.0 && unit + 1 < UNITS.len() {
        amount /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{value} {}", UNITS[unit])
    } else {
        format!("{amount:.1} {}", UNITS[unit])
    }
}

fn protection_text(catalog: &Catalog, reason: &ProtectionReason) -> String {
    match reason {
        ProtectionReason::Pinned => catalog.text("reason_pinned").into(),
        ProtectionReason::PinnedUnknown => catalog.text("reason_pin_unknown").into(),
        ProtectionReason::Running => catalog.text("reason_running").into(),
        ProtectionReason::WaitingApproval => catalog.text("reason_waiting_approval").into(),
        ProtectionReason::WaitingInput => catalog.text("reason_waiting_input").into(),
        ProtectionReason::UnsafeStatus(value) => {
            format!("{}: {value}", catalog.text("reason_unsafe_status"))
        }
        ProtectionReason::UnverifiableSource => catalog.text("reason_unverifiable_source").into(),
        ProtectionReason::MissingTimestamp => catalog.text("reason_missing_timestamp").into(),
        ProtectionReason::IncompleteRelations => catalog.text("reason_incomplete_relations").into(),
        ProtectionReason::WriteCapabilityMissing => {
            catalog.text("reason_capability_missing").into()
        }
        ProtectionReason::StorageUnavailable => catalog.text("reason_storage_unavailable").into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{
        MaintenanceCandidate, MutationAck, PendingBatch, PortFuture, RuntimeStatus, ScanSnapshot,
        SessionNode, SessionSource,
    };

    #[derive(Clone)]
    struct Gateway {
        snapshot: ScanSnapshot,
    }

    impl SessionGateway for Gateway {
        fn scan(&mut self) -> PortFuture<'_, ScanSnapshot> {
            let snapshot = self.snapshot.clone();
            Box::pin(async move { Ok(snapshot) })
        }

        fn mutate<'a>(&'a mut self, _: Action, _: &'a str) -> PortFuture<'a, MutationAck> {
            Box::pin(async { unreachable!("selection tests never mutate Codex") })
        }
    }

    #[derive(Clone)]
    struct SlowGateway {
        snapshot: ScanSnapshot,
        delay: Duration,
    }

    impl SessionGateway for SlowGateway {
        fn scan(&mut self) -> PortFuture<'_, ScanSnapshot> {
            let snapshot = self.snapshot.clone();
            let delay = self.delay;
            Box::pin(async move {
                tokio::time::sleep(delay).await;
                Ok(snapshot)
            })
        }

        fn mutate<'a>(&'a mut self, _: Action, _: &'a str) -> PortFuture<'a, MutationAck> {
            Box::pin(async {
                Ok(MutationAck {
                    response_received: true,
                    notification_seen: true,
                })
            })
        }
    }

    #[derive(Default)]
    struct Store;

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
            Ok("batch-test".into())
        }

        fn complete_batch(
            &mut self,
            _: &str,
            _: &BTreeMap<String, ItemResult>,
            _: &str,
            _: i64,
        ) -> Result<(), VaultError> {
            Ok(())
        }

        fn pending_batches(&self) -> Result<Vec<PendingBatch>, VaultError> {
            Ok(Vec::new())
        }

        fn prune(&mut self, _: i64, _: u32) -> Result<usize, VaultError> {
            Ok(0)
        }

        fn history(&self, _: usize) -> Result<Vec<HistoryEntry>, VaultError> {
            Ok(Vec::new())
        }
    }

    fn selectable_snapshot() -> ScanSnapshot {
        ScanSnapshot {
            nodes: vec![
                tree("one", false, vec![]).nodes.remove(0),
                tree("two", false, vec![]).nodes.remove(0),
            ],
            write_capable: true,
            relation_complete: true,
            diagnostics: Vec::new(),
        }
    }

    fn tree(id: &str, archived: bool, protection: Vec<ProtectionReason>) -> SessionTree {
        SessionTree {
            root_id: id.into(),
            nodes: vec![SessionNode {
                id: id.into(),
                title: "中文标题🙂".into(),
                project: Some("项目".into()),
                provider: Some("free".into()),
                model: Some("gpt-test".into()),
                cwd: "/tmp/项目".into(),
                rollout_bytes: Some(512),
                last_activity: Some(1),
                archived,
                pinned: Some(false),
                status: RuntimeStatus::Idle,
                parent_id: None,
                source: SessionSource::Cli,
            }],
            last_activity: Some(1),
            protection,
        }
    }

    #[test]
    fn action_candidates_follow_archive_lifecycle() {
        let active = tree("active", false, vec![]);
        let archived = tree("archived", true, vec![]);
        assert!(action_candidate(&active, Action::Archive));
        assert!(!action_candidate(&active, Action::Restore));
        assert!(!action_candidate(&active, Action::Delete));
        assert!(!action_candidate(&archived, Action::Archive));
        assert!(action_candidate(&archived, Action::Restore));
        assert!(action_candidate(&archived, Action::Delete));
    }

    #[tokio::test]
    async fn project_tabs_count_render_and_cycle_without_hidden_selection() {
        let mut snapshot = selectable_snapshot();
        snapshot.nodes[0].project = Some("/work/alpha".into());
        snapshot.nodes[0].cwd = "/work/alpha".into();
        snapshot.nodes[1].project = Some("/work/beta".into());
        snapshot.nodes[1].cwd = "/work/beta".into();
        let mut service = VaultService::new(Gateway { snapshot }, Store);
        service.refresh().await.unwrap();
        let mut state = UiState::new(Preferences::default());
        state.screen = Screen::Select;
        state.choose_action(Action::Archive);
        state.selected.insert("one".into());
        state.index = 1;

        handle_select(
            KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE),
            &mut state,
            &mut service,
        );

        assert_eq!(state.filter.project.as_deref(), Some("/work/alpha"));
        assert_eq!(state.preferences.project, state.filter.project);
        assert!(state.selected.is_empty());
        assert_eq!(state.index, 0);
        let tabs = project_tabs(&service, &state.filter);
        assert_eq!(
            tabs,
            vec![
                ProjectTab {
                    value: None,
                    count: 2,
                    storage_bytes: Some(1024),
                },
                ProjectTab {
                    value: Some("/work/alpha".into()),
                    count: 1,
                    storage_bytes: Some(512),
                },
                ProjectTab {
                    value: Some("/work/beta".into()),
                    count: 1,
                    storage_bytes: Some(512),
                },
            ]
        );

        let mut terminal = Terminal::new(ratatui::backend::TestBackend::new(120, 30)).unwrap();
        terminal
            .draw(|frame| draw(frame, &state, &service))
            .unwrap();
        let rendered = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>()
            .replace(' ', "");
        assert!(rendered.contains("全部(2,1.0KiB)"));
        assert!(rendered.contains("alpha(1,512B)"));
        assert!(rendered.contains("beta(1,512B)"));

        handle_select(
            KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE),
            &mut state,
            &mut service,
        );
        assert_eq!(state.filter.project.as_deref(), Some("/work/beta"));
        handle_select(
            KeyEvent::new(KeyCode::BackTab, KeyModifiers::SHIFT),
            &mut state,
            &mut service,
        );
        assert_eq!(state.filter.project.as_deref(), Some("/work/alpha"));
    }

    #[tokio::test]
    async fn storage_totals_and_colored_rows_survive_selection_and_resize() {
        let mut snapshot = selectable_snapshot();
        snapshot.nodes[0].project = Some("/work/a".into());
        snapshot.nodes[0].rollout_bytes = Some(12 * 1024 * 1024);
        snapshot.nodes[1].project = Some("/work/b".into());
        snapshot.nodes[1].rollout_bytes = Some(2 * 1024 * 1024);
        let mut service = VaultService::new(Gateway { snapshot }, Store);
        service.refresh().await.unwrap();
        let mut state = UiState::new(Preferences::default());
        state.screen = Screen::Select;
        state.index = 0;
        let tabs = project_tabs(&service, &state.filter);
        assert_eq!(
            tabs.iter().map(|tab| tab.storage_bytes).collect::<Vec<_>>(),
            vec![
                Some(14 * 1024 * 1024),
                Some(12 * 1024 * 1024),
                Some(2 * 1024 * 1024)
            ]
        );
        for width in [100, 160] {
            let mut terminal =
                Terminal::new(ratatui::backend::TestBackend::new(width, 25)).unwrap();
            terminal
                .draw(|frame| draw(frame, &state, &service))
                .unwrap();
            let buffer = terminal.backend().buffer();
            let lines = (0..25)
                .map(|y| {
                    (0..width)
                        .map(|x| buffer[(x, y)].symbol())
                        .collect::<String>()
                })
                .collect::<Vec<_>>();
            assert!(lines.iter().any(|line| line.contains("14.0 MiB")));
            assert!(lines.iter().any(|line| line.contains("12.0 MiB")));
            assert!(lines.iter().any(|line| line.contains("10 MiB")));
            let first = lines
                .iter()
                .position(|line| line.contains("12.0 MiB") && line.contains("›[ ]"))
                .unwrap_or_else(|| panic!("missing first row in {width}: {}", lines.join("\n")));
            let second = lines
                .iter()
                .position(|line| {
                    line.contains("2.0 MiB") && line.contains("[ ]") && !line.contains("›[ ]")
                })
                .unwrap_or_else(|| panic!("missing second row in {width}: {}", lines.join("\n")));
            assert_eq!(
                buffer[(6, first as u16)].bg,
                storage_background(Some(12 * 1024 * 1024))
            );
            assert_eq!(
                buffer[(6, second as u16)].bg,
                storage_background(Some(2 * 1024 * 1024))
            );
            assert!(
                lines
                    .iter()
                    .any(|line| line.replace(' ', "").contains("占用"))
            );
        }
        assert_eq!(format_storage(None), "?");
        assert_eq!(storage_background(None), Color::Rgb(44, 44, 50));
        assert_eq!(storage_background(Some(0)), Color::Rgb(27, 58, 48));
        let mut unknown = selectable_snapshot();
        unknown.nodes[1].rollout_bytes = None;
        let mut service = VaultService::new(Gateway { snapshot: unknown }, Store);
        service.refresh().await.unwrap();
        assert_eq!(project_tabs(&service, &state.filter)[0].storage_bytes, None);
        assert_eq!(service.trees()[1].storage_bytes(), None);
    }

    #[test]
    fn protected_tree_is_not_actionable() {
        let eligible = tree("eligible", false, vec![]);
        let protected = tree("protected", false, vec![ProtectionReason::Pinned]);
        let outside_filter = tree("outside", false, vec![]);
        let preview = OperationPreview {
            action: Action::Archive,
            trees: vec![crate::domain::PlannedTree {
                root_id: eligible.root_id.clone(),
                signature: eligible.signature(),
                node_ids: vec![eligible.root_id.clone()],
            }],
            impacted_count: 1,
            blocked: vec![(protected.root_id.clone(), vec![ProtectionReason::Pinned])],
        };
        let filtered = [&eligible, &protected];
        let summary = summarize_scope(&filtered, preview, Action::Archive);
        assert_eq!(summary.eligible, BTreeSet::from(["eligible".into()]));
        assert!(!summary.eligible.contains(&protected.root_id));
        assert!(!summary.eligible.contains(&outside_filter.root_id));
        assert_eq!(summary.impacted, 1);
        assert_eq!(summary.matched, 2);
        let mut selected = BTreeSet::from(["stale-selection".into()]);
        select_all_eligible(&mut selected, &summary);
        assert_eq!(selected, BTreeSet::from(["eligible".into()]));
        assert!(!toggle_selection(&mut selected, "protected", false));
        assert!(!selected.contains("protected"));
        assert!(!toggle_selection(&mut selected, "eligible", true));
        assert!(selected.is_empty());
    }

    #[test]
    fn changing_action_and_filter_clear_selection() {
        let mut state = UiState::new(Preferences::default());
        state.selected.insert("one".into());
        state.choose_action(Action::Archive);
        assert!(state.selected.is_empty());
        state.selected.insert("one".into());
        state.choose_action(Action::Archive);
        assert_eq!(state.selected.len(), 1);
        state.filter.cutoff = Cutoff::RollingDays(1);
        state.reset_selection();
        assert!(state.selected.is_empty());
    }

    #[test]
    fn index_and_window_are_clamped_after_filter_shrinks() {
        assert_eq!(clamp_index(4_000, 2), 1);
        assert_eq!(clamp_index(4_000, 0), 0);
        assert_eq!(window_start(4_000, 2, 10), 0);
        assert_eq!(window_start(99, 100, 10), 90);
    }

    #[test]
    fn unicode_truncation_is_safe_and_width_bounded() {
        let value = fit_to_width("会话🙂abc", 6);
        assert_eq!(value, "会话…");
        assert!(display_width(&value) <= 6);
        assert_eq!(fit_to_width("🙂", 1), "…");
    }

    #[test]
    fn narrow_terminal_has_an_explicit_gate() {
        let narrow = Rect::new(0, 0, 79, 30);
        assert!(terminal_too_small(narrow));
        assert!(terminal_too_small(Rect::new(0, 0, 100, 19)));
        assert!(!terminal_too_small(Rect::new(0, 0, 80, 20)));
        assert!(!event_allowed(
            narrow.width,
            narrow.height,
            KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)
        ));
        assert!(event_allowed(
            narrow.width,
            narrow.height,
            KeyEvent::new(KeyCode::Char('q'), KeyModifiers::NONE)
        ));
    }

    #[test]
    fn screens_map_to_five_steps_and_screen_specific_help() {
        assert_eq!(screen_step(Screen::Scan, Screen::Scan), 0);
        assert_eq!(screen_step(Screen::Search, Screen::Scan), 1);
        assert_eq!(screen_step(Screen::Select, Screen::Scan), 2);
        assert_eq!(screen_step(Screen::Preview, Screen::Scan), 3);
        assert_eq!(screen_step(Screen::Result, Screen::Scan), 4);
        assert_eq!(screen_step(Screen::Maintenance, Screen::Scan), 1);
        assert_eq!(screen_step(Screen::MaintenancePreview, Screen::Scan), 3);
        assert_eq!(screen_step(Screen::Help, Screen::Select), 2);
        assert_eq!(screen_help_key(Screen::Select), "keys_select");
        assert_eq!(screen_help_key(Screen::Filter), "keys_filter");
        assert_eq!(screen_help_key(Screen::Maintenance), "keys_maintenance");
    }

    #[test]
    fn maintenance_categories_toggle_without_text_input() {
        let candidates = [
            MaintenanceCandidate {
                key: "rollout:a".into(),
                kind: MaintenanceKind::UnreferencedRollout,
                label: "a".into(),
                detail: "/a".into(),
                project: None,
                bytes: 1,
                fingerprint: "a".into(),
            },
            MaintenanceCandidate {
                key: "edge:a:b".into(),
                kind: MaintenanceKind::StaleSpawnEdge,
                label: "a → b".into(),
                detail: "closed".into(),
                project: None,
                bytes: 0,
                fingerprint: "a:b:closed".into(),
            },
        ];
        let mut selected = BTreeSet::new();
        toggle_maintenance_kind(
            &mut selected,
            &candidates,
            MaintenanceKind::UnreferencedRollout,
        );
        assert_eq!(selected, BTreeSet::from(["rollout:a".into()]));
        toggle_maintenance_kind(
            &mut selected,
            &candidates,
            MaintenanceKind::UnreferencedRollout,
        );
        assert!(selected.is_empty());
    }

    #[test]
    fn execution_progress_renders_real_phase_counts_and_percent() {
        let catalog = Catalog::new(crate::domain::Language::ZhCn);
        let mut terminal = Terminal::new(ratatui::backend::TestBackend::new(100, 20)).unwrap();
        terminal
            .draw(|frame| {
                draw_execution(
                    frame,
                    &catalog,
                    Action::Archive,
                    ExecutionProgress {
                        phase: ExecutionPhase::Applying,
                        completed: 3,
                        total: 4,
                    },
                    0,
                )
            })
            .unwrap();
        let rendered = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>()
            .replace(' ', "");
        assert!(rendered.contains("正在执行状态变更"));
        assert!(rendered.contains("|正在执行状态变更"));
        assert!(rendered.contains("3/4·75%"));
        assert!(rendered.contains("期间不会接受其他按键"));

        terminal
            .draw(|frame| {
                draw_execution(
                    frame,
                    &catalog,
                    Action::Archive,
                    ExecutionProgress {
                        phase: ExecutionPhase::Verifying,
                        completed: 4,
                        total: 4,
                    },
                    1,
                )
            })
            .unwrap();
        let rendered = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>()
            .replace(' ', "");
        assert!(rendered.contains("正在核验最终状态"));
        assert!(rendered.contains("/正在核验最终状态"));
        assert!(rendered.contains("4/4·100%"));
    }

    #[test]
    fn delete_confirmation_input_stays_visible_below_a_long_preview() {
        let planned = (0..100)
            .map(|index| crate::domain::PlannedTree {
                root_id: format!("root-{index}"),
                signature: format!("signature-{index}"),
                node_ids: vec![format!("root-{index}")],
            })
            .collect::<Vec<_>>();
        let mut state = UiState::new(Preferences::default());
        state.catalog = Catalog::new(crate::domain::Language::ZhCn);
        state.screen = Screen::Preview;
        state.input = "12".into();
        state.preview = Some(OperationPreview {
            action: Action::Delete,
            trees: planned,
            impacted_count: 123,
            blocked: Vec::new(),
        });
        let service = VaultService::new(
            Gateway {
                snapshot: selectable_snapshot(),
            },
            Store,
        );
        let mut terminal = Terminal::new(ratatui::backend::TestBackend::new(100, 20)).unwrap();

        terminal
            .draw(|frame| draw_preview(frame, frame.area(), &state, &service))
            .unwrap();

        let rendered = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>()
            .replace(' ', "");
        assert!(rendered.contains("永久删除确认"));
        assert!(rendered.contains("请输入“影响节点”数123:12_"));
    }

    #[tokio::test]
    async fn execution_screen_repaints_while_a_scan_is_pending() {
        let mut service = VaultService::new(
            SlowGateway {
                snapshot: selectable_snapshot(),
                delay: Duration::from_millis(300),
            },
            Store,
        );
        service.refresh().await.unwrap();
        let preview = service.preview(&BTreeSet::from(["one".into()]), Action::Archive);
        let mut state = UiState::new(Preferences::default());
        state.catalog = Catalog::new(crate::domain::Language::ZhCn);
        let mut terminal = Terminal::new(ratatui::backend::TestBackend::new(100, 20)).unwrap();

        execute_preview(&mut terminal, &mut state, &mut service, preview, None)
            .await
            .unwrap();

        let rendered = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>()
            .replace(' ', "");
        assert_eq!(state.screen, Screen::Result);
        assert!(
            [
                "/正在核验最终状态",
                "-正在核验最终状态",
                "\\正在核验最终状态",
            ]
            .iter()
            .any(|value| rendered.contains(value))
        );
    }

    #[tokio::test]
    async fn preview_confirmation_queues_execution_for_the_progress_loop() {
        let mut service = VaultService::new(
            Gateway {
                snapshot: selectable_snapshot(),
            },
            Store,
        );
        service.refresh().await.unwrap();
        let preview = service.preview(&BTreeSet::from(["one".into()]), Action::Archive);
        let mut state = UiState::new(Preferences::default());
        state.screen = Screen::Preview;
        state.preview = Some(preview.clone());

        handle_preview(
            KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE),
            &mut state,
        );

        assert_eq!(state.pending_execution, Some((preview, None)));
        assert_eq!(state.screen, Screen::Preview);
    }

    #[test]
    fn delete_confirmation_requires_the_affected_node_count_before_execution() {
        let preview = OperationPreview {
            action: Action::Delete,
            trees: vec![crate::domain::PlannedTree {
                root_id: "root".into(),
                signature: "signature".into(),
                node_ids: vec!["root".into(), "child".into()],
            }],
            impacted_count: 2,
            blocked: Vec::new(),
        };
        let mut state = UiState::new(Preferences::default());
        state.catalog = Catalog::new(crate::domain::Language::ZhCn);
        state.screen = Screen::Preview;
        state.preview = Some(preview.clone());

        handle_preview(
            KeyEvent::new(KeyCode::Char('1'), KeyModifiers::NONE),
            &mut state,
        );
        handle_preview(
            KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE),
            &mut state,
        );

        assert_eq!(state.pending_execution, None);
        assert_eq!(state.screen, Screen::Preview);
        assert!(state.message.contains("影响节点"));
        assert!(state.message.contains('2'));

        handle_preview(
            KeyEvent::new(KeyCode::Backspace, KeyModifiers::NONE),
            &mut state,
        );
        handle_preview(
            KeyEvent::new(KeyCode::Char('2'), KeyModifiers::NONE),
            &mut state,
        );
        handle_preview(
            KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE),
            &mut state,
        );

        assert_eq!(state.pending_execution, Some((preview, Some(2))));
        assert!(state.message.is_empty());
    }

    #[tokio::test]
    async fn select_all_and_space_update_markers_counts_and_feedback() {
        let mut service = VaultService::new(
            Gateway {
                snapshot: selectable_snapshot(),
            },
            Store,
        );
        service.refresh().await.unwrap();
        let mut state = UiState::new(Preferences::default());
        state.screen = Screen::Select;
        state.choose_action(Action::Archive);

        handle_select(
            KeyEvent::new(KeyCode::Char('A'), KeyModifiers::NONE),
            &mut state,
            &mut service,
        );
        assert_eq!(state.selected.len(), 2);
        assert!(state.message.contains("影响节点=2"));
        assert_eq!(selection_marker(&state.selected, "one"), "[x]");

        let mut terminal = Terminal::new(ratatui::backend::TestBackend::new(120, 30)).unwrap();
        terminal
            .draw(|frame| draw(frame, &state, &service))
            .unwrap();
        let rendered = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        let rendered = rendered.replace(' ', "");
        assert!(rendered.contains("已选择=2"));
        assert!(rendered.contains("选择·已选择2/2"));
        assert_eq!(rendered.matches("[x]").count(), 2);

        handle_select(
            KeyEvent::new(KeyCode::Char(' '), KeyModifiers::NONE),
            &mut state,
            &mut service,
        );
        assert_eq!(state.selected.len(), 1);
        assert!(state.message.contains("已取消选择"));
        assert_eq!(selection_marker(&state.selected, "one"), "[ ]");

        terminal
            .draw(|frame| draw(frame, &state, &service))
            .unwrap();
        let rendered = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>()
            .replace(' ', "");
        assert!(rendered.contains("选择·已选择1/2"));

        handle_select(
            KeyEvent::new(KeyCode::Char('a'), KeyModifiers::SHIFT),
            &mut state,
            &mut service,
        );
        assert_eq!(state.selected.len(), 2);
    }

    #[tokio::test]
    async fn result_scroll_reaches_both_ends_and_language_is_global() {
        let mut service = VaultService::new(
            Gateway {
                snapshot: selectable_snapshot(),
            },
            Store,
        );
        service.refresh().await.unwrap();
        let mut state = UiState::new(Preferences::default());
        state.screen = Screen::Result;
        state.result = (0..30).map(|index| format!("result-{index}")).collect();

        for _ in 0..100 {
            handle_key(
                KeyEvent::new(KeyCode::Down, KeyModifiers::NONE),
                20,
                &mut state,
                &mut service,
            )
            .await
            .unwrap();
        }
        assert_eq!(state.result_offset, 22);
        let mut terminal = Terminal::new(ratatui::backend::TestBackend::new(100, 20)).unwrap();
        terminal
            .draw(|frame| draw(frame, &state, &service))
            .unwrap();
        let rendered = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains("result-29"));

        handle_key(
            KeyEvent::new(KeyCode::Up, KeyModifiers::NONE),
            21,
            &mut state,
            &mut service,
        )
        .await
        .unwrap();
        assert_eq!(state.result_offset, 20);
        let mut terminal = Terminal::new(ratatui::backend::TestBackend::new(100, 21)).unwrap();
        terminal
            .draw(|frame| draw(frame, &state, &service))
            .unwrap();
        let rendered = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains("result-20"));
        assert!(!rendered.contains("result-29"));

        for _ in 0..100 {
            handle_key(
                KeyEvent::new(KeyCode::Up, KeyModifiers::NONE),
                20,
                &mut state,
                &mut service,
            )
            .await
            .unwrap();
        }
        assert_eq!(state.result_offset, 0);
        let original = state.catalog.language();
        handle_key(
            KeyEvent::new(KeyCode::Char('l'), KeyModifiers::NONE),
            20,
            &mut state,
            &mut service,
        )
        .await
        .unwrap();
        assert_ne!(state.catalog.language(), original);

        state.screen = Screen::History;
        let before_history = state.catalog.language();
        handle_key(
            KeyEvent::new(KeyCode::Char('l'), KeyModifiers::NONE),
            20,
            &mut state,
            &mut service,
        )
        .await
        .unwrap();
        assert_ne!(state.catalog.language(), before_history);
    }

    #[test]
    fn large_scope_summary_remains_complete() {
        let trees = (0..5_000)
            .map(|index| tree(&format!("tree-{index}"), false, vec![]))
            .collect::<Vec<_>>();
        let preview = OperationPreview {
            action: Action::Archive,
            trees: trees
                .iter()
                .map(|tree| crate::domain::PlannedTree {
                    root_id: tree.root_id.clone(),
                    signature: tree.signature(),
                    node_ids: vec![tree.root_id.clone()],
                })
                .collect(),
            impacted_count: trees.len(),
            blocked: vec![],
        };
        let references = trees.iter().collect::<Vec<_>>();
        let summary = summarize_scope(&references, preview, Action::Archive);
        assert_eq!(summary.matched, 5_000);
        assert_eq!(summary.eligible.len(), 5_000);
        assert_eq!(summary.impacted, 5_000);
    }

    #[tokio::test]
    async fn global_pin_failure_is_shown_once_instead_of_on_every_row() {
        let mut snapshot = selectable_snapshot();
        for node in &mut snapshot.nodes {
            node.pinned = None;
        }
        snapshot.write_capable = false;
        snapshot.diagnostics = vec!["pin fallback unavailable".into()];
        let mut service = VaultService::new(Gateway { snapshot }, Store);
        service.refresh().await.unwrap();
        let mut state = UiState::new(Preferences::default());
        state.screen = Screen::Select;
        state.choose_action(Action::Archive);

        let mut terminal = Terminal::new(ratatui::backend::TestBackend::new(120, 30)).unwrap();
        terminal
            .draw(|frame| draw(frame, &state, &service))
            .unwrap();
        let rendered = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>()
            .replace(' ', "");

        assert_eq!(rendered.matches("pinfallbackunavailable").count(), 1);
        assert!(!rendered.contains("置顶状态未知"));
        assert!(!rendered.contains("缺少必要的app-server写入能力"));
    }
}
