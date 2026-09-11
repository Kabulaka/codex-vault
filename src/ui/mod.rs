use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    time::Duration,
};

use chrono::{DateTime, Local};
use crossterm::event::{Event, EventStream, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use futures_util::StreamExt;
use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, Paragraph, Wrap},
};

use crate::{
    application::VaultService,
    domain::{
        Action, Cutoff, Filter, HistoryEntry, ItemResult, OperationPreview, OperationStore,
        Preferences, ProtectionReason, SessionGateway, SessionTree, VaultError,
    },
    i18n::Catalog,
};

const MIN_TERMINAL_WIDTH: u16 = 80;
const MIN_TERMINAL_HEIGHT: u16 = 20;

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
}

#[derive(Debug, Default)]
struct ScopeSummary {
    matched: usize,
    eligible: BTreeSet<String>,
    impacted: usize,
    blocked: BTreeMap<String, Vec<ProtectionReason>>,
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
        if handle_key(key, &mut state, service).await? {
            break;
        }
    }
    state.sync_preferences();
    service.save_preferences(&state.preferences)?;
    Ok(())
}

async fn handle_key<G, S>(
    key: KeyEvent,
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
        state.history = service.history(50).unwrap_or_default();
        state.screen = Screen::History;
        return Ok(false);
    }

    match state.screen {
        Screen::Scan => handle_scan(key, state, service).await,
        Screen::Filter => handle_filter(key, state, service).await,
        Screen::Select => handle_select(key, state, service),
        Screen::Preview => handle_preview(key, state, service).await,
        Screen::Result => {
            if key.code == KeyCode::Enter {
                state.screen = Screen::Scan;
                state.action = None;
                state.preview = None;
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
        KeyCode::Char('r') => {
            state.message = match service.refresh().await {
                Ok(trees) => format!("{} {}", trees.len(), state.catalog.text("trees")),
                Err(error) => error.to_string(),
            };
            state.reset_selection();
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
                Cutoff::RollingDays(1) => Cutoff::RollingDays(7),
                Cutoff::RollingDays(7) => Cutoff::RollingDays(30),
                _ => Cutoff::RollingDays(1),
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
        KeyCode::Char('l') => {
            let language = match state.catalog.language() {
                crate::domain::Language::En => crate::domain::Language::ZhCn,
                crate::domain::Language::ZhCn => crate::domain::Language::En,
            };
            state.catalog = Catalog::new(language);
            persist_preferences(state, service);
        }
        KeyCode::Char('r') => {
            state.message = match service.refresh().await {
                Ok(trees) => format!("{} {}", trees.len(), state.catalog.text("trees")),
                Err(error) => error.to_string(),
            };
            state.reset_selection();
        }
        _ => {}
    }
}

fn handle_select<G, S>(key: KeyEvent, state: &mut UiState, service: &VaultService<G, S>)
where
    G: SessionGateway,
    S: OperationStore,
{
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

async fn handle_preview<G, S>(key: KeyEvent, state: &mut UiState, service: &mut VaultService<G, S>)
where
    G: SessionGateway,
    S: OperationStore,
{
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
            KeyCode::Char(value) if value.is_ascii_digit() => state.input.push(value),
            KeyCode::Backspace => {
                state.input.pop();
            }
            KeyCode::Enter => {
                let confirmation = state.input.parse::<usize>().ok();
                execute_preview(state, service, preview, confirmation).await;
            }
            _ => {}
        }
    } else if key.code == KeyCode::Enter {
        execute_preview(state, service, preview, None).await;
    }
}

async fn execute_preview<G, S>(
    state: &mut UiState,
    service: &mut VaultService<G, S>,
    preview: OperationPreview,
    confirmation: Option<usize>,
) where
    G: SessionGateway,
    S: OperationStore,
{
    state.result.clear();
    match service.execute(&preview, confirmation).await {
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
    let mut summary = ScopeSummary {
        matched: trees.len(),
        blocked: preview.blocked.into_iter().collect(),
        ..ScopeSummary::default()
    };
    for planned in preview.trees {
        let candidate = trees
            .iter()
            .find(|tree| tree.root_id == planned.root_id)
            .is_some_and(|tree| action_candidate(tree, action));
        if candidate && !planned.node_ids.is_empty() {
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
    draw_header(frame, areas[0], state, service);
    match state.screen {
        Screen::Scan => draw_scan(frame, areas[1], state, service),
        Screen::Filter => draw_filter(frame, areas[1], state, service),
        Screen::Select => draw_select(frame, areas[1], state, service),
        Screen::Preview => draw_preview(frame, areas[1], state, service),
        Screen::Result => draw_result(frame, areas[1], state),
        Screen::History => draw_history(frame, areas[1], state),
        Screen::Help => draw_help(frame, areas[1], state),
        Screen::Search | Screen::CustomCutoff => draw_input(frame, areas[1], state),
    }
    draw_footer(frame, areas[2], state, service);
}

fn draw_header<G, S>(frame: &mut Frame, area: Rect, state: &UiState, service: &VaultService<G, S>)
where
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
    let (eligible, blocked, impacted) = state.action.map_or((0, 0, 0), |action| {
        let scope = scope_summary(service, &state.filter, action);
        let selected_impacted = service.preview(&state.selected, action).impacted_count;
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
    let mut lines = vec![
        Line::from(Span::styled(
            state.catalog.text("scan_ready"),
            Style::default().add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
        Line::from(format!(
            "{}: {}    {}: {}    {}: {}",
            state.catalog.text("trees"),
            service.trees().len(),
            state.catalog.text("archived"),
            archived,
            state.catalog.text("protected"),
            protected
        )),
    ];
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
    let lines = vec![
        Line::from(Span::styled(
            state.catalog.text("filter_intro"),
            Style::default().add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
        Line::from(format!(
            "{}: {}",
            state.catalog.text("cutoff"),
            cutoff_text(&state.filter.cutoff)
        )),
        Line::from(format!(
            "{}: {}",
            state.catalog.text("view"),
            view_text(&state.catalog, state.filter.archived)
        )),
        Line::from(format!(
            "{}: {}",
            state.catalog.text("query"),
            state.filter.query.as_deref().unwrap_or("*")
        )),
        Line::from(format!(
            "{}: {}",
            state.catalog.text("project"),
            state.filter.project.as_deref().unwrap_or("*")
        )),
        Line::from(""),
        Line::from(format!(
            "{}: {}",
            state.catalog.text("matched"),
            service.filtered(&state.filter).len()
        )),
    ];
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

fn draw_select<G, S>(frame: &mut Frame, area: Rect, state: &UiState, service: &VaultService<G, S>)
where
    G: SessionGateway,
    S: OperationStore,
{
    let trees = service.filtered(&state.filter);
    if trees.is_empty() {
        frame.render_widget(
            Paragraph::new(state.catalog.text("empty")).block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(state.catalog.text("step_select")),
            ),
            area,
        );
        return;
    }
    let scope = state
        .action
        .map(|action| scope_summary(service, &state.filter, action));
    let visible = area.height.saturating_sub(2) as usize;
    let index = clamp_index(state.index, trees.len());
    let start = window_start(index, trees.len(), visible);
    let width = area.width.saturating_sub(2) as usize;
    let items = trees
        .iter()
        .enumerate()
        .skip(start)
        .take(visible)
        .map(|(position, tree)| {
            let selected = selection_marker(&state.selected, &tree.root_id);
            let marker = if position == index { "›" } else { " " };
            let eligible = scope
                .as_ref()
                .is_some_and(|value| value.eligible.contains(&tree.root_id));
            let reason = match (state.action, scope.as_ref()) {
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
            let compact = width < 100;
            let title_width = if compact { 12 } else { 28 };
            let location_width = if compact { 8 } else { 24 };
            let reason_width = if compact { 10 } else { 30 };
            let last = if compact {
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
                fit_to_width(&reason, reason_width)
            };
            let row = format!(
                "{marker}{selected} {} · {} · {} · {last} · {}{} · {status} · {reason}",
                fit_to_width(title, title_width),
                short_id(&tree.root_id),
                fit_to_width(location, location_width),
                tree.nodes.len(),
                state.catalog.text("nodes")
            );
            let style = if position == index {
                Style::default().fg(Color::Black).bg(Color::Cyan)
            } else if eligible {
                Style::default().fg(Color::White)
            } else {
                Style::default().fg(Color::DarkGray)
            };
            ListItem::new(fit_to_width(&row, width)).style(style)
        })
        .collect::<Vec<_>>();
    frame.render_widget(
        List::new(items).block(Block::default().borders(Borders::ALL).title(format!(
            "{} · {} {}/{}",
            state.catalog.text("step_select"),
            state.catalog.text("selected"),
            state.selected.len(),
            trees.len()
        ))),
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
    let mut lines = vec![
        Line::from(Span::styled(
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
        Line::from(format!(
            "{}: {} · {}: {} · {}: {}",
            state.catalog.text("cutoff"),
            cutoff_text(&state.filter.cutoff),
            state.catalog.text("view"),
            view_text(&state.catalog, state.filter.archived),
            state.catalog.text("query"),
            state.filter.query.as_deref().unwrap_or("*")
        )),
        Line::from(""),
    ];
    for planned in &preview.trees {
        let title = service
            .trees()
            .iter()
            .find(|tree| tree.root_id == planned.root_id)
            .and_then(|tree| tree.nodes.first())
            .map_or("-", |node| node.title.as_str());
        lines.push(Line::from(format!(
            "✓ {} · {} · {}{}",
            fit_to_width(title, 36),
            short_id(&planned.root_id),
            planned.node_ids.len(),
            state.catalog.text("nodes")
        )));
    }
    for (root, reasons) in &preview.blocked {
        lines.push(Line::from(format!(
            "✗ {}: {}",
            short_id(root),
            reasons
                .iter()
                .map(|reason| protection_text(&state.catalog, reason))
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }
    lines.push(Line::from(""));
    lines.push(Line::from(if preview.action == Action::Delete {
        format!(
            "{} {}: {}_",
            state.catalog.text("delete_confirm_prefix"),
            preview.impacted_count,
            state.input
        )
    } else {
        state.catalog.text("confirm").to_owned()
    }));
    let height = area.height.saturating_sub(2) as usize;
    let max_start = lines.len().saturating_sub(height);
    let offset = state.preview_offset.min(max_start);
    frame.render_widget(
        Paragraph::new(
            lines
                .into_iter()
                .skip(offset)
                .take(height)
                .collect::<Vec<_>>(),
        )
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(state.catalog.text("step_preview")),
        )
        .wrap(Wrap { trim: false }),
        area,
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
    frame.render_widget(
        Paragraph::new(state.result.join("\n"))
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(state.catalog.text("step_result")),
            )
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
        Screen::Filter | Screen::Search | Screen::CustomCutoff => 1,
        Screen::Select => 2,
        Screen::Preview => 3,
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
        Screen::Result => "keys_result",
        Screen::History => "keys_history",
        Screen::Help => "keys_help",
        Screen::Search | Screen::CustomCutoff => "keys_input",
    }
}

fn terminal_too_small(area: Rect) -> bool {
    area.width < MIN_TERMINAL_WIDTH || area.height < MIN_TERMINAL_HEIGHT
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
        MutationAck, PortFuture, RuntimeStatus, ScanSnapshot, SessionNode, SessionSource,
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
        ) -> Result<(), VaultError> {
            unreachable!("selection tests never create a batch")
        }

        fn record_result(
            &mut self,
            _: &str,
            _: &str,
            _: &ItemResult,
            _: i64,
        ) -> Result<(), VaultError> {
            unreachable!("selection tests never record results")
        }

        fn finish_batch(&mut self, _: &str, _: &str) -> Result<(), VaultError> {
            unreachable!("selection tests never finish a batch")
        }

        fn recover_interrupted(&mut self) -> Result<usize, VaultError> {
            Ok(0)
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
                cwd: "/tmp/项目".into(),
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
        assert!(terminal_too_small(Rect::new(0, 0, 79, 30)));
        assert!(terminal_too_small(Rect::new(0, 0, 100, 19)));
        assert!(!terminal_too_small(Rect::new(0, 0, 80, 20)));
    }

    #[test]
    fn screens_map_to_five_steps_and_screen_specific_help() {
        assert_eq!(screen_step(Screen::Scan, Screen::Scan), 0);
        assert_eq!(screen_step(Screen::Search, Screen::Scan), 1);
        assert_eq!(screen_step(Screen::Select, Screen::Scan), 2);
        assert_eq!(screen_step(Screen::Preview, Screen::Scan), 3);
        assert_eq!(screen_step(Screen::Result, Screen::Scan), 4);
        assert_eq!(screen_step(Screen::Help, Screen::Select), 2);
        assert_eq!(screen_help_key(Screen::Select), "keys_select");
        assert_eq!(screen_help_key(Screen::Filter), "keys_filter");
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
            &service,
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
            &service,
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
            &service,
        );
        assert_eq!(state.selected.len(), 2);
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
