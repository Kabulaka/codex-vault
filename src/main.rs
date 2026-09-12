use std::{
    env,
    ffi::OsString,
    io::{self, stdout},
    path::PathBuf,
};

use anyhow::{Context, Result, bail};
use codex_vault::{
    adapters::{codex::AppServerClient, storage::StorageBackend},
    application::VaultService,
    ui,
};
use crossterm::{
    cursor::Show,
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{Terminal, backend::CrosstermBackend};

struct Options {
    codex: PathBuf,
    database: PathBuf,
}

impl Options {
    fn parse() -> Result<Option<Self>> {
        Self::parse_from(env::args_os().skip(1), default_database_path)
    }

    fn parse_from<I, T, F>(args: I, default_database: F) -> Result<Option<Self>>
    where
        I: IntoIterator<Item = T>,
        T: Into<OsString>,
        F: FnOnce() -> Result<PathBuf>,
    {
        let mut args = args.into_iter().map(Into::into);
        let mut codex = PathBuf::from("codex");
        let mut database = None;
        while let Some(argument) = args.next() {
            match argument.to_str() {
                Some("-h" | "--help") => {
                    println!(
                        "Codex Vault {}\n\nUsage: codex-vault [--codex PATH] [--db PATH]\n\n\
                         A local interactive TUI. No unattended write mode is provided.",
                        env!("CARGO_PKG_VERSION")
                    );
                    return Ok(None);
                }
                Some("-V" | "--version") => {
                    println!("codex-vault {}", env!("CARGO_PKG_VERSION"));
                    return Ok(None);
                }
                Some("--codex") => codex = required_path("--codex", args.next())?,
                Some("--db") => database = Some(required_path("--db", args.next())?),
                Some(value) => bail!("unknown argument: {value}"),
                None => bail!("arguments must be valid UTF-8"),
            }
        }
        let database = match database {
            Some(path) => path,
            None => default_database()?,
        };
        Ok(Some(Self { codex, database }))
    }
}

fn required_path(name: &str, value: Option<OsString>) -> Result<PathBuf> {
    value
        .map(PathBuf::from)
        .filter(|path| !path.as_os_str().is_empty())
        .with_context(|| format!("{name} requires a non-empty path"))
}

fn default_database_path() -> Result<PathBuf> {
    database_path_from(|name| env::var_os(name))
}

fn database_path_from(mut get: impl FnMut(&str) -> Option<OsString>) -> Result<PathBuf> {
    if let Some(value) = get("CODEX_VAULT_STATE_DIR") {
        return Ok(PathBuf::from(value).join("codex-vault.db"));
    }
    if cfg!(target_os = "macos") {
        let home = get("HOME").context("HOME is required to locate application state")?;
        return Ok(PathBuf::from(home)
            .join("Library")
            .join("Application Support")
            .join("codex-vault")
            .join("codex-vault.db"));
    }
    if let Some(value) = get("XDG_STATE_HOME") {
        return Ok(PathBuf::from(value)
            .join("codex-vault")
            .join("codex-vault.db"));
    }
    let home = get("HOME").context("HOME is required to locate application state")?;
    Ok(PathBuf::from(home)
        .join(".local")
        .join("state")
        .join("codex-vault")
        .join("codex-vault.db"))
}

struct TerminalCleanup;

impl TerminalCleanup {
    fn activate() -> io::Result<Self> {
        enable_raw_mode()?;
        if let Err(error) = execute!(stdout(), EnterAlternateScreen) {
            let _ = disable_raw_mode();
            return Err(error);
        }
        Ok(Self)
    }
}

impl Drop for TerminalCleanup {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(stdout(), LeaveAlternateScreen, Show);
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let Some(options) = Options::parse()? else {
        return Ok(());
    };
    let store = StorageBackend::open_or_read_only(&options.database);
    let client = AppServerClient::spawn(&options.codex)
        .await
        .context("unable to start and initialize local codex app-server")?;
    let mut service = VaultService::new(client, store);
    eprintln!("Codex Vault: scanning local session metadata through app-server...");
    service
        .refresh()
        .await
        .context("unable to discover local Codex sessions")?;
    service
        .recover_and_prune()
        .context("unable to reconcile interrupted operations")?;
    let preferences = service.load_preferences().unwrap_or_default();

    let cleanup = TerminalCleanup::activate().context("unable to initialize terminal")?;
    let mut terminal = Terminal::new(CrosstermBackend::new(stdout()))
        .context("unable to create terminal renderer")?;
    let result = ui::run(&mut terminal, &mut service, preferences).await;
    drop(terminal);
    drop(cleanup);
    result.context("Codex Vault stopped with an error")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_state_directory_wins() {
        let value = database_path_from(|name| {
            (name == "CODEX_VAULT_STATE_DIR").then(|| "/tmp/codex-vault-test-state".into())
        })
        .unwrap();
        assert_eq!(
            value,
            PathBuf::from("/tmp/codex-vault-test-state/codex-vault.db")
        );
    }

    #[test]
    fn help_version_and_explicit_database_do_not_require_home() {
        let unavailable = || bail!("HOME unavailable");
        assert!(
            Options::parse_from(["--help"], unavailable)
                .unwrap()
                .is_none()
        );
        assert!(
            Options::parse_from(["--version"], || bail!("HOME unavailable"))
                .unwrap()
                .is_none()
        );
        let options =
            Options::parse_from(["--db", "/tmp/explicit.db"], || bail!("HOME unavailable"))
                .unwrap()
                .unwrap();
        assert_eq!(options.database, PathBuf::from("/tmp/explicit.db"));
    }
}
