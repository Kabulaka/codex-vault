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
        let mut args = env::args_os().skip(1);
        let mut codex = PathBuf::from("codex");
        let mut database = default_database_path()?;
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
                Some("--db") => database = required_path("--db", args.next())?,
                Some(value) => bail!("unknown argument: {value}"),
                None => bail!("arguments must be valid UTF-8"),
            }
        }
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
    if let Some(value) = env::var_os("CODEX_VAULT_STATE_DIR") {
        return Ok(PathBuf::from(value).join("codex-vault.db"));
    }
    if cfg!(target_os = "macos") {
        let home = env::var_os("HOME").context("HOME is required to locate application state")?;
        return Ok(PathBuf::from(home)
            .join("Library")
            .join("Application Support")
            .join("codex-vault")
            .join("codex-vault.db"));
    }
    if let Some(value) = env::var_os("XDG_STATE_HOME") {
        return Ok(PathBuf::from(value)
            .join("codex-vault")
            .join("codex-vault.db"));
    }
    let home = env::var_os("HOME").context("HOME is required to locate application state")?;
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
    let _ = service.recover_and_prune();
    eprintln!("Codex Vault: scanning local session metadata through app-server...");
    service
        .refresh()
        .await
        .context("unable to discover local Codex sessions")?;
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
        unsafe { env::set_var("CODEX_VAULT_STATE_DIR", "/tmp/codex-vault-test-state") };
        let value = default_database_path().unwrap();
        unsafe { env::remove_var("CODEX_VAULT_STATE_DIR") };
        assert_eq!(
            value,
            PathBuf::from("/tmp/codex-vault-test-state/codex-vault.db")
        );
    }
}
