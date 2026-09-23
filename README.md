# Codex Vault

Codex Vault is a current-host TUI for safely archiving, restoring, and
permanently deleting complete Codex conversation trees. It discovers the task
catalog from the newest compatible `state_*.sqlite` under the effective Codex
SQLite home, then attaches to an official `codex app-server` control socket (or
starts a managed app-server) to add runtime state. Archive, unarchive, and
delete operations run through the official `codex` CLI on that same host so
Codex owns every file, index, and database update. Codex Vault never edits
Codex databases or session files.

## Why this differs from claude-code-cleaner

| Area | claude-code-cleaner | Codex Vault |
|---|---|---|
| Primary object | Claude Code files, caches, metrics, and orphaned project data | Interactive Codex root tasks and every spawned descendant |
| Archive | No session archive lifecycle | Archive and restore are first-class operations through the official Codex CLI |
| Age | 30 days is the initial default and can be adjusted | First run uses 30 days, then remembers the last `1d`, `7d`, `30d`, or custom RFC3339 cutoff |
| Selection | Cleanup categories and projects | `A` safely selects every eligible, unprotected tree in the current filter; `Space` toggles one tree |
| Protection | Protected paths and recent files | Pinned, active, queued, waiting, unknown, or incomplete trees are read-only |
| Deletion | Deletes selected filesystem data | Requires a fully archived tree and typing the exact node count |
| Privacy | Scans cleanup targets | Uses metadata only and never reads message turns |

Independent `codex exec` records are excluded. Interactive CLI and VS Code
roots are included together with descendants from the SQLite relationship
catalog. App-server metadata may enrich that catalog but cannot silently remove
historical tasks from it. Missing parents, unsafe rollout paths, unavailable
runtime state, or incompatible schemas protect the affected scope.

The SQLite filenames and schemas are versioned Codex implementation details,
not a public compatibility promise. Codex Vault probes the required task,
relationship, provider, model, archive, pin, and rollout-path columns in
read-only mode and fails closed when they are unavailable. It never reads
`auth.json` or message turns. After deletion, it verifies the task row, exact
rollout path, `session_index.jsonl`, relationship tables, and available thread
history tables before reporting success. A remote Mac Codex app may still need
to restart to refresh its own `local_thread_catalog`; the server-side tool does
not reach across hosts to edit that cache.

## Install and run

Requirements:

- Linux or macOS
- Rust 1.85 or newer when building from source
- `codex` available on `PATH`

```sh
cargo install --path .
codex-vault
```

Optional paths:

```sh
codex-vault --codex /path/to/codex --db /path/to/codex-vault.db
```

Connection selection defaults to `auto`:

```sh
codex-vault --connection auto       # attach when available, otherwise managed
codex-vault --connection attached   # require an existing control socket
codex-vault --connection managed    # always start an independent app-server
```

The scan page shows the current user, host, `CODEX_HOME`, and selected
connection mode. If a control socket exists but cannot be used, auto mode fails
closed rather than silently starting a second app-server. The configured
`codex` executable must provide `archive`, `unarchive`, and `delete`; their
absence also keeps Codex Vault read-only.

There is intentionally no unattended write mode.

Official OpenAI documentation defines `CODEX_HOME` (default `~/.codex`),
`CODEX_SQLITE_HOME`, and the `sessions` / `archived_sessions` roots; it does not
promise the internal database filenames or table schemas used by a particular
Codex build. See [environment variables](https://learn.chatgpt.com/docs/config-file/environment-variables#core-locations)
and [troubleshooting](https://learn.chatgpt.com/docs/reference/troubleshooting#feedback-and-logs).

## 使用与操作流程

主会话整理仍显示五个阶段：**扫描 → 筛选 → 选择 → 预览 → 结果**。扫描和执行期间
显示进度；`Enter` 进入下一阶段，`Esc` 返回。顶部持续显示匹配、可操作、
已阻止、已选择和影响节点数量，底部只显示当前页面可用的快捷键，`?` 可打开完整帮助。

1. 在“扫描”确认发现的会话树和诊断。存在可证明的脏数据时按 `g` 打开独立复选清单。
2. 在“筛选”用 `↑/↓` 选择时间、状态或项目，用 `←/→` 在图形预设间切换；时间支持
   全部、超过 1、7、30 天，无需输入路径。`c` 仍可输入 RFC3339 精确截止时间；`/`
   保留高级组合搜索，例如
   `provider:custom project:/home/nika/workspace`。
3. 在“选择”用 `Tab` / `Shift+Tab` 在“全部”和扫描得到的项目 Tab 间切换，Tab
   会显示当前筛选条件下的会话树数量。再按 `a`、`u` 或 `d` 选择动作；大写 `A`
   会一次选择**当前项目内、适用于当前动作且未受保护**的全部完整会话树；`n` 清空，
   `Space` 只切换当前树。切换项目、动作或修改筛选会自动清空选择。
4. 在“预览”核对树数和准确节点数，再按 `Enter` 执行。永久删除还必须输入
   屏幕显示的准确节点数。
5. 在“结果”查看成功、失败、跳过和中断数量以及逐项结果；结果超过一屏时使用
   `↑/↓` 滚动。

脏数据清理使用单独的 **清单 → 预览 → 执行 → 重扫** 流程。清单只包括任务目录未引用
的 JSONL、子任务已不在目录中的陈旧关系，以及正文路径已不存在的目录记录。`Space`
逐项复选，`1` / `2` / `3` 按类别切换，`A` 全选。执行前会把原始文件和 SQLite 在线
备份保存到 `$CODEX_HOME/codex-vault-backups/<batch-id>/`；SQLite 修改使用事务，结束后
重新扫描验证。备份不会被解析或展示，工具不会读取 `auth.json`，也不会直接删除
SQLite 的 `-wal` / `-shm` 文件。

“缺少 Codex 读写能力”不是等待一段时间后自动获得的权限，而是当前状态库模式、
关系、正文路径、app-server 运行态或官方 CLI 命令不足以安全完成操作。此时 Codex
Vault 会保护受影响的树；升级到兼容版本并重新扫描后才会自动恢复。常规归档、恢复和
删除仍只调用官方 Codex CLI；只有上述已证明失效的数据会进入受控直接维护流程。

## Keyboard

| Key | Action |
|---|---|
| `↑` / `↓` | Move between controls, matching trees, or dirty-data candidates |
| `←` / `→` | Switch time, status, and project filter presets |
| `a` / `u` / `d` | Choose archive, restore, or permanent deletion without executing it |
| `A` | Select all eligible, unprotected trees in the current filter |
| `n` | Clear the selection |
| `Space` | Select or unselect the current eligible complete tree |
| `Enter` / `Esc` | Move forward or back through the guarded flow |
| `g` | Open the dirty-data checklist from the scan page |
| `1` / `2` / `3` | Toggle orphan rollouts, stale relations, or missing-rollout tasks in cleanup |
| `t` | Cycle all, 1, 7, and 30 day cutoffs |
| `c` | Enter an absolute RFC3339 cutoff |
| `/` | Search text or combine `id:`, `title:`, `project:`, `cwd:`, `provider:`, and `model:` filters |
| `v` | Cycle all, active, and archived views (legacy shortcut) |
| `h` | Show local operation history |
| `l` | Switch Chinese and English |
| `r` | Rescan the Codex SQLite catalog and app-server runtime state |
| `?` | Show complete help |
| `q` / `Ctrl-C` | Quit |

Every write is preceded by an immutable SQLite plan. Codex Vault re-reads the
tree after preview and rescans after execution, so partial CLI outcomes
are reported as partial rather than as a false success. Operation records are
retained for 90 days by default.

## Development

```sh
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all
```

The repository uses the MIT license.
