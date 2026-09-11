# Codex Vault

Codex Vault is a local TUI for safely archiving, restoring, and permanently
deleting complete Codex conversation trees. It talks only to a locally spawned
official `codex app-server`; it never edits Codex databases or session files.

## Why this differs from claude-code-cleaner

| Area | claude-code-cleaner | Codex Vault |
|---|---|---|
| Primary object | Claude Code files, caches, metrics, and orphaned project data | Interactive Codex root tasks and every spawned descendant |
| Archive | No session archive lifecycle | Archive and restore are first-class operations through app-server |
| Age | 30 days is the initial default and can be adjusted | First run uses 30 days, then remembers the last `1d`, `7d`, `30d`, or custom RFC3339 cutoff |
| Selection | Cleanup categories and projects | `A` safely selects every eligible, unprotected tree in the current filter; `Space` toggles one tree |
| Protection | Protected paths and recent files | Pinned, active, queued, waiting, unknown, or incomplete trees are read-only |
| Deletion | Deletes selected filesystem data | Requires a fully archived tree and typing the exact node count |
| Privacy | Scans cleanup targets | Uses metadata only and never reads message turns |

Independent `codex exec` records are excluded. Interactive CLI and VS Code
roots are included together with descendants discovered through the
experimental ancestor relationship API. If that relationship cannot be
proven complete, Codex Vault stays read-only.

Pinned-state handling is also capability-driven. Some Codex app-server builds
accept the `isPinned` filter but do not return pin metadata and ignore that
filter. Codex Vault detects overlapping true/false results and deliberately
stays read-only instead of treating an unknown pin state as safe.

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

There is intentionally no unattended write mode.

## 使用与操作流程

界面固定显示五个阶段：**扫描 → 筛选 → 选择 → 预览 → 结果**。`Enter`
进入下一阶段，`Esc` 返回；不能用数字键跳过安全步骤。顶部持续显示匹配、可操作、
已阻止、已选择和影响节点数量，底部只显示当前页面可用的快捷键，`?` 可打开完整帮助。

1. 在“扫描”确认发现的会话树和只读诊断。
2. 在“筛选”用 `t` 选择 1、7、30 天，或用 `c` 输入 RFC3339 截止时间；
   `/` 可按标题、ID、项目或 cwd 搜索。
3. 在“选择”先按 `a`、`u` 或 `d` 选择动作。按大写 `A` 会一次选择
   **当前筛选结果内、适用于当前动作且未受保护**的全部完整会话树；`n` 清空，
   `Space` 只切换当前树。切换动作或修改筛选会自动清空选择。
4. 在“预览”核对树数和准确节点数，再按 `Enter` 执行。永久删除还必须输入
   屏幕显示的准确节点数。
5. 在“结果”查看成功、失败、跳过和中断数量以及逐项结果。

“缺少 app-server 写入能力”不是等待一段时间后自动获得的权限，而是当前 Codex
`app-server` 没有暴露工具所需的完整能力。此时 Codex Vault 会保持只读；升级到提供
完整关系、置顶状态和写入接口的官方 Codex 版本并重新扫描后，才会自动恢复可操作状态。
工具不会直接修改 Codex 数据库来绕过此限制。

## Keyboard

| Key | Action |
|---|---|
| `↑` / `↓` | Move between matching trees |
| `a` / `u` / `d` | Choose archive, restore, or permanent deletion without executing it |
| `A` | Select all eligible, unprotected trees in the current filter |
| `n` | Clear the selection |
| `Space` | Select or unselect the current eligible complete tree |
| `Enter` / `Esc` | Move forward or back through the five guarded stages |
| `t` | Cycle rolling 1, 7, and 30 day cutoffs |
| `c` | Enter an absolute RFC3339 cutoff |
| `/` | Search text or combine `id:`, `title:`, `project:`, and `cwd:` filters |
| `v` | Cycle all, active, and archived views |
| `h` | Show local operation history |
| `l` | Switch Chinese and English |
| `r` | Rescan app-server state |
| `?` | Show complete help |
| `q` / `Ctrl-C` | Quit |

Every write is preceded by an immutable SQLite plan. Codex Vault re-reads the
tree after preview and rescans after execution, so partial app-server outcomes
are reported as partial rather than as a false success. Operation records are
retained for 90 days by default.

## Development

```sh
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all
```

The repository uses the MIT license.
