# Codex Vault — 工程骨架契约

> 工程骨架契约版本：1

## 1. 技术与运行

| 语言 | 框架 | 运行形态 | 持久化 | 缓存 | 消息 | 鉴权 | 部署 |
|------|------|----------|--------|------|------|------|------|
| Rust 2024 | Ratatui、Crossterm、Tokio、Serde | 单个原生 TUI 进程拥有一个本地 `codex app-server` 标准输入输出子进程 | Rusqlite 内置 SQLite，仅保存偏好和操作记录 | 不使用 | 不使用消息系统；仅处理当前子进程 JSONL | 不单独鉴权，继承当前操作系统用户权限 | MIT；GitHub Releases 发布 Linux x86_64/ARM64 与 macOS Intel/Apple Silicon 二进制和 SHA-256；macOS 不签名、不公证 |

## 2. 目录与依赖

| 代码区域 | 职责 | 允许依赖 | 禁止依赖 |
|----------|------|----------|----------|
| `src/domain/` | 会话树、资格、操作计划、结果和端口 | Rust 标准库 | Ratatui、Crossterm、Tokio、Serde JSON、Rusqlite、进程和文件系统实现 |
| `src/application/` | 扫描、筛选、预览、执行和核对用例 | `src/domain/` | 终端渲染、JSONL、SQLite 和具体子进程 |
| `src/ui/` | 终端状态机、输入与渲染 | `src/application/`、领域只读模型、`src/i18n/` | `src/adapters/` 和业务数据写入 |
| `src/adapters/codex/` | app-server 传输、能力探测、协议映射 | 领域端口、Tokio、Serde | Ratatui、SQLite 和领域策略判断 |
| `src/adapters/storage/` | 自有数据迁移、事务和保留期 | 领域端口、Rusqlite | Ratatui、Codex 协议和会话正文 |
| `src/i18n/`、`locales/` | 双语文案和语言选择 | 文案资源与标准库 | Codex 协议、SQLite 和领域状态变更 |
| `src/main.rs` | 组合根、终端与子进程生命周期 | UI、应用、适配器、国际化 | 具体业务资格和筛选规则 |
| `tests/` | 跨层验收和故障夹具 | 公开用例、领域端口、脱敏 Mock | 真实用户会话写操作和私有数据 |
| `.github/workflows/` | 构建、测试、四目标打包和发布 | Cargo 与 GitHub Releases | 产品运行时逻辑、签名凭据和部分目标发布 |
