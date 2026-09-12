---
name: release-codex-vault
description: 分析 Codex Vault 尚未发布的变更，建议 SemVer 版本，并在维护者精确授权后通过标签触发式 GitHub Actions 发布和在线验收四平台二进制。只用于 Kabulaka/codex-vault；不用于 crates.io、签名公证或其他仓库，也不得把版本选择视为推送授权。
---

# 发布 Codex Vault

把版本建议、本地准备、Nova Review、远端推送、标签工作流和 GitHub Release 在线验收视为独立证据状态，分别报告。只有远端标签、成功工作流、Release 和全部资产均已在线验证，才能宣称发布完成。

## 1. 仓库身份与发布契约

仅当当前仓库同时满足以下事实时运行；任一项漂移都停止并重新分析，不按旧技能文本继续发布：

- `Cargo.toml` 的包名是 `codex-vault`，仓库地址是 `https://github.com/Kabulaka/codex-vault`；
- `origin` 的目标仓库是 `Kabulaka/codex-vault`，默认分支是 `main`；
- `.github/workflows/release.yml` 由 `v*` 标签触发，通过 GitHub Actions 构建以下四个 Rust target：
  - `x86_64-unknown-linux-gnu`
  - `aarch64-unknown-linux-gnu`
  - `x86_64-apple-darwin`
  - `aarch64-apple-darwin`
- 每个 target 生成 `codex-vault-vX.Y.Z-<target>.tar.gz` 及对应 `.sha256`；publish job 只在全部 build job 成功后创建 GitHub Release；
- macOS 产物不签名、不公证，发布说明必须明确这一限制。

不得在本技能中增加 Windows、crates.io、包管理器、自动更新、签名、公证或其他发布渠道。

## 2. 获取当前证据

提出版本建议或执行任何远端写入前，取得当前实时证据：

- 当前分支、完整 `HEAD`、工作树、暂存区、上游以及领先/落后数量；
- 本地标签、`git ls-remote` 返回的远端分支与标签；带注释标签必须 peel 到实际 commit；
- GitHub 上的 Release、草稿、预发布和相关 Actions 运行；
- `Cargo.toml` 与 `Cargo.lock` 中的包版本；
- 从最近一个已发布稳定版本到当前 `HEAD` 的 commits、完整 diff、Nova 工作项与可信 Review 关闭状态；
- `.github/workflows/release.yml` 的触发条件、权限、矩阵、打包名和 publish 依赖。

远端标签、Release、工作流或资产只能由 `git ls-remote`、`gh release`、`gh run` 与 GitHub API 的当前结果证明。工作树或暂存区存在非本次发布准备的变化时停止并保留现场。

## 3. 建议版本

以最近一个已发布且非草稿、非预发布的稳定 Release 为变更基线，并以本地或远端已占用的最高稳定版本为下限。已存在但发布失败的版本不得静默复用。

- 尚无任何稳定标签和 Release 时，首发标签必须等于当前 Cargo 包版本并加 `v` 前缀；例如包版本 `0.1.0` 对应 `v0.1.0`。
- 当前主版本为 `0` 时，存在不兼容变化或新的 `FEAT-*`，建议下一个次版本；只有 FIX、PATCH、MAINT、文档或 Review 修正，建议下一个修订版本。
- 当前主版本为 `1+` 时，不兼容变化建议下一个主版本，新功能建议下一个次版本，只有修复或维护建议下一个修订版本。
- 没有用户可观察或发布治理价值的未发布变化时不发布。

同一实现 commit 和对应 Review closure 只算一个逻辑变化，但两者都保留为证据。向维护者展示基线、按工作项聚合的变化、最高影响类别、建议版本和不确定项，并取得具体版本选择。版本选择不授权 push。

## 4. 本地发布门禁

选定版本后重新获取第 2 节证据。满足以下条件才可进入远端发布：

1. `HEAD`、上游和远端标签未漂移，选定标签在本地、远端和 GitHub Release 中均未占用。
2. 目标 commit 中所有 `Review-Policy: required` 的工作项都有可信 Review closure；Nova 待 Review 集合为空。合法 `exempt` 项必须有匹配的客观豁免证据。
3. Cargo 包版本与去掉 `v` 的选定版本一致。若不一致，先更新 `Cargo.toml` 和 `Cargo.lock`，形成独立发布准备 MAINT，完成其验证与适用的 Nova Review 后再继续；不得只靠标签覆盖版本漂移。
4. 复用仍有效的验证证据；证据失效时使用 Rust 1.85.0 执行：

```bash
rustup run 1.85.0 cargo fmt --all -- --check
rustup run 1.85.0 cargo test --locked
rustup run 1.85.0 cargo clippy --locked --all-targets -- -D warnings
rustup run 1.85.0 cargo build --locked --release
```

5. 静态核对 release workflow 恰好覆盖四个目标、每个目标上传压缩包和校验文件、publish 依赖完整 build 且拥有最小 `contents: write` 权限。

## 5. 精确发布授权

任何远端写入前，向维护者展示：

- 选定标签与目标完整 commit；
- `origin` 的准确 push URL、目标分支和 GitHub 仓库；
- 本地相对远端的领先/落后状态；
- 本地验证、Nova Review 和标签冲突检查结果；
- 即将执行的两个远端命令：`git push origin main` 与 `git push origin vX.Y.Z`。

必须取得覆盖上述具体 commit、仓库、分支和标签的明确授权。普通“继续”、版本选择、本地 commit 或先前宽泛发布意图都不能替代该授权。不得在发布过程中新增、替换或改写远端配置。

## 6. 标签工作流发布

精确授权后重新确认状态未变化，再依次执行：

1. `git push origin main`，并验证 `origin/main` 与目标 commit 完全一致。
2. 在该目标 commit 上创建唯一带注释标签 `vX.Y.Z`，消息为 `Codex Vault vX.Y.Z`。
3. 只推送该标签：`git push origin vX.Y.Z`。
4. 持续查询此次标签触发的 `Release` Actions run，直到 `completed`；不得用固定等待时间替代终态。
5. 若工作流成功，在线验证 GitHub Release 为非草稿、非预发布，标签指向目标 commit，标题与正文存在。
6. 下载 Release 到新建临时目录，要求恰好存在四个 `.tar.gz` 和四个同名 `.sha256`。逐个校验 SHA-256，并确认每个压缩包只提供对应平台的 `codex-vault` 可执行文件。
7. 清理临时下载目录；不删除用户文件或仓库内产物。

正常路径只允许标签工作流创建 Release，不同时运行 `gh release create`，也不手工替换 Actions 已发布的资产。

## 7. 失败与恢复

- 推送 `main` 失败：不得创建或推送标签。
- 本地标签创建失败：保留现场，修正原因后重新执行冲突检查。
- 标签已推送但 Actions 或 Release 失败：保留标签、run URL 和失败证据；不得删除、移动、强推或静默复用标签。
- Release 存在但资产数量、名称、摘要或压缩包内容不符：发布未完成，不替换资产；修复工作流或选择新版本必须重新形成维护项并取得授权。
- 对已推送标签的任何破坏性修改、工作流重跑或新版本选择，都需要新的明确决定。

## 8. 完成报告

分别报告：版本依据与选择、目标 commit、本地门禁和 Nova Review、`main` 与标签推送、Actions run URL 与终态、Release URL、八个资产及摘要验证、临时目录清理、失败现场，以及未执行或未验证事项。
