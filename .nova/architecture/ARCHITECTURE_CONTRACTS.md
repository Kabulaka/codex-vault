# Codex Vault — 架构契约

> 架构契约版本：2
> 蓝图引用：../PROJECT_BLUEPRINT.md

## 1. 并行开发门禁

| 门禁 | 是否需要 | 状态 | 确认依据 |
|------|----------|------|-------------|
| 共享工程骨架 | 是 | 已确认 | ARCH-01a08e78-c219-744a-a8d6-aa1c76a9baf4 |
| 数据所有权与契约 | 是 | 已确认 | ARCH-01a08e78-c219-744a-a8d6-aa1c76a9baf4 |
| 公共 API 契约 | 否 | 不适用 | 无 |
| 事件契约 | 是 | 已确认 | ARCH-01a08e78-c219-744a-a8d6-aa1c76a9baf4 |
| Mock 与测试夹具 | 是 | 已确认 | ARCH-01a08e78-c219-744a-a8d6-aa1c76a9baf4 |

## 2. 契约索引

| 契约类型 | 业务范围 | 路径 | 状态 | 所有者 |
|----------|----------|------|------|--------|
| 工程骨架 | 全项目 | [工程骨架契约](foundation/project-skeleton.md) | 已确认 | 架构负责人 |
| 数据 | 本地会话整理 | [本地数据契约](data/local-session-data.md) | 已确认 | 本地会话整理模块 |
| 事件 | Codex 会话协议子集 | [app-server 协议子集](events/app-server-subset.yaml) | 已确认 | Codex 接入模块 |
| Mock | Codex 会话协议子集 | [app-server 场景夹具](mocks/app-server-scenarios.json) | 已确认 | Codex 接入模块 |

## 3. 硬依赖

| 需求块 | 依赖需求块 | 无法解除的业务原因 | 开发顺序 |
|--------|------------|--------------------|----------|
| REQ-01a08e5a-fe60-749a-8bc9-5b1b292e02d6@v1 | 无 | 无 | 并行 |
