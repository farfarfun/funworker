# Changelog

## 未发布

### 修复

- `BaseProcessor.on_error`/`BaseConsumer.on_error` 默认日志不再直接 repr 完整 `item`
  内容（可能携带凭据、用户隐私等业务敏感数据），改为只记录类型名；完整内容降级到
  `DEBUG` 级别，需调用方显式把日志级别调到 DEBUG 才会落盘
  （farfarfun/todo-list#766）。

## 0.1.6

### 新增

- 增加 `uv.lock` 和 Ruff 开发配置。

### 修复

- 将 `farlog` 依赖下限更新到组织规范要求的版本。
- 补全 `Many.__init__`、`CountingQueue.put` 的参数类型标注；批处理超时相关测试改用
  `threading.Event` 等待替代固定 `time.sleep`，避免在慢环境下产生不确定性
  （farfarfun/todo-list#619）。

### 变更

- 构建后端迁移至 Hatchling，类型标注统一为 Python 3.10 语法。
- 补充开发环境、仓库忽略规则和组织信息文档。

### 废弃

（无）

## 0.1.5 及更早版本（早于 CHANGELOG 引入）

本文件自 0.1.6 版本起开始逐版本维护；0.1.5 及更早版本发布时尚未建立 CHANGELOG 记录
规范，没有可靠的逐版本变更记录来源，不做追溯性重建（避免凭 git 提交历史事后臆测、
整理出看似精确实则不可靠的条目）。
