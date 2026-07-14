# KiraAI_typing_indicator_plugin / 私聊显示输入中插件 1.3.0

[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/znq19/KiraAI_typing_indicator_plugin)

该插件已整合至 KiraAI 官方 QQ 增强插件并得到开发者 @xxynet 更优秀的代码修改：https://github.com/xxynet/kira-ai-plugin-qq-enhance

AI 生成回复在私聊时显示“正在输入...”状态，提升交互体验。需要OneBot程序有相应接口。

## 行为说明（与 QQ 增强对齐）

- 在 `llm_request`（`Priority.LOW`）阶段启动，晚于限流等插件；若 `event.is_stopped` 则不启动，避免限流不回复时假输入中。
- 无 `tool_calls` 的最终 `llm_response` 时立即停止。
- `typing_max_seconds`（默认 90s）为兜底：模型/工具异常、掉线等无最终回复时强制停止，防止无限 `Typing sent`。

## WebUI 配置

| 配置项 | 默认 | 说明 |
|---|---|---|
| `typing_delay_seconds` | 2.0 | 首次延时（秒），0 为立即显示 |
| `typing_interval_seconds` | 2.0 | 持续发送间隔（秒） |
| `typing_max_seconds` | 90.0 | 最大持续时长（秒），0 表示不限制（不推荐） |

> 若仍使用旧配置键 `delay_seconds` / `interval_seconds`，代码会兼容读取。
