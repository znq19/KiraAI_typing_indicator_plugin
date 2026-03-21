import asyncio
import json
from core.plugin import BasePlugin, logger, on, Priority
from core.chat.message_utils import KiraMessageEvent, KiraMessageBatchEvent
from core.provider.llm_model import LLMRequest


class TypingIndicatorPlugin(BasePlugin):
    def __init__(self, ctx, cfg: dict):
        super().__init__(ctx, cfg)
        self.enable_group = cfg.get("enable_group", False)
        self.delay_seconds = float(cfg.get("delay_seconds", 1.0))
        self.action = "set_input_status"
        self.params_template = {"event_type": 1}  # 1 = 对方正在输入...
        self._delay_tasks = {}  # 会话ID -> asyncio.Task

    async def initialize(self):
        logger.info(f"TypingIndicatorPlugin initialized: enable_group={self.enable_group}, delay={self.delay_seconds}s")
        if not hasattr(self.ctx, 'adapter_mgr'):
            logger.error("PluginContext missing 'adapter_mgr' attribute! Plugin will not work.")

    async def terminate(self):
        # 取消所有未完成的延时任务
        for task in self._delay_tasks.values():
            if not task.done():
                task.cancel()
        self._delay_tasks.clear()

    async def send_typing(self, session: str):
        """实际发送输入状态的核心方法"""
        parts = session.split(":")
        if len(parts) != 3:
            logger.error(f"Invalid session id: {session}")
            return

        adapter_name, chat_type, pid = parts
        if chat_type == "gm" and not self.enable_group:
            return

        adapter = self.ctx.adapter_mgr.get_adapter(adapter_name)
        if not adapter:
            logger.error(f"Adapter '{adapter_name}' not found")
            return

        client = adapter.get_client()
        if not client:
            logger.error("Adapter client not available")
            return

        if chat_type == "dm":
            params = {"user_id": int(pid), "event_type": 1}
        else:
            params = {"group_id": int(pid), "event_type": 1}

        # 尝试通过 send_action 发送
        if hasattr(client, 'send_action') and callable(client.send_action):
            try:
                await client.send_action(self.action, params)
                logger.debug(f"Typing sent via send_action('{self.action}') to {session}")
                return
            except Exception as e:
                logger.debug(f"send_action failed: {e}")
        else:
            logger.debug("No send_action method, falling back to WebSocket")

        # 回退：直接通过 WebSocket 发送 JSON
        ws = getattr(client, 'ws', None)
        if ws and hasattr(ws, 'send'):
            payload = json.dumps({"action": self.action, "params": params})
            try:
                await ws.send(payload)
                logger.debug(f"Typing sent via WebSocket to {session}")
                return
            except Exception as e:
                logger.debug(f"WebSocket send failed: {e}")

        # 尝试其他可能的 WebSocket 属性
        for attr in ['_ws', '_client', 'websocket']:
            ws_attr = getattr(client, attr, None)
            if ws_attr and hasattr(ws_attr, 'send'):
                payload = json.dumps({"action": self.action, "params": params})
                try:
                    await ws_attr.send(payload)
                    logger.debug(f"Typing sent via {attr} to {session}")
                    return
                except Exception as e:
                    logger.debug(f"{attr}.send failed: {e}")
                    continue

        logger.error("No working method to send typing indicator")

    async def _delayed_send_typing(self, session: str, delay: float):
        """延时发送输入状态，如果延时期间被取消则不发送"""
        try:
            await asyncio.sleep(delay)
            await self.send_typing(session)
        except asyncio.CancelledError:
            logger.debug(f"Typing delayed task cancelled for {session}")

    @on.im_message(priority=Priority.HIGH)
    async def on_im_message(self, event: KiraMessageEvent):
        if event.is_group_message() and not self.enable_group:
            return
        sid = event.session.sid
        # 取消之前的延时任务（如果有）
        if sid in self._delay_tasks:
            self._delay_tasks[sid].cancel()
        # 创建新的延时任务
        task = asyncio.create_task(self._delayed_send_typing(sid, self.delay_seconds))
        self._delay_tasks[sid] = task
        # 任务完成后从字典中移除
        task.add_done_callback(lambda t: self._delay_tasks.pop(sid, None))

    @on.llm_request(priority=Priority.HIGH)
    async def on_llm_request(self, event: KiraMessageBatchEvent, req: LLMRequest, *_):
        # 如果启用了延时，并且 on_im_message 已经处理了延时，这里就不重复发送了
        # 保留此钩子是为了兼容，但实际已由 on_im_message 处理
        pass