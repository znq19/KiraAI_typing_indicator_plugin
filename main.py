import asyncio
import json
from core.plugin import BasePlugin, logger, on, Priority
from core.chat.message_utils import KiraMessageEvent, KiraMessageBatchEvent
from core.provider.llm_model import LLMRequest, LLMResponse


class TypingIndicatorPlugin(BasePlugin):
    def __init__(self, ctx, cfg: dict):
        super().__init__(ctx, cfg)
        # 保留配置项但不再使用 enable_group（群聊强制禁用）
        self.delay_seconds = float(cfg.get("delay_seconds", 1.0))
        self.interval_seconds = float(cfg.get("interval_seconds", 3.0))
        self.action = "set_input_status"
        self.params_template = {"event_type": 1}
        self._delay_tasks = {}      # 会话ID -> 延时发送任务（首次）
        self._loop_tasks = {}       # 会话ID -> 持续发送循环任务
        self._typing_running = {}   # 会话ID -> 是否正在持续发送

    async def initialize(self):
        logger.info(
            f"TypingIndicatorPlugin initialized (private only): "
            f"delay={self.delay_seconds}s, interval={self.interval_seconds}s"
        )
        if not hasattr(self.ctx, 'adapter_mgr'):
            logger.error("PluginContext missing 'adapter_mgr' attribute! Plugin will not work.")

    async def terminate(self):
        for task in self._delay_tasks.values():
            if not task.done():
                task.cancel()
        for task in self._loop_tasks.values():
            if not task.done():
                task.cancel()
        self._delay_tasks.clear()
        self._loop_tasks.clear()
        self._typing_running.clear()

    async def send_typing(self, session: str):
        parts = session.split(":")
        if len(parts) != 3:
            logger.error(f"Invalid session id: {session}")
            return

        adapter_name, chat_type, pid = parts
        # 强制禁止群聊
        if chat_type == "gm":
            return

        adapter = self.ctx.adapter_mgr.get_adapter(adapter_name)
        if not adapter:
            logger.error(f"Adapter '{adapter_name}' not found")
            return

        client = adapter.get_client()
        if not client:
            logger.error("Adapter client not available")
            return

        # 私聊参数
        params = {"user_id": int(pid), "event_type": 1}

        if hasattr(client, 'send_action') and callable(client.send_action):
            try:
                await client.send_action(self.action, params)
                logger.debug(f"Typing sent via send_action('{self.action}') to {session}")
                return
            except Exception as e:
                logger.debug(f"send_action failed: {e}")
        else:
            logger.debug("No send_action method, falling back to WebSocket")

        ws = getattr(client, 'ws', None)
        if ws and hasattr(ws, 'send'):
            payload = json.dumps({"action": self.action, "params": params})
            try:
                await ws.send(payload)
                logger.debug(f"Typing sent via WebSocket to {session}")
                return
            except Exception as e:
                logger.debug(f"WebSocket send failed: {e}")

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
        try:
            await asyncio.sleep(delay)
            await self.send_typing(session)
            if session not in self._loop_tasks or self._loop_tasks[session].done():
                self._typing_running[session] = True
                task = asyncio.create_task(self._typing_loop(session))
                self._loop_tasks[session] = task
        except asyncio.CancelledError:
            logger.debug(f"Typing delayed task cancelled for {session}")

    async def _typing_loop(self, session: str):
        while self._typing_running.get(session, False):
            try:
                await asyncio.sleep(self.interval_seconds)
                if self._typing_running.get(session, False):
                    await self.send_typing(session)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Typing loop error for {session}: {e}")
        logger.debug(f"Typing loop stopped for {session}")

    def _stop_typing_loop(self, session: str):
        if session in self._typing_running:
            self._typing_running[session] = False
        if session in self._loop_tasks and not self._loop_tasks[session].done():
            self._loop_tasks[session].cancel()
        self._loop_tasks.pop(session, None)
        self._typing_running.pop(session, None)

    @on.im_message(priority=Priority.HIGH)
    async def on_im_message(self, event: KiraMessageEvent):
        # 只处理私聊
        if event.is_group_message():
            return
        sid = event.session.sid

        self._stop_typing_loop(sid)

        if sid in self._delay_tasks and not self._delay_tasks[sid].done():
            self._delay_tasks[sid].cancel()

        task = asyncio.create_task(self._delayed_send_typing(sid, self.delay_seconds))
        self._delay_tasks[sid] = task
        task.add_done_callback(lambda t: self._delay_tasks.pop(sid, None))

    @on.llm_response(priority=Priority.HIGH)
    async def on_llm_response(self, event: KiraMessageBatchEvent, resp: LLMResponse):
        # 只处理私聊
        if event.is_group_message():
            return
        sid = event.sid
        if not resp.tool_calls:
            self._stop_typing_loop(sid)
            logger.debug(f"Stopped typing loop for {sid} due to final response (no tool calls)")

    @on.llm_request(priority=Priority.HIGH)
    async def on_llm_request(self, event: KiraMessageBatchEvent, req: LLMRequest, *_):
        pass