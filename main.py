import asyncio
import json
from core.plugin import BasePlugin, logger, on, Priority
from core.chat.message_utils import KiraMessageBatchEvent
from core.chat import Session
from core.provider.llm_model import LLMRequest, LLMResponse


class TypingIndicatorPlugin(BasePlugin):
    def __init__(self, ctx, cfg: dict):
        super().__init__(ctx, cfg)
        # 兼容旧配置键 delay_seconds / interval_seconds
        self.typing_delay_seconds = float(
            cfg.get("typing_delay_seconds", cfg.get("delay_seconds", 2.0))
        )
        self.typing_interval_seconds = float(
            cfg.get("typing_interval_seconds", cfg.get("interval_seconds", 2.0))
        )
        self.typing_max_seconds = float(cfg.get("typing_max_seconds", 90.0))
        self.action = "set_input_status"
        self._delay_tasks = {}
        self._loop_tasks = {}
        self._max_tasks = {}
        self._typing_running = {}

    async def initialize(self):
        logger.info(
            f"TypingIndicatorPlugin initialized (QQ only): "
            f"delay={self.typing_delay_seconds}s, "
            f"interval={self.typing_interval_seconds}s, "
            f"max={self.typing_max_seconds}s"
        )
        if not hasattr(self.ctx, "adapter_mgr"):
            logger.error("PluginContext missing 'adapter_mgr' attribute! Plugin will not work.")

    async def terminate(self):
        for task in self._delay_tasks.values():
            if not task.done():
                task.cancel()
        for task in self._loop_tasks.values():
            if not task.done():
                task.cancel()
        for task in self._max_tasks.values():
            if not task.done():
                task.cancel()
        self._delay_tasks.clear()
        self._loop_tasks.clear()
        self._max_tasks.clear()
        self._typing_running.clear()

    async def _send_typing(self, session: Session):
        # 群聊不发送
        if session.session_type == "gm":
            return

        adapter = self.ctx.adapter_mgr.get_adapter(session.adapter_name)
        if not adapter:
            logger.error(f"Adapter '{session.adapter_name}' not found")
            return

        # 只处理 QQ 适配器
        platform = getattr(adapter.info, "platform", "").lower()
        if platform != "qq":
            logger.debug(f"Skip typing for non-QQ adapter: {platform}")
            return

        client = adapter.get_client()
        if not client:
            logger.error("Adapter client not available")
            return

        params = {"user_id": int(session.session_id), "event_type": 1}

        if hasattr(client, "send_action") and callable(client.send_action):
            try:
                await client.send_action(self.action, params)
                logger.debug(f"Typing sent to {session.sid}")
                return
            except Exception as e:
                logger.debug(f"send_action failed: {e}")

        # 尝试 WebSocket 发送
        ws = getattr(client, "ws", None)
        if ws and hasattr(ws, "send"):
            payload = json.dumps({"action": self.action, "params": params})
            try:
                await ws.send(payload)
                logger.debug(f"Typing sent via WebSocket to {session.sid}")
                return
            except Exception as e:
                logger.debug(f"WebSocket send failed: {e}")

        # 尝试其他可能属性
        for attr in ["_ws", "_client", "websocket"]:
            ws_attr = getattr(client, attr, None)
            if ws_attr and hasattr(ws_attr, "send"):
                payload = json.dumps({"action": self.action, "params": params})
                try:
                    await ws_attr.send(payload)
                    logger.debug(f"Typing sent via {attr} to {session.sid}")
                    return
                except Exception:
                    continue

        logger.error("No working method to send typing indicator")

    async def _delayed_send_typing(self, session_obj: Session, delay: float):
        session = session_obj.sid
        try:
            await asyncio.sleep(delay)
            await self._send_typing(session_obj)
            if session not in self._loop_tasks or self._loop_tasks[session].done():
                self._typing_running[session] = True
                task = asyncio.create_task(self._typing_loop(session_obj))
                self._loop_tasks[session] = task
        except asyncio.CancelledError:
            logger.debug(f"Typing delayed task cancelled for {session}")

    async def _typing_loop(self, session_obj: Session):
        session = session_obj.sid
        while self._typing_running.get(session, False):
            try:
                await asyncio.sleep(self.typing_interval_seconds)
                if self._typing_running.get(session, False):
                    await self._send_typing(session_obj)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Typing loop error for {session}: {e}")
        logger.debug(f"Typing loop stopped for {session}")

    async def _typing_max_timeout(self, session_obj: Session, max_seconds: float):
        session = session_obj.sid
        try:
            await asyncio.sleep(max_seconds)
            self._stop_typing_loop(session_obj)
            logger.debug(f"Stopped typing for {session} due to max timeout ({max_seconds}s)")
        except asyncio.CancelledError:
            pass

    def _stop_typing_loop(self, session_obj: Session):
        session = session_obj.sid
        if session in self._typing_running:
            self._typing_running[session] = False
        if session in self._loop_tasks and not self._loop_tasks[session].done():
            self._loop_tasks[session].cancel()
        self._loop_tasks.pop(session, None)
        self._typing_running.pop(session, None)
        # 取消延迟任务，防止在 sleep 结束后启动新循环
        if session in self._delay_tasks and not self._delay_tasks[session].done():
            self._delay_tasks[session].cancel()
        self._delay_tasks.pop(session, None)
        # 取消最大时长兜底任务（若当前就在该任务内则只清理引用）
        max_task = self._max_tasks.pop(session, None)
        if max_task and not max_task.done() and max_task is not asyncio.current_task():
            max_task.cancel()

    def _start_typing_for_session(self, session_obj: Session):
        sid = session_obj.sid
        self._stop_typing_loop(session_obj)

        task = asyncio.create_task(
            self._delayed_send_typing(session_obj, self.typing_delay_seconds)
        )
        self._delay_tasks[sid] = task
        task.add_done_callback(lambda t: self._delay_tasks.pop(sid, None))

        if self.typing_max_seconds > 0:
            max_task = asyncio.create_task(
                self._typing_max_timeout(session_obj, self.typing_max_seconds)
            )
            self._max_tasks[sid] = max_task
            max_task.add_done_callback(lambda t: self._max_tasks.pop(sid, None))

    # 在 llm_request 阶段启动（Priority.LOW，晚于限流等插件），避免未真正请求 LLM 时假输入中
    @on.llm_request(priority=Priority.LOW)
    async def handle_typing_indication(self, event: KiraMessageBatchEvent, req: LLMRequest, *_):
        if event.adapter.platform != "QQ":
            return
        # 只处理私聊
        if event.is_group_message():
            return
        if event.is_stopped:
            return
        self._start_typing_for_session(event.session)

    @on.llm_response(priority=Priority.HIGH)
    async def on_llm_response(self, event: KiraMessageBatchEvent, resp: LLMResponse):
        if event.adapter.platform != "QQ":
            return
        # 只处理私聊
        if event.is_group_message():
            return
        sid = event.sid
        if not resp.tool_calls:
            self._stop_typing_loop(event.session)
            logger.debug(f"Stopped typing loop for {sid} due to final response (no tool calls)")
