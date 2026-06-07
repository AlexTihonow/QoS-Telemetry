import asyncio
import json
import logging
import threading
import time

import websockets
from websockets.server import WebSocketServerProtocol

logger = logging.getLogger(__name__)

class WebSocketBridge:

    def __init__(self, host: str = '0.0.0.0', port: int = 8765):
        self.host = host
        self.port = port

        self._client_queues: dict[WebSocketServerProtocol, asyncio.Queue] = {}
        self._clients_lock = threading.Lock()

        self._loop: asyncio.AbstractEventLoop | None = None
        self._last_payload: str | None = None
        self._thread: threading.Thread | None = None

    def start(self):
        ready = threading.Event()
        self._thread = threading.Thread(
            target=self._run_loop, args=(ready,), daemon=True, name="ws-bridge"
        )
        self._thread.start()
        ready.wait(timeout=5.0)
        if self._loop and self._loop.is_running():
            logger.info("WebSocket bridge → ws://%s:%d", self.host, self.port)
        else:
            logger.error("WebSocket bridge не запустился!")

    def _run_loop(self, ready: threading.Event):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._serve(ready))

    async def _serve(self, ready: threading.Event):
        async with websockets.serve(
            self._handler,
            self.host,
            self.port,
            ping_interval=20,
            ping_timeout=10,
        ):
            ready.set()
            await asyncio.Future()

    async def _handler(self, ws: WebSocketServerProtocol, path: str = '/'):
        queue: asyncio.Queue = asyncio.Queue(maxsize=32)

        with self._clients_lock:
            self._client_queues[ws] = queue

        peer = ws.remote_address
        logger.info("WS ↑ подключён %s:%d  (всего: %d)",
                    *peer, len(self._client_queues))

        try:
            if self._last_payload:
                await ws.send(self._last_payload)

            while True:
                try:
                    payload = await asyncio.wait_for(queue.get(), timeout=30.0)
                    await ws.send(payload)
                except asyncio.TimeoutError:
                    await ws.ping()
                except websockets.exceptions.ConnectionClosed:
                    break

        except websockets.exceptions.ConnectionClosed:
            pass
        except Exception as e:
            logger.warning("WS handler error: %s", e)
        finally:
            with self._clients_lock:
                self._client_queues.pop(ws, None)
            logger.info("WS ↓ отключён %s:%d  (всего: %d)",
                        *peer, len(self._client_queues))

    def on_data(self, snapshot: dict):
        
        if self._loop is None or not self._loop.is_running():
            return

        payload = json.dumps({
            "type":  "update",
            "ts":    snapshot.get("ts", time.time()),
            "nodes": snapshot.get("nodes", []),
        })
        self._last_payload = payload

        self._loop.call_soon_threadsafe(self._broadcast, payload)

    def _broadcast(self, payload: str):
        
        with self._clients_lock:
            queues = list(self._client_queues.values())

        dropped = 0
        for q in queues:
            try:
                q.put_nowait(payload)
            except asyncio.QueueFull:
                dropped += 1

        if queues:
            logger.debug("broadcast → %d клиентов (dropped=%d)", len(queues), dropped)