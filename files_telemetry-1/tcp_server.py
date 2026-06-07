import json
import logging
import socket
import socketserver
import threading
import time
from typing import Callable

logger = logging.getLogger(__name__)

class DataBus:
    

    def __init__(self):
        self._lock      = threading.Lock()
        self._snapshot  : dict          = {}
        self._listeners : list[Callable] = []
        self._last_update: float        = 0.0
        self._update_count: int         = 0

    def publish(self, stats_list: list[dict]):
        with self._lock:
            for s in stats_list:
                nid = s.get("node_id")
                if nid:
                    self._snapshot[nid] = s
            self._last_update  = time.time()
            self._update_count += 1
            count = self._update_count

        logger.info("DataBus publish #%d  (%d узлов)", count, len(stats_list))

        payload = self.snapshot()
        for cb in list(self._listeners):
            try:
                cb(payload)
            except Exception as e:
                logger.warning("DataBus listener error: %s", e)

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "ts":    self._last_update,
                "nodes": list(self._snapshot.values()),
            }

    def subscribe(self, cb: Callable):
        self._listeners.append(cb)
        logger.info("DataBus: новый подписчик (%d всего)", len(self._listeners))

    def unsubscribe(self, cb: Callable):
        try:
            self._listeners.remove(cb)
        except ValueError:
            pass

class PingDataHandler(socketserver.BaseRequestHandler):
    

    def handle(self):
        peer = self.client_address
        logger.info("TCP ↑ %s:%d", *peer)
        try:
            while True:
                raw_len = self._recvall(4)
                if not raw_len:
                    break
                length = int.from_bytes(raw_len, 'big')
                if length == 0 or length > 2_000_000:
                    logger.warning("Некорректная длина пакета: %d", length)
                    break

                raw_body = self._recvall(length)
                if not raw_body:
                    break

                try:
                    data = json.loads(raw_body.decode('utf-8'))
                except json.JSONDecodeError as e:
                    logger.error("JSON error: %s", e)
                    continue

                if isinstance(data, list):
                    self.server.bus.publish(data)

        except (ConnectionResetError, BrokenPipeError, OSError):
            pass
        finally:
            logger.info("TCP ↓ %s:%d", *peer)

    def _recvall(self, n: int) -> bytes:
        buf = b''
        while len(buf) < n:
            try:
                chunk = self.request.recv(n - len(buf))
            except OSError:
                return b''
            if not chunk:
                return b''
            buf += chunk
        return buf

class ThreadedTCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads      = True

    def __init__(self, host: str, port: int, bus: DataBus):
        self.bus = bus
        super().__init__((host, port), PingDataHandler)
        logger.info("ThreadedTCPServer слушает %s:%d", host, port)

class TCPPublisher:
    

    def __init__(self, host: str = '127.0.0.1', port: int = 9877):
        self.host   = host
        self.port   = port
        self._sock  = None
        self._lock  = threading.Lock()

    def _connect(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5.0)
        s.connect((self.host, self.port))
        s.settimeout(None)
        self._sock = s
        logger.info("TCPPublisher подключён → %s:%d", self.host, self.port)

    def send(self, stats_list: list[dict]):
        payload = json.dumps(stats_list).encode('utf-8')
        frame   = len(payload).to_bytes(4, 'big') + payload

        with self._lock:
            for attempt in range(4):
                try:
                    if self._sock is None:
                        self._connect()
                    self._sock.sendall(frame)
                    logger.debug("TCPPublisher отправил %d байт (%d узлов)",
                                 len(frame), len(stats_list))
                    return
                except (ConnectionRefusedError, BrokenPipeError, OSError) as e:
                    logger.warning("TCP send fail #%d: %s", attempt + 1, e)
                    try:
                        self._sock.close()
                    except Exception:
                        pass
                    self._sock = None
                    time.sleep(0.3 * (attempt + 1))
            logger.error("TCPPublisher: не удалось отправить после 4 попыток")

    def close(self):
        with self._lock:
            if self._sock:
                self._sock.close()
                self._sock = None