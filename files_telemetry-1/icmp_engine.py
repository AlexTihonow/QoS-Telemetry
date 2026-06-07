import asyncio
import os
import socket
import struct
import time
import logging
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)

@dataclass
class PingResult:
    host:      str
    node_id:   str
    rtt_ms:    Optional[float]
    timestamp: float = field(default_factory=time.time)
    success:   bool  = False
    error:     Optional[str] = None

def _checksum(data: bytes) -> int:
    
    if len(data) % 2:
        data += b'\x00'
    total = 0
    for i in range(0, len(data), 2):
        total += (data[i] << 8) + data[i + 1]
    total = (total >> 16) + (total & 0xFFFF)
    total += (total >> 16)
    return ~total & 0xFFFF

def _build_packet(seq: int, identifier: int, send_time: float) -> bytes:
    
    payload = struct.pack('d', send_time) + b'QoSTelemetry'
    header  = struct.pack('!BBHHH', 8, 0, 0, identifier, seq)
    csum    = _checksum(header + payload)
    header  = struct.pack('!BBHHH', 8, 0, csum, identifier, seq)
    return header + payload

def _parse_reply(data: bytes, identifier: int, seq: int) -> Optional[float]:
    
    if len(data) < 28:
        return None

    ip_ihl  = (data[0] & 0x0F) * 4
    icmp    = data[ip_ihl:]

    if len(icmp) < 16:
        return None

    itype, _, _, rid, rseq = struct.unpack('!BBHHH', icmp[:8])

    if itype != 0 or rid != identifier or rseq != seq:
        return None

    send_time = struct.unpack('d', icmp[8:16])[0]
    return (time.time() - send_time) * 1000

class ICMPDispatcher:
    

    def __init__(self, timeout: float = 2.0):
        self.timeout     = timeout
        self._sock       = None
        self._loop       = None
        self._identifier = os.getpid() & 0xFFFF
        self._seq        = 0
        self._waiters: dict[tuple, asyncio.Future] = {}

    def open(self):
        
        try:
            self._sock = socket.socket(
                socket.AF_INET,
                socket.SOCK_RAW,
                socket.IPPROTO_ICMP
            )
            self._sock.setblocking(False)
            logger.info(
                "SOCK_RAW открыт (AF_INET / IPPROTO_ICMP, id=%d)",
                self._identifier
            )
        except PermissionError:
            raise PermissionError(
                "SOCK_RAW требует root. Запускайте: sudo python server.py"
            )

    def start(self, loop: asyncio.AbstractEventLoop):
        
        self._loop = loop
        loop.add_reader(self._sock.fileno(), self._on_readable)
        logger.info("ICMP dispatcher запущен (loop.add_reader fd=%d)", self._sock.fileno())

    def _on_readable(self):
        
        while True:
            try:
                data, addr = self._sock.recvfrom(1024)
            except BlockingIOError:
                break   # больше нет пакетов
            except OSError as e:
                logger.warning("recvfrom error: %s", e)
                break

            if len(data) < 28:
                continue

            ip_ihl = (data[0] & 0x0F) * 4
            icmp   = data[ip_ihl:]

            if len(icmp) < 8:
                continue

            itype, _, _, rid, rseq = struct.unpack('!BBHHH', icmp[:8])

            if itype != 0 or rid != self._identifier:
                continue

            key = (rid, rseq)
            fut = self._waiters.get(key)
            if fut and not fut.done():
                fut.set_result(data)
                logger.debug("ICMP reply: seq=%d from %s", rseq, addr[0])

    async def ping(self, host: str, node_id: str,
                   loop: asyncio.AbstractEventLoop = None) -> PingResult:
        
        try:
            addr = socket.gethostbyname(host)
        except socket.gaierror as e:
            return PingResult(host=host, node_id=node_id,
                              rtt_ms=None, error=f"dns:{e}")

        self._seq = (self._seq + 1) & 0xFFFF
        seq       = self._seq

        send_time = time.time()
        packet    = _build_packet(seq, self._identifier, send_time)

        key = (self._identifier, seq)
        fut = self._loop.create_future()
        self._waiters[key] = fut

        try:
            self._sock.sendto(packet, (addr, 0))
        except OSError as e:
            self._waiters.pop(key, None)
            return PingResult(host=host, node_id=node_id,
                              rtt_ms=None, error=f"send:{e}")

        try:
            data = await asyncio.wait_for(fut, timeout=self.timeout)
            rtt  = _parse_reply(data, self._identifier, seq)
            if rtt is not None:
                return PingResult(host=host, node_id=node_id,
                                  rtt_ms=round(rtt, 3), success=True)
            return PingResult(host=host, node_id=node_id,
                              rtt_ms=None, error="parse_fail")
        except asyncio.TimeoutError:
            return PingResult(host=host, node_id=node_id,
                              rtt_ms=None, error="timeout")
        finally:
            self._waiters.pop(key, None)

    def close(self):
        if self._sock and self._loop:
            try:
                self._loop.remove_reader(self._sock.fileno())
            except Exception:
                pass
        if self._sock:
            self._sock.close()
            self._sock = None
            logger.info("SOCK_RAW закрыт")

AsyncICMPPinger = ICMPDispatcher

class PingAggregator:
    
    WINDOW = 60

    def __init__(self):
        self._history: dict[str, list[PingResult]] = {}

    def add(self, result: PingResult):
        h = self._history.setdefault(result.node_id, [])
        h.append(result)
        if len(h) > self.WINDOW:
            h.pop(0)

    def stats(self, node_id: str) -> dict:
        history = self._history.get(node_id, [])
        if not history:
            return {}

        total = len(history)
        rtts  = [r.rtt_ms for r in history if r.success and r.rtt_ms is not None]
        lost  = total - len(rtts)

        if not rtts:
            return dict(node_id=node_id, latency_p50=None, latency_p95=None,
                        jitter=None, loss_pct=100.0, last_rtt=None, samples=total)

        s   = sorted(rtts)
        n   = len(s)
        p50 = s[int(n * 0.50)]
        p95 = s[min(int(n * 0.95), n - 1)]

        diffs  = [abs(rtts[i] - rtts[i-1]) for i in range(1, len(rtts))]
        jitter = round(sum(diffs) / len(diffs), 3) if diffs else 0.0

        return dict(
            node_id     = node_id,
            latency_p50 = round(p50, 2),
            latency_p95 = round(p95, 2),
            jitter      = jitter,
            loss_pct    = round(lost / total * 100, 3),
            last_rtt    = round(rtts[-1], 2),
            samples     = total,
        )