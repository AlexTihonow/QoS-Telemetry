import asyncio
import logging
import signal
import sys
import threading
import time
import argparse

from icmp_engine import ICMPDispatcher, PingAggregator, PingResult
from tcp_server import ThreadedTCPServer, TCPPublisher, DataBus
from ws_bridge import WebSocketBridge

TCP_HOST = '127.0.0.1'
TCP_PORT = 9877
WS_PORT  = 8765

NODES = [
    ("Gorbatka",        "gorbatka.ru",      "ru"),   # Горбатка ру
    ("Yandex-2",        "77.88.8.1",        "ru"),   # Яндекс.DNS safe
    ("Yandex-3",        "77.88.8.88",       "ru"),   # Яндекс.DNS secondary
    ("Yandex-4",        "77.88.8.2",        "ru"),   # Яндекс.DNS family

    ("SkyDNS-1",        "193.58.251.251",   "ru"),   # SkyDNS Москва
    ("SkyDNS-2",        "193.58.251.252",   "ru"),   # SkyDNS secondary

    ("RIPN",            "193.232.128.6",    "ru"),   # РИПН Москва

    ("Google-8888",     "8.8.8.8",          "eu"),   # Google DNS primary
    ("Google-8844",     "8.8.4.4",          "eu"),   # Google DNS secondary

    ("CF-1111",         "1.1.1.1",          "eu"),   # Cloudflare primary
    ("CF-1001",         "1.0.0.1",          "eu"),   # Cloudflare secondary

    ("Quad9-9999",      "9.9.9.9",          "eu"),   # Quad9 primary
    ("Quad9-149",       "149.112.112.112",  "eu"),   # Quad9 secondary

    ("OpenDNS-1",       "208.67.222.222",   "eu"),   # OpenDNS primary
    ("OpenDNS-2",       "208.67.220.220",   "eu"),   # OpenDNS secondary

    ("Level3-421",      "4.2.2.1",          "na"),   # Level3 Dallas
    ("Level3-422",      "4.2.2.2",          "na"),   # Level3 Dallas
    ("Level3-426",      "4.2.2.6",          "na"),   # Level3 Dallas

    ("Root-A",          "198.41.0.4",       "na"),   # a.root-servers.net Verisign VA
    ("Root-K",          "193.0.14.129",     "eu"),   # k.root-servers.net RIPE Amsterdam
    ("Root-M",          "202.12.27.33",     "apac"), # m.root-servers.net WIDE Tokyo
    ("Root-F",          "192.5.5.241",      "na"),   # f.root-servers.net ISC
    ("Root-L",          "199.7.83.42",      "na"),   # l.root-servers.net IANA
    ("Root-I",          "192.36.148.17",    "eu"),   # i.root-servers.net Netnod Stockholm
    ("Root-E",          "192.203.230.10",   "na"),   # e.root-servers.net NASA
]

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(threadName)s] %(levelname)s: %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger("server")

async def ping_worker(dispatcher: ICMPDispatcher,
                      publisher: TCPPublisher,
                      interval: float = 5.0):
    aggregator = PingAggregator()
    loop = asyncio.get_running_loop()

    dispatcher.start(loop)
    logger.info("Ping worker запущен (%d узлов, интервал=%.1fs)", len(NODES), interval)

    while True:
        t0 = time.time()

        tasks   = [dispatcher.ping(host, nid) for nid, host, _ in NODES]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        ok, fail = 0, 0
        for r in results:
            if isinstance(r, PingResult):
                aggregator.add(r)
                if r.success:
                    ok += 1
                else:
                    fail += 1
                    logger.debug("  ✗ %s → %s (%s)", r.node_id, r.host, r.error)

        stats = []
        for nid, host, region in NODES:
            s = aggregator.stats(nid)
            if s:
                s["region"] = region
                s["host"]   = host
                stats.append(s)

        if stats:
            publisher.send(stats)
            valid = [s["latency_p95"] for s in stats if s.get("latency_p95")]
            avg   = sum(valid) / len(valid) if valid else 0.0
            logger.info("Раунд: %d✓ %d✗ | avg p95=%.1f ms", ok, fail, avg)

        elapsed = time.time() - t0
        await asyncio.sleep(max(0.1, interval - elapsed))

def main():
    parser = argparse.ArgumentParser(description="QoS Telemetry — SOCK_RAW ICMP")
    parser.add_argument('--ws-port',  type=int,   default=WS_PORT)
    parser.add_argument('--tcp-port', type=int,   default=TCP_PORT)
    parser.add_argument('--interval', type=float, default=5.0,
                        help='Интервал между раундами (сек)')
    parser.add_argument('--timeout',  type=float, default=2.0,
                        help='Таймаут одного пинга (сек)')
    parser.add_argument('--debug', action='store_true',
                        help='Показывать детали по каждому узлу')
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    dispatcher = ICMPDispatcher(timeout=args.timeout)
    try:
        dispatcher.open()
    except PermissionError as e:
        print(f"\n  ✗ {e}\n")
        sys.exit(1)

    bus = DataBus()

    tcp_srv = ThreadedTCPServer(TCP_HOST, args.tcp_port, bus)
    threading.Thread(target=tcp_srv.serve_forever,
                     daemon=True, name="tcp-srv").start()

    ws_bridge = WebSocketBridge('0.0.0.0', args.ws_port)
    ws_bridge.start()
    bus.subscribe(ws_bridge.on_data)

    publisher = TCPPublisher(TCP_HOST, args.tcp_port)

    def shutdown(sig, frame):
        logger.info("Завершение...")
        dispatcher.close()
        publisher.close()
        tcp_srv.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGINT,  shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    print(f)

    asyncio.run(ping_worker(dispatcher, publisher, interval=args.interval))

if __name__ == '__main__':
    main()