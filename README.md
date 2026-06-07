# QoS-Telemetry
Система мониторинга качества сети на Python — пингует серверы через необработанные ICMP-сокеты, собирает метрики (задержка p95, джиттер, потери пакетов) и показывает всё в браузере в реальном времени через WebSocket. Архитектура: asyncio + SOCK_RAW + многопоточный TCP-сервер + браузерный дашборд.

# QoS.Telemetry

> Мониторинг качества сети в реальном времени — необработанные ICMP-сокеты, asyncio, WebSocket, браузерный дашборд.

![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python&logoColor=white)
![asyncio](https://img.shields.io/badge/asyncio-built--in-green)
![WebSocket](https://img.shields.io/badge/WebSocket-websockets%2016-purple)
![License](https://img.shields.io/badge/license-MIT-orange)
![Platform](https://img.shields.io/badge/platform-Linux-lightgrey?logo=linux)

---

## Что это

Программа каждые 5 секунд пингует список серверов через ICMP и показывает результаты в браузере в реальном времени. Никаких баз данных, никаких внешних сервисов — один Python-скрипт и браузер.

**Измеряемые метрики:**
- **Latency p95** — 95-й процентиль задержки (мс)
- **Jitter** — нестабильность задержки (мс)
- **Packet loss** — процент потерянных пакетов (%)
- **Last RTT** — последнее измерение

---

## Как устроено

```
SOCK_RAW (один сокет)
    └── asyncio.gather()        ← все узлы параллельно
            └── PingAggregator  ← p50 / p95 / jitter / loss
                    └── TCPPublisher
                            └── ThreadedTCPServer  ← отдельный поток
                                    └── DataBus    ← pub/sub, thread-safe
                                            └── WebSocketBridge
                                                    └── браузер (dashboard.html)
```

**Файлы:**

| Файл | Что делает |
|------|-----------|
| `icmp_engine.py` | Необработанный ICMP-сокет + asyncio диспетчер + агрегатор метрик |
| `server.py` | Точка входа, список узлов, ping-воркер |
| `tcp_server.py` | Многопоточный TCP-сервер, DataBus, TCPPublisher |
| `ws_bridge.py` | WebSocket-мост (per-client очереди, call_soon_threadsafe) |
| `dashboard.html` | Браузерный дашборд, подключается по WebSocket |

---

## Быстрый старт

### Требования

- Python 3.10+
- Linux (необработанные ICMP-сокеты требуют `sudo`)
- `pip install websockets`

### Запуск

```bash
git clone https://github.com/yourname/qos-telemetry
cd qos-telemetry
pip install websockets

sudo python server.py
```

Открой `dashboard.html` в браузере — данные появятся через 5 секунд.

### Опции

```bash
sudo python server.py --interval 10   # интервал пинга в секундах
sudo python server.py --timeout 2.0   # таймаут одного пинга
sudo python server.py --debug         # подробные логи по каждому узлу
sudo python server.py --ws-port 8765  # порт WebSocket (по умолчанию 8765)
```

---

## Узлы мониторинга

По умолчанию в `server.py` настроены публичные DNS-серверы — они гарантированно отвечают на ICMP:

```python
NODES = [
    ("Yandex-1",    "77.88.8.8",       "ru"),   # Яндекс.DNS
    ("CF-1111",     "1.1.1.1",         "eu"),   # Cloudflare
    ("Google-8888", "8.8.8.8",         "eu"),   # Google DNS
    ("Root-K",      "193.0.14.129",    "eu"),   # k.root-servers.net
    ("Root-M",      "202.12.27.33",    "apac"), # m.root-servers.net Tokyo
    # ...
]
```

Чтобы добавить свой сервер — просто допиши строку в список:

```python
("My-Server", "192.168.1.1", "eu"),
```

Формат: `("имя", "ip или hostname", "регион")`. Регионы: `ru`, `eu`, `na`, `apac`, `other`.

> ⚠️ **Важно:** большинство коммерческих серверов (Amazon, Microsoft и др.) блокируют ICMP. Для них лучше использовать TCP-мониторинг.

---

## Известные ограничения

| Ситуация | Поведение |
|----------|-----------|
| VPN включён | Все узлы показывают 100% loss — VPN перехватывает ICMP |
| macOS | Система блокирует `add_reader` для ICMP-сокетов, используйте Linux |
| Без `sudo` | `PermissionError` — необработанные сокеты требуют CAP_NET_RAW |
| Сервер блокирует ICMP | 100% loss, это нормально для AWS/Azure/GCP |

---

## Аналоги в production

Та же архитектура используется в:

- **[Smokeping](https://oss.oetiker.ch/smokeping/)** — CERN, стандарт у ISP
- **[Zabbix](https://www.zabbix.com/)** — enterprise-мониторинг (Ростелеком, МТС)
- **[RIPE Atlas](https://atlas.ripe.net/)** — 12 000 зондов по всему миру
- **[Nagios](https://www.nagios.org/)** — классика NOC-мониторинга

Разница — только в масштабе. Архитектурно то же самое.

---

## Стек

- `socket.SOCK_RAW` + `IPPROTO_ICMP` — отправка и приём ICMP-пакетов
- `asyncio` + `loop.add_reader()` — неблокирующее чтение сокета
- `asyncio.gather()` — параллельный опрос всех узлов
- `socketserver.ThreadingTCPServer` — многопоточный TCP-сервер
- `threading.Lock` — потокобезопасный DataBus
- `websockets` — WebSocket-сервер для браузера
- `call_soon_threadsafe()` — передача данных между потоками и asyncio

---

## Лицензия

MIT — делай что хочешь, ссылка на автора приветствуется.
