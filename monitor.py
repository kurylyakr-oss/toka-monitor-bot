"""
Моніторинг зарядних станцій TOKA
TOKA #557 та TOKA #581 OKKO Gas Station Fast Charger
"""

import io
import sys
import requests
import sqlite3
import json
import time
import threading
from datetime import datetime, timezone
from pathlib import Path

# Примусово UTF-8 на Windows
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# ── Конфігурація ─────────────────────────────────────────────────────────────

STATIONS      = [628, 649]   # TOKA #557 і TOKA #581 OKKO Gas Station Fast Charger
API_BASE      = "https://toka.energy/map/stations"
DB_PATH       = Path(__file__).parent / "stations.db"
POLL_INTERVAL = 60           # секунд

TG_TOKEN   = "8076877842:AAG8cYMbxomPEOW5IqZ1urkYG0RkxAmnML8"
TG_CHAT_ID = 871290769
TG_API     = f"https://api.telegram.org/bot{TG_TOKEN}"

# Статуси, що означають "зайнято"
BUSY_STATUSES = {1, 3}

PORT_STATUS = {
    0: ("Вільний",       "✓"),
    1: ("Заряджає",      "⚡"),
    2: ("Очікування",    "○"),
    3: ("Підключений",   "🔌"),
    4: ("Невідомий",     "?"),
    5: ("Вільний|Швид.", "✓✓"),
}


def status_label(code) -> tuple[str, str]:
    try:
        code = int(code)
    except (TypeError, ValueError):
        return str(code), "?"
    return PORT_STATUS.get(code, (f"#{code}", "?"))


def fmt_duration(seconds: float) -> str:
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}с"
    m, s = divmod(seconds, 60)
    if m < 60:
        return f"{m}хв {s:02d}с"
    h, m = divmod(m, 60)
    return f"{h}год {m:02d}хв"


# ── Telegram ─────────────────────────────────────────────────────────────────

def tg_send(text: str):
    try:
        requests.post(
            f"{TG_API}/sendMessage",
            json={"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
    except Exception as e:
        print(f"  [TG] Помилка: {e}")


def tg_status_message() -> str:
    lines = ["<b>Поточний статус станцій</b>"]
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    for sid in STATIONS:
        # Завжди беремо живі дані з API
        data = fetch_station(sid)
        if not data:
            lines.append(f"\n#{sid}: не вдалось отримати дані")
            continue

        lines.append(f"\n<b>{data.get('name', sid)}</b>")

        for i, p in enumerate(data.get("ports", [])):
            label, icon = status_label(p.get("status"))
            title = p.get("title", f"Порт {i+1}")
            power = p.get("power", "?")

            cur_dur = ""
            if p.get("status") in BUSY_STATUSES:
                c.execute(
                    "SELECT started_at FROM sessions"
                    " WHERE station_id=? AND port_index=? AND ended_at IS NULL",
                    (sid, i),
                )
                r2 = c.fetchone()
                if r2:
                    elapsed = (datetime.now(timezone.utc) -
                               datetime.fromisoformat(r2[0])).total_seconds()
                    cur_dur = f" ({fmt_duration(elapsed)})"

            lines.append(f"  {icon} Порт {i+1} — {title} {power} кВт: {label}{cur_dur}")

    conn.close()
    return "\n".join(lines)


def tg_stats_message() -> str:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    lines = ["<b>Статистика сесій зарядки</b>"]

    for sid in STATIONS:
        c.execute("SELECT name FROM snapshots WHERE station_id=? LIMIT 1", (sid,))
        row = c.fetchone()
        name = row[0] if row else f"#{sid}"
        lines.append(f"\n<b>{name}</b>")

        c.execute("""
            SELECT DISTINCT port_index, port_title, power_kw
            FROM port_snapshots ps JOIN snapshots s ON s.id=ps.snapshot_id
            WHERE s.station_id=? ORDER BY port_index
        """, (sid,))
        ports = c.fetchall()

        for port_idx, port_title, power_kw in ports:
            lines.append(f"  Порт {port_idx+1} — {port_title or '?'} {int(power_kw or 0)} кВт")

            c.execute("""
                SELECT COUNT(*), AVG(duration_sec), MIN(duration_sec),
                       MAX(duration_sec), SUM(duration_sec)
                FROM sessions
                WHERE station_id=? AND port_index=? AND ended_at IS NOT NULL
            """, (sid, port_idx))
            cnt, avg_s, min_s, max_s, sum_s = c.fetchone()

            if cnt:
                lines.append(f"    Сесій: {cnt}")
                lines.append(f"    Сер: {fmt_duration(avg_s)}  Мін: {fmt_duration(min_s)}  Макс: {fmt_duration(max_s)}")
                lines.append(f"    Загалом: {fmt_duration(sum_s)}")
            else:
                lines.append("    Сесій ще немає")

            c.execute(
                "SELECT started_at FROM sessions"
                " WHERE station_id=? AND port_index=? AND ended_at IS NULL",
                (sid, port_idx),
            )
            open_row = c.fetchone()
            if open_row:
                elapsed = (datetime.now(timezone.utc) -
                           datetime.fromisoformat(open_row[0])).total_seconds()
                lines.append(f"    ⚡ Зараз заряджає: {fmt_duration(elapsed)}")

    conn.close()
    return "\n".join(lines)


def tg_period_message(label: str, date_from: str, date_to: str) -> str:
    """Статистика за період (date_from, date_to — ISO рядки UTC)."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    lines = [f"<b>{label}</b>"]

    for sid in STATIONS:
        c.execute("SELECT name FROM snapshots WHERE station_id=? LIMIT 1", (sid,))
        row = c.fetchone()
        name = row[0] if row else f"#{sid}"
        lines.append(f"\n<b>{name}</b>")

        c.execute("""
            SELECT DISTINCT port_index, port_title, power_kw
            FROM port_snapshots ps JOIN snapshots s ON s.id=ps.snapshot_id
            WHERE s.station_id=? ORDER BY port_index
        """, (sid,))
        ports = c.fetchall()

        for port_idx, port_title, power_kw in ports:
            lines.append(f"  Порт {port_idx+1} — {port_title or '?'} {int(power_kw or 0)} кВт")

            c.execute("""
                SELECT COUNT(*), AVG(duration_sec), MIN(duration_sec),
                       MAX(duration_sec), SUM(duration_sec)
                FROM sessions
                WHERE station_id=? AND port_index=?
                  AND ended_at IS NOT NULL
                  AND started_at >= ? AND started_at < ?
            """, (sid, port_idx, date_from, date_to))
            cnt, avg_s, min_s, max_s, sum_s = c.fetchone()

            if cnt:
                lines.append(f"    Сесій: {cnt}")
                lines.append(f"    Сер: {fmt_duration(avg_s)}  Мін: {fmt_duration(min_s)}  Макс: {fmt_duration(max_s)}")
                lines.append(f"    Загалом: {fmt_duration(sum_s)}")
            else:
                lines.append("    Сесій не було")

    conn.close()
    return "\n".join(lines)


def tg_today_message() -> str:
    now = datetime.now(timezone.utc)
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    day_end   = now.replace(hour=23, minute=59, second=59).isoformat()
    label = f"Статистика за сьогодні ({now.strftime('%d.%m.%Y')})"
    return tg_period_message(label, day_start, day_end)


def tg_month_message() -> str:
    now = datetime.now(timezone.utc)
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).isoformat()
    # перший день наступного місяця
    if now.month == 12:
        month_end = now.replace(year=now.year+1, month=1, day=1, hour=0, minute=0, second=0).isoformat()
    else:
        month_end = now.replace(month=now.month+1, day=1, hour=0, minute=0, second=0).isoformat()
    label = f"Статистика за {now.strftime('%B %Y')}"
    return tg_period_message(label, month_start, month_end)


def tg_poll_commands():
    """Фоновий потік: слухає команди бота."""
    # Пропустити всі старі повідомлення — відповідати тільки на нові
    try:
        r = requests.get(f"{TG_API}/getUpdates", params={"offset": -1}, timeout=10)
        updates = r.json().get("result", [])
        offset = updates[-1]["update_id"] + 1 if updates else None
    except Exception:
        offset = None

    print(f"  [TG] Polling запущено (offset={offset})")

    while True:
        try:
            params = {"timeout": 10, "allowed_updates": ["message"]}
            if offset:
                params["offset"] = offset
            r = requests.get(f"{TG_API}/getUpdates", params=params, timeout=20)
            updates = r.json().get("result", [])
            for upd in updates:
                offset = upd["update_id"] + 1
                text = upd.get("message", {}).get("text", "") or ""
                print(f"  [TG] Команда: {text!r}")
                if text.startswith("/status"):
                    tg_send(tg_status_message())
                elif text.startswith("/today"):
                    tg_send(tg_today_message())
                elif text.startswith("/month"):
                    tg_send(tg_month_message())
                elif text.startswith("/stats"):
                    tg_send(tg_stats_message())
                elif text.startswith("/help") or text.startswith("/start"):
                    tg_send(
                        "<b>Команди бота:</b>\n"
                        "/status — поточний стан станцій\n"
                        "/today  — статистика за сьогодні\n"
                        "/month  — статистика за місяць\n"
                        "/stats  — статистика за весь час"
                    )
        except Exception as e:
            print(f"  [TG poll] {e}")
            time.sleep(5)


# ── База даних ───────────────────────────────────────────────────────────────

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS snapshots (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            station_id  INTEGER NOT NULL,
            timestamp   TEXT    NOT NULL,
            name        TEXT,
            raw_json    TEXT
        );
        CREATE TABLE IF NOT EXISTS port_snapshots (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_id  INTEGER NOT NULL REFERENCES snapshots(id),
            port_index   INTEGER NOT NULL,
            port_title   TEXT,
            power_kw     REAL,
            price        TEXT,
            status_code  INTEGER
        );
        CREATE TABLE IF NOT EXISTS sessions (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            station_id   INTEGER NOT NULL,
            port_index   INTEGER NOT NULL,
            port_title   TEXT,
            power_kw     REAL,
            started_at   TEXT NOT NULL,
            ended_at     TEXT,
            duration_sec INTEGER
        );
        CREATE INDEX IF NOT EXISTS idx_station_time ON snapshots(station_id, timestamp);
        CREATE INDEX IF NOT EXISTS idx_sessions ON sessions(station_id, port_index, ended_at);
    """)
    conn.commit()
    conn.close()


def save_snapshot(station_id: int, data: dict, conn: sqlite3.Connection) -> int:
    c = conn.cursor()
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    c.execute(
        "INSERT INTO snapshots (station_id, timestamp, name, raw_json) VALUES (?,?,?,?)",
        (station_id, ts, data.get("name"), json.dumps(data, ensure_ascii=False)),
    )
    snap_id = c.lastrowid
    for i, port in enumerate(data.get("ports", [])):
        c.execute(
            "INSERT INTO port_snapshots (snapshot_id, port_index, port_title, power_kw, price, status_code)"
            " VALUES (?,?,?,?,?,?)",
            (snap_id, i, port.get("title"), port.get("power"),
             str(port.get("price", "")), port.get("status")),
        )
    return snap_id


def update_sessions(station_id: int, data: dict, conn: sqlite3.Connection):
    c = conn.cursor()
    now_ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    name = data.get("name", f"#{station_id}")

    for i, port in enumerate(data.get("ports", [])):
        code = port.get("status")
        is_busy = code in BUSY_STATUSES
        title = port.get("title", f"Порт {i+1}")
        power = port.get("power", "?")

        c.execute(
            "SELECT id, started_at FROM sessions"
            " WHERE station_id=? AND port_index=? AND ended_at IS NULL",
            (station_id, i),
        )
        open_session = c.fetchone()

        if is_busy and not open_session:
            c.execute(
                "INSERT INTO sessions (station_id, port_index, port_title, power_kw, started_at)"
                " VALUES (?,?,?,?,?)",
                (station_id, i, title, port.get("power"), now_ts),
            )
            label, icon = status_label(code)
            msg = (f"{icon} <b>Зарядка розпочата</b>\n"
                   f"{name}\n"
                   f"Порт {i+1} — {title} {power} кВт")
            tg_send(msg)
            print(f"  ⚡ Сесія розпочата: {title}")

        elif not is_busy and open_session:
            sess_id, started_at = open_session
            start_dt = datetime.fromisoformat(started_at)
            end_dt   = datetime.fromisoformat(now_ts)
            duration = int((end_dt - start_dt).total_seconds())
            c.execute(
                "UPDATE sessions SET ended_at=?, duration_sec=? WHERE id=?",
                (now_ts, duration, sess_id),
            )
            msg = (f"✅ <b>Зарядка завершена</b>\n"
                   f"{name}\n"
                   f"Порт {i+1} — {title} {power} кВт\n"
                   f"Тривалість: <b>{fmt_duration(duration)}</b>")
            tg_send(msg)
            print(f"  ✓ Сесія завершена: {title} ({fmt_duration(duration)})")


# ── API ──────────────────────────────────────────────────────────────────────

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "uk-UA,uk;q=0.9,en;q=0.8",
    "Referer": "https://toka.energy/mapa",
    "Origin": "https://toka.energy",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
}


def fetch_station(station_id: int) -> dict | None:
    url = f"{API_BASE}/{station_id}"
    try:
        r = requests.get(url, timeout=15, headers=HEADERS)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        print(f"  [!] Помилка отримання станції {station_id}: {e}")
        return None


# ── Виведення в консоль ───────────────────────────────────────────────────────

def print_station(data: dict, station_id: int, conn: sqlite3.Connection):
    name  = (data.get("name") or f"#{station_id}")[:36]
    addr  = (data.get("address") or "—")[:46]
    ports = data.get("ports", [])
    c     = conn.cursor()

    print()
    print(f"  +------------------------------------------------------------------+")
    print(f"  | #{station_id}  {name:<56} |")
    print(f"  | {addr:<68} |")
    print(f"  +----+-----------+----------+----------+------------------+----------+")
    print(f"  | №  | {'Тип':<9} | {'Потужн.':<8} | {'Ціна':<8} | {'Статус':<16} | {'Поточна':<8} |")
    print(f"  +----+-----------+----------+----------+------------------+----------+")

    for i, p in enumerate(ports):
        title  = (p.get("title") or "")[:9]
        power  = f"{p.get('power', '?')} кВт"
        price  = str(p.get("price", "?"))[:8]
        code   = p.get("status")
        label, icon = status_label(code)
        status_str  = f"{icon} {label}"[:16]

        cur_dur = ""
        if code in BUSY_STATUSES:
            c.execute(
                "SELECT started_at FROM sessions"
                " WHERE station_id=? AND port_index=? AND ended_at IS NULL",
                (station_id, i),
            )
            row = c.fetchone()
            if row:
                elapsed = (datetime.now(timezone.utc) -
                           datetime.fromisoformat(row[0])).total_seconds()
                cur_dur = fmt_duration(elapsed)

        print(f"  | {i+1:<2} | {title:<9} | {power:<8} | {price:<8} | {status_str:<16} | {cur_dur:<8} |")

    print(f"  +----+-----------+----------+----------+------------------+----------+")


# ── Головний цикл ────────────────────────────────────────────────────────────

def poll_once():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n{'='*70}")
    print(f"  Оновлення: {now}")
    print(f"{'='*70}")

    conn = sqlite3.connect(DB_PATH)
    try:
        for sid in STATIONS:
            data = fetch_station(sid)
            if data:
                save_snapshot(sid, data, conn)
                update_sessions(sid, data, conn)
                print_station(data, sid, conn)
            else:
                print(f"\n  [!] Станцію #{sid} не вдалось отримати")
        conn.commit()
    finally:
        conn.close()


def print_stats():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    print()
    print("=" * 65)
    print("  СТАТИСТИКА")
    print("=" * 65)

    for sid in STATIONS:
        c.execute("SELECT COUNT(*), MIN(timestamp), MAX(timestamp) FROM snapshots WHERE station_id=?", (sid,))
        total, first, last = c.fetchone()
        if not total:
            print(f"  #{sid}: даних ще немає")
            continue

        c.execute("SELECT name FROM snapshots WHERE station_id=? LIMIT 1", (sid,))
        name = (c.fetchone() or ("?",))[0]
        print(f"\n  {name}  (опитувань: {total})")
        print(f"  {first} → {last}")

        c.execute("""
            SELECT DISTINCT port_index, port_title, power_kw
            FROM port_snapshots ps JOIN snapshots s ON s.id=ps.snapshot_id
            WHERE s.station_id=? ORDER BY port_index
        """, (sid,))

        for port_idx, port_title, power_kw in c.fetchall():
            print(f"\n    Порт {port_idx+1} — {port_title or '?'} {int(power_kw or 0)} кВт")

            c.execute("""
                SELECT ps.status_code, COUNT(*) AS cnt
                FROM port_snapshots ps JOIN snapshots s ON s.id=ps.snapshot_id
                WHERE s.station_id=? AND ps.port_index=?
                GROUP BY ps.status_code ORDER BY cnt DESC
            """, (sid, port_idx))
            for code, cnt in c.fetchall():
                label, icon = status_label(code)
                pct = cnt / total * 100
                print(f"      {icon} {label:<14} {cnt:>5}x  {pct:>5.1f}%")

            c.execute("""
                SELECT COUNT(*), AVG(duration_sec), MIN(duration_sec),
                       MAX(duration_sec), SUM(duration_sec)
                FROM sessions WHERE station_id=? AND port_index=? AND ended_at IS NOT NULL
            """, (sid, port_idx))
            cnt_s, avg_s, min_s, max_s, sum_s = c.fetchone()
            if cnt_s:
                print(f"      Сесій: {cnt_s}  |  Сер: {fmt_duration(avg_s)}  Мін: {fmt_duration(min_s)}  Макс: {fmt_duration(max_s)}")
                print(f"      Загалом заряджав: {fmt_duration(sum_s)}")

            c.execute(
                "SELECT started_at FROM sessions WHERE station_id=? AND port_index=? AND ended_at IS NULL",
                (sid, port_idx),
            )
            open_row = c.fetchone()
            if open_row:
                elapsed = (datetime.now(timezone.utc) - datetime.fromisoformat(open_row[0])).total_seconds()
                print(f"      ⚡ Зараз заряджає: {fmt_duration(elapsed)}")

    conn.close()
    print()


def main():
    init_db()

    mode = sys.argv[1] if len(sys.argv) > 1 else "loop"

    if mode == "stats":
        print_stats()
        return

    if mode == "once":
        poll_once()
        return

    print()
    print("  Моніторинг зарядних станцій TOKA")
    print(f"  TOKA #557 (ID 628) та TOKA #581 (ID 649)")
    print(f"  Інтервал: {POLL_INTERVAL} с | DB: {DB_PATH.name}")
    print(f"  Telegram: @toka_monitor_bot")
    print()
    print("  Команди бота: /status  /stats")
    print("  Ctrl+C для зупинки")

    # Запуск Telegram polling у фоновому потоці
    t = threading.Thread(target=tg_poll_commands, daemon=True)
    t.start()

    tg_send(
        "✅ Моніторинг запущено\n"
        "/status — стан станцій\n"
        "/today  — статистика за сьогодні\n"
        "/month  — статистика за місяць\n"
        "/stats  — статистика за весь час"
    )

    try:
        while True:
            poll_once()
            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        print("\n\n  Зупинено.")
        tg_send("🛑 Моніторинг зупинено")
        print_stats()


if __name__ == "__main__":
    main()
