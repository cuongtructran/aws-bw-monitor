"""SQLite cache for parsed access log aggregates."""
from __future__ import annotations

import sqlite3
import threading
from contextlib import closing
from pathlib import Path

_DB_PATH = Path.home() / ".aws-bw-monitor" / "cache.db"
_lock = threading.Lock()


def _ensure_dir() -> None:
    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)


def _conn() -> sqlite3.Connection:
    _ensure_dir()
    c = sqlite3.connect(_DB_PATH, timeout=30.0, isolation_level=None)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA synchronous=NORMAL")
    return c


def init_db() -> None:
    with _lock, closing(_conn()) as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS bw_minute (
            lb_arn TEXT NOT NULL,
            listener_port INTEGER NOT NULL,
            minute_ts INTEGER NOT NULL,
            bytes INTEGER NOT NULL,
            requests INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (lb_arn, listener_port, minute_ts)
        );
        CREATE INDEX IF NOT EXISTS idx_bw_ts ON bw_minute(lb_arn, minute_ts);

        CREATE TABLE IF NOT EXISTS client_traffic (
            lb_arn TEXT NOT NULL,
            client_ip TEXT NOT NULL,
            listener_port INTEGER NOT NULL,
            minute_ts INTEGER NOT NULL,
            bytes INTEGER NOT NULL,
            requests INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (lb_arn, client_ip, listener_port, minute_ts)
        );
        CREATE INDEX IF NOT EXISTS idx_client_ts ON client_traffic(lb_arn, minute_ts);

        CREATE TABLE IF NOT EXISTS parsed_files (
            s3_bucket TEXT NOT NULL,
            s3_key TEXT NOT NULL,
            parsed_at INTEGER NOT NULL,
            PRIMARY KEY (s3_bucket, s3_key)
        );
        """)


def is_parsed(bucket: str, key: str) -> bool:
    with _lock, closing(_conn()) as c:
        r = c.execute(
            "SELECT 1 FROM parsed_files WHERE s3_bucket=? AND s3_key=?",
            (bucket, key),
        ).fetchone()
        return r is not None


def insert_aggregates(
    lb_arn: str,
    rows: dict[tuple[int, int], tuple[int, int]],
    client_rows: dict[tuple[str, int, int], tuple[int, int]],
    s3_bucket: str,
    s3_key: str,
    parsed_at: int,
) -> None:
    """Atomically upsert per-(port, minute) aggregates and mark file parsed.

    rows: { (listener_port, minute_ts): (bytes_sum, request_count) }
    client_rows: { (client_ip, listener_port, minute_ts): (bytes_sum, request_count) }
    """
    with _lock, closing(_conn()) as c:
        c.execute("BEGIN")
        try:
            c.executemany(
                """
                INSERT INTO bw_minute (lb_arn, listener_port, minute_ts, bytes, requests)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(lb_arn, listener_port, minute_ts)
                DO UPDATE SET bytes = bytes + excluded.bytes,
                              requests = requests + excluded.requests
                """,
                [
                    (lb_arn, port, ts, b, r)
                    for (port, ts), (b, r) in rows.items()
                ],
            )
            c.executemany(
                """
                INSERT INTO client_traffic (lb_arn, client_ip, listener_port, minute_ts, bytes, requests)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(lb_arn, client_ip, listener_port, minute_ts)
                DO UPDATE SET bytes = bytes + excluded.bytes,
                              requests = requests + excluded.requests
                """,
                [
                    (lb_arn, ip, port, ts, b, r)
                    for (ip, port, ts), (b, r) in client_rows.items()
                ],
            )
            c.execute(
                "INSERT OR IGNORE INTO parsed_files (s3_bucket, s3_key, parsed_at) VALUES (?, ?, ?)",
                (s3_bucket, s3_key, parsed_at),
            )
            c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK")
            raise


def query_series(
    lb_arn: str,
    ports: list[int],
    start_ts: int,
    end_ts: int,
    bucket_seconds: int,
) -> dict[int, list[tuple[int, int, int]]]:
    """Return per-port list of (bucket_start_ts, bytes_sum, requests_sum).

    Aggregation: rounds each minute row down to its bucket and sums.
    """
    if not ports:
        return {}
    placeholders = ",".join("?" for _ in ports)
    sql = f"""
        SELECT listener_port,
               (minute_ts / ?) * ? AS bucket_ts,
               SUM(bytes), SUM(requests)
          FROM bw_minute
         WHERE lb_arn = ?
           AND listener_port IN ({placeholders})
           AND minute_ts >= ?
           AND minute_ts <  ?
         GROUP BY listener_port, bucket_ts
         ORDER BY listener_port, bucket_ts
    """
    with _lock, closing(_conn()) as c:
        cur = c.execute(
            sql,
            (bucket_seconds, bucket_seconds, lb_arn, *ports, start_ts, end_ts),
        )
        out: dict[int, list[tuple[int, int, int]]] = {p: [] for p in ports}
        for port, ts, bytes_sum, req_sum in cur:
            out[port].append((int(ts), int(bytes_sum or 0), int(req_sum or 0)))
        return out


def query_top_clients(
    lb_arn: str,
    ports: list[int],
    start_ts: int,
    end_ts: int,
    limit: int = 50,
) -> list[tuple[str, int, int, int]]:
    """Return top clients by total bytes across the given ports and time range.

    Returns list of (client_ip, listener_port, total_bytes, total_requests)
    ordered by total_bytes descending.
    """
    if not ports:
        return []
    placeholders = ",".join("?" for _ in ports)
    sql = f"""
        SELECT client_ip, listener_port, SUM(bytes), SUM(requests)
          FROM client_traffic
         WHERE lb_arn = ?
           AND listener_port IN ({placeholders})
           AND minute_ts >= ?
           AND minute_ts <  ?
         GROUP BY client_ip, listener_port
         ORDER BY SUM(bytes) DESC
         LIMIT ?
    """
    with _lock, closing(_conn()) as c:
        cur = c.execute(
            sql,
            (lb_arn, *ports, start_ts, end_ts, limit),
        )
        return [(ip, int(port), int(b or 0), int(r or 0)) for ip, port, b, r in cur]
