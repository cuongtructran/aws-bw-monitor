"""S3 access log discovery, download, and parsing for ALB / NLB-TLS."""
from __future__ import annotations

import gzip
import io
import logging
import re
import shlex
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable
from urllib.parse import urlparse

import boto3

from . import cache

log = logging.getLogger(__name__)

# Filename end-time, e.g. 20230801T1500Z
_END_TIME_RE = re.compile(r"_(\d{8}T\d{4}Z)_")


@dataclass
class S3LogFile:
    bucket: str
    key: str
    end_time_utc: datetime


def _iter_dates(start: datetime, end: datetime):
    d = datetime(start.year, start.month, start.day, tzinfo=timezone.utc)
    stop = datetime(end.year, end.month, end.day, tzinfo=timezone.utc)
    while d <= stop:
        yield d
        d += timedelta(days=1)


def list_log_files(
    s3,
    bucket: str,
    prefix: str | None,
    account_id: str,
    region: str,
    lb_id: str,  # e.g. "app.NAME.ID" or "net.NAME.ID"
    start_utc: datetime,
    end_utc: datetime,
) -> list[S3LogFile]:
    """Enumerate log files whose end-time falls within [start, end + 5min]."""
    window_end = end_utc + timedelta(minutes=5)
    base = ""
    if prefix:
        base = prefix.rstrip("/") + "/"
    out: list[S3LogFile] = []
    name_needle = f"_{lb_id}_"

    for d in _iter_dates(start_utc, end_utc):
        key_prefix = (
            f"{base}AWSLogs/{account_id}/elasticloadbalancing/{region}/"
            f"{d.year:04d}/{d.month:02d}/{d.day:02d}/"
        )
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=key_prefix):
            for obj in page.get("Contents", []) or []:
                key = obj["Key"]
                if name_needle not in key or not key.endswith(".log.gz"):
                    continue
                m = _END_TIME_RE.search(key)
                if not m:
                    continue
                end_dt = datetime.strptime(m.group(1), "%Y%m%dT%H%MZ").replace(tzinfo=timezone.utc)
                if start_utc <= end_dt <= window_end:
                    out.append(S3LogFile(bucket=bucket, key=key, end_time_utc=end_dt))
    return out


def _port_from_alb_url(url: str) -> int | None:
    try:
        p = urlparse(url)
        if p.port:
            return p.port
        if p.scheme == "https":
            return 443
        if p.scheme == "http":
            return 80
    except Exception:
        return None
    return None


def _parse_iso8601(ts: str) -> int | None:
    """Return epoch seconds (UTC) for an ISO-8601 timestamp; None on failure."""
    try:
        # e.g. 2023-08-01T14:59:59.123456Z
        if ts.endswith("Z"):
            ts = ts[:-1] + "+00:00"
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception:
        return None


def parse_alb_lines(lines: Iterable[str]) -> dict[tuple[int, int], tuple[int, int]]:
    """Aggregate ALB log lines by (listener_port, minute_ts) → (bytes, requests)."""
    agg: dict[tuple[int, int], list[int]] = defaultdict(lambda: [0, 0])
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            fields = shlex.split(line)
        except ValueError:
            continue
        if len(fields) < 13:
            continue
        ts_s = _parse_iso8601(fields[1])
        if ts_s is None:
            continue
        try:
            received = int(fields[10])
            sent = int(fields[11])
        except ValueError:
            continue
        req = fields[12]
        # request is "METHOD URL PROTO"
        parts = req.split(" ", 2)
        if len(parts) < 2:
            continue
        port = _port_from_alb_url(parts[1])
        if port is None:
            continue
        minute = ts_s - (ts_s % 60)
        key = (port, minute)
        agg[key][0] += received + sent
        agg[key][1] += 1
    return {k: (v[0], v[1]) for k, v in agg.items()}


def parse_nlb_lines(lines: Iterable[str]) -> dict[tuple[int, int], tuple[int, int]]:
    """Aggregate NLB-TLS log lines by (listener_port, minute_ts) → (bytes, requests).

    NLB TLS format (positional):
      0 type  1 version  2 time  3 elb  4 listener
      5 client:port  6 destination:port
      7 connection_time  8 tls_handshake_time
      9 received_bytes  10 sent_bytes  ...
    """
    agg: dict[tuple[int, int], list[int]] = defaultdict(lambda: [0, 0])
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            fields = shlex.split(line)
        except ValueError:
            continue
        if len(fields) < 11:
            continue
        ts_s = _parse_iso8601(fields[2])
        if ts_s is None:
            continue
        dest = fields[6]
        if ":" not in dest:
            continue
        try:
            port = int(dest.rsplit(":", 1)[1])
            received = int(fields[9])
            sent = int(fields[10])
        except ValueError:
            continue
        minute = ts_s - (ts_s % 60)
        key = (port, minute)
        agg[key][0] += received + sent
        agg[key][1] += 1
    return {k: (v[0], v[1]) for k, v in agg.items()}


def _download_and_parse(
    s3,
    f: S3LogFile,
    lb_type: str,
) -> dict[tuple[int, int], tuple[int, int]]:
    body = s3.get_object(Bucket=f.bucket, Key=f.key)["Body"].read()
    with gzip.GzipFile(fileobj=io.BytesIO(body)) as gz:
        text = io.TextIOWrapper(gz, encoding="utf-8", errors="replace")
        lines = list(text)
    if lb_type == "application":
        return parse_alb_lines(lines)
    return parse_nlb_lines(lines)


def ingest_logs(
    session: boto3.Session,
    lb_arn: str,
    lb_type: str,
    bucket: str,
    prefix: str | None,
    account_id: str,
    region: str,
    lb_id: str,
    start_utc: datetime,
    end_utc: datetime,
    progress_cb=None,
) -> int:
    """Download and parse all log files in range that are not yet cached.

    Returns the number of files newly parsed.
    """
    s3 = session.client("s3")
    files = list_log_files(s3, bucket, prefix, account_id, region, lb_id, start_utc, end_utc)
    todo = [f for f in files if not cache.is_parsed(f.bucket, f.key)]
    if progress_cb:
        progress_cb({"total": len(files), "todo": len(todo), "done": 0})
    if not todo:
        return 0

    done_count = 0

    def work(f: S3LogFile):
        rows = _download_and_parse(s3, f, lb_type)
        cache.insert_aggregates(lb_arn, rows, f.bucket, f.key, int(time.time()))
        return f

    # Modest concurrency — S3 reads are I/O bound.
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = [ex.submit(work, f) for f in todo]
        for fut in as_completed(futs):
            try:
                fut.result()
            except Exception as exc:
                log.warning("failed parsing log file: %s", exc)
            done_count += 1
            if progress_cb:
                progress_cb({"total": len(files), "todo": len(todo), "done": done_count})
    return done_count
