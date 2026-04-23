"""FastAPI backend for AWS bandwidth monitor."""
from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from . import aws, cache, s3_logs

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

app = FastAPI(title="AWS Bandwidth Monitor")


TIMEFRAMES = {
    # key: (range_seconds, bucket_seconds, label)
    "5m":  (5 * 60,            60,      "Last 5 minutes"),
    "1h":  (60 * 60,           60,      "Last hour"),
    "1d":  (24 * 60 * 60,      5 * 60,  "Last day"),
    "1w":  (7 * 24 * 60 * 60,  60 * 60, "Last week"),
    "1mo": (30 * 24 * 60 * 60, 6 * 60 * 60, "Last 30 days"),
}


@app.on_event("startup")
def _startup() -> None:
    cache.init_db()


# ----- request / response models -----


class ListenerDTO(BaseModel):
    arn: str
    port: int
    protocol: str
    supported: bool  # False for non-TLS NLB listeners
    reason: str | None = None
    default_action_type: str = "unknown"
    target_group_names: list[str] = []
    tag_name: str | None = None
    display_label: str = ""  # best human-readable name for this listener


class LoadBalancerDTO(BaseModel):
    arn: str
    name: str
    type: str
    region: str
    account_id: str
    access_logs_enabled: bool
    access_logs_bucket: str | None = None
    access_logs_prefix: str | None = None


class BandwidthRequest(BaseModel):
    profile: str
    region: str
    lb_arn: str
    lb_type: str  # application | network
    listener_ports: list[int] = Field(min_length=1)
    timeframe: str


class SeriesPoint(BaseModel):
    ts: int
    bytes: int
    requests: int


class BandwidthSeries(BaseModel):
    port: int
    points: list[SeriesPoint]


class BandwidthResponse(BaseModel):
    timeframe: str
    start_ts: int
    end_ts: int
    bucket_seconds: int
    series: list[BandwidthSeries]
    ingested_files: int
    cache_only: bool


# ----- endpoints -----


@app.get("/api/profiles")
def get_profiles() -> dict:
    return {"profiles": aws.list_profiles()}


@app.get("/api/regions")
def get_regions() -> dict:
    return {"regions": aws.COMMON_REGIONS}


@app.get("/api/load_balancers")
def get_load_balancers(profile: str, region: str) -> dict:
    try:
        session = aws.get_session(profile, region)
        lbs = aws.list_load_balancers(session)
        out: list[LoadBalancerDTO] = []
        for lb in lbs:
            cfg = aws.get_access_log_config(session, lb.arn)
            out.append(LoadBalancerDTO(
                arn=lb.arn, name=lb.name, type=lb.type,
                region=lb.region, account_id=lb.account_id,
                access_logs_enabled=cfg.enabled,
                access_logs_bucket=cfg.bucket,
                access_logs_prefix=cfg.prefix,
            ))
        return {"load_balancers": [lb.model_dump() for lb in out]}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


def _listener_display_label(li: aws.Listener) -> str:
    if li.tag_name:
        return li.tag_name
    if li.default_action_type == "forward" and li.target_group_names:
        if len(li.target_group_names) == 1:
            return li.target_group_names[0]
        return " + ".join(li.target_group_names[:3]) + (
            f" (+{len(li.target_group_names) - 3})" if len(li.target_group_names) > 3 else ""
        )
    if li.default_action_type in ("redirect", "fixed-response"):
        return f"<{li.default_action_type}>"
    return "<no target>"


@app.get("/api/listeners")
def get_listeners(profile: str, region: str, lb_arn: str, lb_type: str) -> dict:
    try:
        session = aws.get_session(profile, region)
        listeners = aws.list_listeners(session, lb_arn)
        out: list[ListenerDTO] = []
        for li in listeners:
            label = _listener_display_label(li)
            if lb_type == "network" and li.protocol != "TLS":
                out.append(ListenerDTO(
                    arn=li.arn, port=li.port, protocol=li.protocol,
                    supported=False,
                    reason="NLB non-TLS listeners have no access logs; VPC Flow Logs required (not supported).",
                    default_action_type=li.default_action_type,
                    target_group_names=li.target_group_names,
                    tag_name=li.tag_name,
                    display_label=label,
                ))
            else:
                out.append(ListenerDTO(
                    arn=li.arn, port=li.port, protocol=li.protocol, supported=True,
                    default_action_type=li.default_action_type,
                    target_group_names=li.target_group_names,
                    tag_name=li.tag_name,
                    display_label=label,
                ))
        return {"listeners": [d.model_dump() for d in out]}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/bandwidth")
def post_bandwidth(req: BandwidthRequest) -> BandwidthResponse:
    if req.timeframe not in TIMEFRAMES:
        raise HTTPException(status_code=400, detail=f"unknown timeframe {req.timeframe}")
    range_s, bucket_s, _ = TIMEFRAMES[req.timeframe]

    end = datetime.now(timezone.utc)
    start = end - timedelta(seconds=range_s)
    start_ts = int(start.timestamp())
    end_ts = int(end.timestamp())

    session = aws.get_session(req.profile, req.region)
    lb_cfg = aws.get_access_log_config(session, req.lb_arn)
    if not lb_cfg.enabled or not lb_cfg.bucket:
        raise HTTPException(
            status_code=400,
            detail=(
                "Access logs are not enabled on this load balancer. "
                "Enable them (LB > Attributes > Access logs → S3) and wait ~5 min for first delivery."
            ),
        )
    account_id = req.lb_arn.split(":")[4]
    lb_id = aws.lb_id_from_arn(req.lb_arn)

    t0 = time.time()
    ingested = s3_logs.ingest_logs(
        session=session,
        lb_arn=req.lb_arn,
        lb_type=req.lb_type,
        bucket=lb_cfg.bucket,
        prefix=lb_cfg.prefix,
        account_id=account_id,
        region=req.region,
        lb_id=lb_id,
        start_utc=start,
        end_utc=end,
    )
    cache_only = ingested == 0
    logging.info("ingest took %.2fs, files=%d", time.time() - t0, ingested)

    series_map = cache.query_series(
        lb_arn=req.lb_arn,
        ports=req.listener_ports,
        start_ts=start_ts,
        end_ts=end_ts,
        bucket_seconds=bucket_s,
    )
    series = [
        BandwidthSeries(
            port=p,
            points=[SeriesPoint(ts=ts, bytes=b, requests=r) for ts, b, r in pts],
        )
        for p, pts in series_map.items()
    ]
    return BandwidthResponse(
        timeframe=req.timeframe,
        start_ts=start_ts,
        end_ts=end_ts,
        bucket_seconds=bucket_s,
        series=series,
        ingested_files=ingested,
        cache_only=cache_only,
    )


# ----- static frontend -----

_FRONTEND_DIR = Path(__file__).resolve().parent.parent / "frontend"


@app.get("/")
def root() -> FileResponse:
    return FileResponse(_FRONTEND_DIR / "index.html")


app.mount("/static", StaticFiles(directory=_FRONTEND_DIR), name="static")
