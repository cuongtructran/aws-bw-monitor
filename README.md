# AWS Bandwidth Monitor

> Local web tool for charting **per-listener** bandwidth on AWS ALB / NLB, driven by S3 access logs.

CloudWatch's `ProcessedBytes` metric is LoadBalancer-level only — it does not support a `TargetGroup` dimension on either ALB or NLB. This tool parses raw access logs to give you true per-listener bandwidth charts.

## Listener coverage

| LB type | Listener type | Supported |
| ------- | ------------- | --------- |
| ALB     | HTTP / HTTPS  | ✅ Yes |
| NLB     | TLS           | ✅ Yes |
| NLB     | TCP / UDP / TCP_UDP | ❌ No — requires VPC Flow Logs (out of scope) |

## Prerequisites

- **Python 3.10+**
- **AWS credentials** — one or more named profiles in `~/.aws/config` / `~/.aws/credentials` with:
  - `elasticloadbalancing:Describe*` on the target LBs
  - `s3:GetObject`, `s3:ListBucket` on the access-log bucket
- **Access logs enabled** on each load balancer you want to monitor
  - Console → EC2 → Load Balancers → _Attributes_ → Access logs → S3
  - First file is delivered ~5 minutes after enabling

## Quick start

### Windows

Double-click `start.bat` — it creates a virtualenv, installs dependencies, starts the server on http://127.0.0.1:8000, and opens your browser. Subsequent runs skip setup.

### Linux / macOS

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn backend.main:app --host 127.0.0.1 --port 8000
```

### Windows (manual)

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
uvicorn backend.main:app --host 127.0.0.1 --port 8000
```

Press `Ctrl+C` to stop the server.

## Usage

1. Pick an **AWS profile** and **region**, click **Load LBs**.
2. Pick a **load balancer**. If access logs are off, the app tells you so.
3. Tick the **listeners** you want to chart (non-TLS NLB listeners are disabled).
4. Pick a **timeframe** (5 min · 1 h · 1 d · 1 w · 30 d) and click **Fetch bandwidth**.

## How it works

```
┌──────────┐     POST /api/bandwidth     ┌──────────────┐
│ Frontend │ ──────────────────────────▶  │   FastAPI     │
│ Chart.js │ ◀──────────────────────────  │   backend     │
└──────────┘     JSON time-series        └──────┬───────┘
                                                │
                              ┌─────────────────┼─────────────────┐
                              ▼                                   ▼
                     ┌────────────────┐                  ┌──────────────┐
                     │  S3 access logs │                  │ SQLite cache │
                     │  (per LB)       │                  │ ~/.aws-bw-   │
                     └────────────────┘                  │ monitor/     │
                                                         │ cache.db     │
                                                         └──────────────┘
```

1. Lists S3 log files within the selected time window using the filename end-time (`…_YYYYMMDDTHHMMZ_…`).
2. Downloads + gunzips files that aren't cached yet, parses each line, attributes bytes to a listener via the destination port.
3. Stores per-minute aggregates in a local SQLite cache (`~/.aws-bw-monitor/cache.db`).
4. Re-queries hit the cache — only new files are fetched from S3.

To clear the cache: `rm ~/.aws-bw-monitor/cache.db`

## Caveats

- **Log delivery cadence is ~5 min** — sub-5-minute ranges may be empty for very recent traffic.
- **Long ranges** (week / 30 d) on a high-traffic LB may trigger hundreds or thousands of S3 reads on first query. The cache amortises this for repeat queries.
- **Port-based attribution is safe** — multiple listeners sharing a port across different LBs works fine (cache keys by `lb_arn`). Sharing a port within one LB is not possible on AWS.

## Project layout

```
backend/
  main.py          FastAPI app + API endpoints
  aws.py           AWS session, LB / listener / attributes helpers
  s3_logs.py       S3 listing, download, log parsing, ingestion pipeline
  cache.py         SQLite cache of per-minute aggregates
frontend/
  index.html       UI shell
  app.js           Pickers + Chart.js rendering
  style.css        Styles
requirements.txt   Python dependencies
start.bat          One-click launcher (Windows)
```
