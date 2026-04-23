"""AWS helpers: profile discovery, sessions, ELBv2 resolution, access log config."""
from __future__ import annotations

import configparser
import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError, ClientError


@dataclass
class LoadBalancer:
    arn: str
    name: str
    type: str  # "application" | "network"
    dns_name: str
    region: str
    account_id: str  # parsed from arn


@dataclass
class Listener:
    arn: str
    lb_arn: str
    port: int
    protocol: str  # HTTP|HTTPS|TCP|UDP|TCP_UDP|TLS
    default_action_type: str  # forward | redirect | fixed-response | authenticate-cognito | authenticate-oidc | unknown
    target_group_names: list[str]
    tag_name: str | None  # value of "Name" tag if set


@dataclass
class AccessLogConfig:
    enabled: bool
    bucket: str | None
    prefix: str | None


def list_profiles() -> list[str]:
    """Return the set of profile names from ~/.aws/config and ~/.aws/credentials."""
    home = Path.home()
    names: set[str] = set()

    creds_path = home / ".aws" / "credentials"
    if creds_path.exists():
        cp = configparser.ConfigParser()
        cp.read(creds_path)
        names.update(cp.sections())

    config_path = home / ".aws" / "config"
    if config_path.exists():
        cp = configparser.ConfigParser()
        cp.read(config_path)
        for section in cp.sections():
            # config file uses "profile NAME" except for default
            if section == "default":
                names.add("default")
            elif section.startswith("profile "):
                names.add(section[len("profile "):])

    return sorted(names)


def get_session(profile: str, region: str) -> boto3.Session:
    return boto3.Session(profile_name=profile, region_name=region)


# Regions where ELBv2 is available — kept static; users can type any region too.
COMMON_REGIONS = [
    "us-east-1", "us-east-2", "us-west-1", "us-west-2",
    "eu-west-1", "eu-west-2", "eu-west-3", "eu-central-1", "eu-north-1",
    "ap-south-1", "ap-southeast-1", "ap-southeast-2", "ap-northeast-1",
    "ap-northeast-2", "ap-northeast-3", "ca-central-1", "sa-east-1",
    "me-south-1", "af-south-1",
]


def _account_from_arn(arn: str) -> str:
    # arn:aws:elasticloadbalancing:region:account:loadbalancer/app/name/id
    parts = arn.split(":")
    return parts[4] if len(parts) > 4 else ""


def list_load_balancers(session: boto3.Session) -> list[LoadBalancer]:
    elbv2 = session.client("elbv2")
    region = session.region_name or ""
    out: list[LoadBalancer] = []
    paginator = elbv2.get_paginator("describe_load_balancers")
    for page in paginator.paginate():
        for lb in page["LoadBalancers"]:
            lb_type = lb.get("Type")
            if lb_type not in ("application", "network"):
                continue
            out.append(LoadBalancer(
                arn=lb["LoadBalancerArn"],
                name=lb["LoadBalancerName"],
                type=lb_type,
                dns_name=lb.get("DNSName", ""),
                region=region,
                account_id=_account_from_arn(lb["LoadBalancerArn"]),
            ))
    return out


_TERMINAL_ACTIONS = {"forward", "redirect", "fixed-response"}


def _terminal_action(default_actions: list[dict]) -> tuple[str, list[str]]:
    """Return (action_type, target_group_arns).

    Listeners may chain auth actions before a terminal action; we pick the
    terminal (forward/redirect/fixed-response) and, for forward, collect all
    target group ARNs (weighted-forward included).
    """
    for act in default_actions:
        t = act.get("Type", "")
        if t not in _TERMINAL_ACTIONS:
            continue
        if t != "forward":
            return t, []
        tg_arns: list[str] = []
        if act.get("TargetGroupArn"):
            tg_arns.append(act["TargetGroupArn"])
        for tg in (act.get("ForwardConfig") or {}).get("TargetGroups", []) or []:
            if tg.get("TargetGroupArn"):
                tg_arns.append(tg["TargetGroupArn"])
        # de-dupe while preserving order
        seen: set[str] = set()
        deduped = [x for x in tg_arns if not (x in seen or seen.add(x))]
        return "forward", deduped
    return "unknown", []


def _chunks(seq, n: int):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def _resolve_target_group_names(elbv2, tg_arns: list[str]) -> dict[str, str]:
    if not tg_arns:
        return {}
    out: dict[str, str] = {}
    # DescribeTargetGroups TargetGroupArns limit is 20 per call.
    for batch in _chunks(list(set(tg_arns)), 20):
        resp = elbv2.describe_target_groups(TargetGroupArns=batch)
        for tg in resp.get("TargetGroups", []):
            out[tg["TargetGroupArn"]] = tg["TargetGroupName"]
    return out


def _resolve_listener_name_tags(elbv2, listener_arns: list[str]) -> dict[str, str | None]:
    out: dict[str, str | None] = {arn: None for arn in listener_arns}
    if not listener_arns:
        return out
    # DescribeTags ResourceArns limit is 20 per call.
    for batch in _chunks(listener_arns, 20):
        resp = elbv2.describe_tags(ResourceArns=batch)
        for td in resp.get("TagDescriptions", []):
            for tag in td.get("Tags", []):
                if tag.get("Key") == "Name":
                    out[td["ResourceArn"]] = tag.get("Value") or None
                    break
    return out


def list_listeners(session: boto3.Session, lb_arn: str) -> list[Listener]:
    elbv2 = session.client("elbv2")
    raw: list[dict] = []
    paginator = elbv2.get_paginator("describe_listeners")
    for page in paginator.paginate(LoadBalancerArn=lb_arn):
        raw.extend(page.get("Listeners", []))

    # Collect target group ARNs across all listeners and resolve names in batch.
    all_tg_arns: list[str] = []
    per_listener: list[tuple[dict, str, list[str]]] = []
    for li in raw:
        action_type, tg_arns = _terminal_action(li.get("DefaultActions", []) or [])
        per_listener.append((li, action_type, tg_arns))
        all_tg_arns.extend(tg_arns)

    tg_names = _resolve_target_group_names(elbv2, all_tg_arns)
    tag_names = _resolve_listener_name_tags(elbv2, [li["ListenerArn"] for li in raw])

    out: list[Listener] = []
    for li, action_type, tg_arns in per_listener:
        out.append(Listener(
            arn=li["ListenerArn"],
            lb_arn=lb_arn,
            port=int(li["Port"]),
            protocol=li["Protocol"],
            default_action_type=action_type,
            target_group_names=[tg_names.get(a, a.rsplit("/", 2)[-2] if "/" in a else a) for a in tg_arns],
            tag_name=tag_names.get(li["ListenerArn"]),
        ))
    return out


def get_access_log_config(session: boto3.Session, lb_arn: str) -> AccessLogConfig:
    """Read access log config for an ALB or NLB.

    Attribute keys are the same across ALB and NLB:
        access_logs.s3.enabled, access_logs.s3.bucket, access_logs.s3.prefix
    """
    elbv2 = session.client("elbv2")
    resp = elbv2.describe_load_balancer_attributes(LoadBalancerArn=lb_arn)
    attrs = {a["Key"]: a["Value"] for a in resp.get("Attributes", [])}
    enabled = attrs.get("access_logs.s3.enabled", "false").lower() == "true"
    bucket = attrs.get("access_logs.s3.bucket") or None
    prefix = attrs.get("access_logs.s3.prefix") or None
    return AccessLogConfig(enabled=enabled, bucket=bucket, prefix=prefix)


def lb_id_from_arn(lb_arn: str) -> str:
    """Extract the short id used in access log filenames.

    ARN form:
      arn:aws:elasticloadbalancing:region:account:loadbalancer/app/NAME/ID
      arn:aws:elasticloadbalancing:region:account:loadbalancer/net/NAME/ID
    Log filename uses "app.NAME.ID" or "net.NAME.ID".
    """
    tail = lb_arn.split(":loadbalancer/")[-1]  # app/NAME/ID
    kind, name, short = tail.split("/")
    return f"{kind}.{name}.{short}"
