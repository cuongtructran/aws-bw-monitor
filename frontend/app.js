"use strict";

const $ = (id) => document.getElementById(id);

let state = {
  loadBalancers: [],
  listeners: [],
  chart: null,
  lastResponse: null,
};

const TZ_MAP = {
  utc: "UTC",
  local: undefined,
  ny: "America/New_York",
};

function tzLabel(key) {
  if (key === "utc") return "UTC";
  if (key === "ny") return "America/New_York";
  const guess = Intl.DateTimeFormat().resolvedOptions().timeZone || "Local";
  return `Local (${guess})`;
}

function fmtTickTime(ts, tz, rangeMs) {
  const opts = rangeMs > 24 * 3600 * 1000
    ? { month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", hour12: false }
    : { hour: "2-digit", minute: "2-digit", hour12: false };
  opts.timeZone = TZ_MAP[tz];
  return new Intl.DateTimeFormat([], opts).format(new Date(ts));
}

function fmtTooltipTime(ts, tz) {
  return new Intl.DateTimeFormat([], {
    year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", second: "2-digit",
    hour12: false, timeZone: TZ_MAP[tz],
  }).format(new Date(ts));
}

const PALETTE = [
  "#0b6bcb", "#d93f0b", "#1f6f2b", "#9c27b0", "#b26a00",
  "#005fae", "#c62828", "#00897b", "#6a1b9a", "#ef6c00",
];

function showError(msg) {
  const el = $("error");
  el.textContent = msg;
  el.hidden = false;
}
function clearError() { $("error").hidden = true; }

async function api(method, path, body) {
  const opts = { method, headers: { "Content-Type": "application/json" } };
  if (body) opts.body = JSON.stringify(body);
  const r = await fetch(path, opts);
  if (!r.ok) {
    const text = await r.text();
    throw new Error(`${r.status} ${r.statusText}: ${text}`);
  }
  return r.json();
}

async function loadProfilesAndRegions() {
  const [p, r] = await Promise.all([api("GET", "/api/profiles"), api("GET", "/api/regions")]);
  const profSel = $("profile");
  profSel.innerHTML = "";
  for (const name of p.profiles) {
    const o = document.createElement("option");
    o.value = o.textContent = name;
    profSel.appendChild(o);
  }
  if (p.profiles.length === 0) {
    showError("No AWS profiles found in ~/.aws/config or ~/.aws/credentials.");
  }

  const regSel = $("region");
  regSel.innerHTML = "";
  for (const name of r.regions) {
    const o = document.createElement("option");
    o.value = o.textContent = name;
    regSel.appendChild(o);
  }
  regSel.value = "us-east-1";
}

async function loadLoadBalancers() {
  clearError();
  $("lb").innerHTML = "";
  $("listeners").innerHTML = "";
  $("lb-status").textContent = "";
  const profile = $("profile").value;
  const region = $("region").value;
  if (!profile || !region) {
    showError("Pick a profile and region first.");
    return;
  }
  $("load-lbs").disabled = true;
  try {
    const resp = await api("GET", `/api/load_balancers?profile=${encodeURIComponent(profile)}&region=${encodeURIComponent(region)}`);
    state.loadBalancers = resp.load_balancers;
    const sel = $("lb");
    if (state.loadBalancers.length === 0) {
      const o = document.createElement("option");
      o.textContent = "(no ALB/NLB found)";
      o.disabled = true;
      sel.appendChild(o);
      return;
    }
    for (const lb of state.loadBalancers) {
      const o = document.createElement("option");
      o.value = lb.arn;
      o.textContent = `${lb.name} [${lb.type}]${lb.access_logs_enabled ? "" : " — access logs OFF"}`;
      sel.appendChild(o);
    }
    sel.onchange = onLBChange;
    onLBChange();
  } catch (e) {
    showError(e.message);
  } finally {
    $("load-lbs").disabled = false;
  }
}

async function onLBChange() {
  const arn = $("lb").value;
  const lb = state.loadBalancers.find((x) => x.arn === arn);
  const status = $("lb-status");
  if (!lb) { status.textContent = ""; return; }
  if (!lb.access_logs_enabled) {
    status.className = "badge warn";
    status.textContent = "Access logs disabled — enable on LB attributes";
  } else {
    status.className = "badge ok";
    status.textContent = `logs → s3://${lb.access_logs_bucket}/${lb.access_logs_prefix || ""}`;
  }

  $("listeners").innerHTML = "<em>loading…</em>";
  try {
    const profile = $("profile").value;
    const region = $("region").value;
    const resp = await api("GET",
      `/api/listeners?profile=${encodeURIComponent(profile)}&region=${encodeURIComponent(region)}` +
      `&lb_arn=${encodeURIComponent(arn)}&lb_type=${encodeURIComponent(lb.type)}`);
    state.listeners = resp.listeners;
    renderListeners();
  } catch (e) {
    $("listeners").innerHTML = "";
    showError(e.message);
  }
}

function listenerSeriesLabel(li) {
  const base = `${li.protocol} :${li.port}`;
  return li.display_label ? `${base} — ${li.display_label}` : base;
}

function renderListeners() {
  const box = $("listeners");
  box.innerHTML = "";
  if (state.listeners.length === 0) {
    box.innerHTML = "<em>no listeners</em>";
    return;
  }
  for (const li of state.listeners) {
    const chip = document.createElement("label");
    chip.className = "listener-chip" + (li.supported ? "" : " unsupported");
    const cb = document.createElement("input");
    cb.type = "checkbox";
    cb.value = li.port;
    cb.disabled = !li.supported;
    cb.checked = li.supported;
    chip.appendChild(cb);

    const text = document.createElement("span");
    text.className = "chip-proto";
    text.textContent = `${li.protocol} :${li.port}`;
    chip.appendChild(text);

    if (li.display_label) {
      const svc = document.createElement("span");
      svc.className = "chip-label";
      svc.textContent = li.display_label;
      if (li.tag_name) {
        svc.title = `Name tag: ${li.tag_name}`;
      } else if (li.target_group_names && li.target_group_names.length) {
        svc.title = `Target groups: ${li.target_group_names.join(", ")}`;
      } else if (li.default_action_type) {
        svc.title = `Default action: ${li.default_action_type}`;
      }
      chip.appendChild(svc);
    }

    const tooltipBits = [];
    if (!li.supported && li.reason) tooltipBits.push(li.reason);
    if (li.default_action_type && li.default_action_type !== "unknown") {
      tooltipBits.push(`action: ${li.default_action_type}`);
    }
    if (tooltipBits.length) chip.title = tooltipBits.join("\n");

    box.appendChild(chip);
  }
}

function selectedPorts() {
  return Array.from(document.querySelectorAll("#listeners input[type=checkbox]:checked"))
    .map((el) => parseInt(el.value, 10));
}

function setAllListeners(checked) {
  for (const el of document.querySelectorAll("#listeners input[type=checkbox]:not(:disabled)")) {
    el.checked = checked;
  }
}

function formatBytes(n) {
  if (n < 1024) return `${n} B`;
  const units = ["KB", "MB", "GB", "TB", "PB"];
  let v = n / 1024, i = 0;
  while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
  return `${v.toFixed(2)} ${units[i]}`;
}

async function fetchBandwidth() {
  clearError();
  const profile = $("profile").value;
  const region = $("region").value;
  const arn = $("lb").value;
  const lb = state.loadBalancers.find((x) => x.arn === arn);
  const ports = selectedPorts();
  if (!arn || !lb) { showError("Pick a load balancer."); return; }
  if (ports.length === 0) { showError("Pick at least one listener."); return; }
  if (!lb.access_logs_enabled) {
    showError("Access logs are not enabled on this LB. Enable them and wait ~5 min for first delivery.");
    return;
  }

  $("run").disabled = true;
  $("progress").textContent = "fetching…";
  try {
    const resp = await api("POST", "/api/bandwidth", {
      profile, region,
      lb_arn: arn, lb_type: lb.type,
      listener_ports: ports,
      timeframe: $("timeframe").value,
    });
    state.lastResponse = resp;
    renderChart(resp);
    $("progress").textContent =
      `ingested ${resp.ingested_files} new file(s)` +
      (resp.cache_only ? " (served from cache)" : "");
  } catch (e) {
    showError(e.message);
    $("progress").textContent = "";
  } finally {
    $("run").disabled = false;
  }
}

function renderChart(resp) {
  const ctx = $("chart").getContext("2d");
  if (state.chart) { state.chart.destroy(); }

  const tz = $("timezone").value;
  const rangeMs = (resp.end_ts - resp.start_ts) * 1000;

  const datasets = resp.series.map((s, i) => {
    const li = state.listeners.find((x) => x.port === s.port);
    const label = li ? listenerSeriesLabel(li) : `:${s.port}`;
    return {
      label,
      data: s.points.map((p) => ({ x: p.ts * 1000, y: p.bytes })),
      borderColor: PALETTE[i % PALETTE.length],
      backgroundColor: PALETTE[i % PALETTE.length] + "22",
      tension: 0.2,
      pointRadius: 2,
      borderWidth: 2,
      fill: false,
    };
  });

  const totalBytes = resp.series.reduce((sum, s) => sum + s.points.reduce((a, p) => a + p.bytes, 0), 0);

  state.chart = new Chart(ctx, {
    type: "line",
    data: { datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      interaction: { mode: "nearest", intersect: false },
      plugins: {
        legend: { position: "top" },
        title: {
          display: true,
          text: `Bandwidth per listener — total ${formatBytes(totalBytes)} over window`,
        },
        tooltip: {
          callbacks: {
            title: (items) => items.length ? fmtTooltipTime(items[0].parsed.x, tz) : "",
            label: (c) => `${c.dataset.label}: ${formatBytes(c.parsed.y)}`,
          },
        },
      },
      scales: {
        x: {
          type: "time",
          min: resp.start_ts * 1000,
          max: resp.end_ts * 1000,
          title: { display: true, text: `Time (${tzLabel(tz)})` },
          ticks: {
            callback: (value) => fmtTickTime(value, tz, rangeMs),
            maxRotation: 0,
            autoSkip: true,
          },
        },
        y: {
          beginAtZero: true,
          title: { display: true, text: `Bytes per ${resp.bucket_seconds}s bucket` },
          ticks: { callback: (v) => formatBytes(v) },
        },
      },
    },
  });
}

function onTimezoneChange() {
  if (state.lastResponse) renderChart(state.lastResponse);
}

document.addEventListener("DOMContentLoaded", () => {
  $("load-lbs").addEventListener("click", loadLoadBalancers);
  $("run").addEventListener("click", fetchBandwidth);
  $("listeners-all").addEventListener("click", () => setAllListeners(true));
  $("listeners-none").addEventListener("click", () => setAllListeners(false));
  $("timezone").addEventListener("change", onTimezoneChange);
  loadProfilesAndRegions().catch((e) => showError(e.message));
});
