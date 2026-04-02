"""
IS_chutes_sn64_analysis.py
=======================
SN64 Chutes — Intelligence Market Hypothesis Test + E2EE Perimeter Forensics
Part of the Intelligence Sovereignty research suite by @im_perseverance

Tests whether Bittensor functions as a market for intelligence by comparing:
  Dataset 1: Real demand    — Chutes invocation exports (api.chutes.ai, public)
  Dataset 2: Recognition    — Validator weight allocations (Bittensor on-chain)
  Dataset 3: Capital flow   — Emission distribution (SN64 metagraph)

Join key: SS58 miner hotkey (present in both invocation CSV and metagraph)
Time window: 7-day rolling aggregate (matches incentive calculation window)

Architecture: single-pass sequential streaming fetch.
  All miner aggregates, user aggregates, and perimeter forensic signals are
  collected in one pass over the 168 hourly CSVs. Each hourly CSV is fetched,
  parsed, aggregated into running counters, and discarded before the next
  fetch begins. Peak memory: ~300-400MB regardless of dataset size.

E2EE Perimeter Forensics (demand authenticity):
  Attempts to answer: is the concentrated invocation volume genuine external
  demand, or internally generated traffic masked by end-to-end encryption?
  Eight independent signals are evaluated at the metadata layer without
  touching encrypted content.

  Signal weights:
    Full weight  : token variance, chute age, miner entropy, inter-arrival,
                   function sequence, parent invocation diversity
    Half weight  : instance entropy (partially redundant with miner entropy)
    Not scored   : image owner (structural constant on E2EE subnet)

Outputs:
  - Console report with snapshot metadata
  - chutes_analysis/chutes_sn64_analysis_YYYY-MM-DD.csv
  - chutes_analysis/chutes_sn64_divergence_YYYY-MM-DD.csv
  - chutes_analysis/chutes_sn64_metadata_YYYY-MM-DD.json
  - chutes_analysis/chutes_sn64_demand_users_YYYY-MM-DD.csv
  - chutes_analysis/chutes_sn64_dominant_user_YYYY-MM-DD.json
  - chutes_analysis/chutes_sn64_perimeter_YYYY-MM-DD.json

Note:
  This is a reference implementation. Longitudinal tracking (trajectories,
  verdict flips) is not included in this public version.

Usage:
    python IS_chutes_sn64_analysis.py
    python IS_chutes_sn64_analysis.py --root-threshold 1000 --alpha-threshold 1000
"""

import ast
import bittensor as bt
import requests
import csv
import json
import math
import argparse
import io
import gc
import random as _random
from datetime import datetime, timezone, timedelta
from pathlib import Path
from collections import defaultdict, Counter
from concurrent.futures import ThreadPoolExecutor

# ── Config ────────────────────────────────────────────────────────────────────
NETUID               = 64
CHUTES_API_BASE      = "https://api.chutes.ai"
INVOCATION_WINDOW    = 7
ROOT_TAO_THRESHOLD   = 1000.0
SN64_ALPHA_THRESHOLD = 1000.0
OUTPUT_DIR           = Path("chutes_analysis")

QUALITY_WEIGHT_COMPUTE    = 0.55
QUALITY_WEIGHT_INVOCATION = 0.25
QUALITY_WEIGHT_DIVERSITY  = 0.15
QUALITY_WEIGHT_EFFICIENCY = 0.05

MAX_TS = 50_000  # timestamp reservoir cap for inter-arrival analysis

SEPARATOR = "=" * 100
THIN_SEP  = "-" * 100

# ── Helpers ───────────────────────────────────────────────────────────────────

def fmt_pct(val):
    if val is None: return "  N/A  "
    return f"{val*100:.2f}%"

def fmt_delta(demand, emission):
    if demand is None or emission is None: return "  N/A  "
    delta = emission - demand
    sign  = "+" if delta >= 0 else ""
    return f"{sign}{delta*100:.2f}pp"

def fmt_tao(val, decimals=4):
    return f"{val:.{decimals}f} TAO" if val is not None else "N/A"

def fmt_large(val):
    if val is None: return "N/A"
    if val >= 1_000_000: return f"{val/1_000_000:.2f}M"
    if val >= 1_000:     return f"{val/1_000:.2f}K"
    return f"{val:.2f}"


def parse_metrics(raw: str) -> dict:
    """
    Parse the metrics field from Chutes CSV exports.
    The field uses single-quote Python dict literal format — requires
    ast.literal_eval rather than json.loads.
    Returns empty dict on any failure.
    """
    if not raw or raw.strip() in ("", "{}"): return {}
    try:
        result = ast.literal_eval(raw.strip())
        return result if isinstance(result, dict) else {}
    except Exception:
        return {}


def load_previous_snapshot(output_dir: Path, current_date: str) -> dict:
    """Load previous snapshot for trend analysis."""
    csvs = sorted([
        f for f in output_dir.glob("chutes_sn64_analysis_*.csv")
        if current_date not in f.name
    ], reverse=True)
    if not csvs:
        print("  ℹ️  No previous snapshot found — trend delta will be N/A")
        return {}
    prev_file = csvs[0]
    print(f"  📂 Previous snapshot: {prev_file.name}")
    prev = {}
    try:
        with open(prev_file, newline="") as f:
            for row in csv.DictReader(f):
                hk = row.get("hotkey", "").strip()
                if hk:
                    prev[hk] = {
                        "emission_share":   float(row.get("emission_share",   0) or 0),
                        "invocation_share": float(row.get("invocation_share", 0) or 0),
                        "weight_share":     float(row.get("weight_share",     0) or 0),
                    }
    except Exception as e:
        print(f"  ⚠️  Could not load previous snapshot: {e}")
        return {}
    print(f"  ✅ Loaded {len(prev)} miners from previous snapshot")
    return prev


# ── Alpha Pool Fetch ──────────────────────────────────────────────────────────

def fetch_alpha_pool_state(sub, netuid: int) -> dict:
    result = {
        "alpha_price": None, "ema_price": None, "spot_ema_gap": None,
        "spot_ema_gap_pct": None, "tao_reserves": None, "alpha_outstanding": None,
        "alpha_in_pool": None, "market_cap_tao": None,
    }
    try:
        sn = sub.subnet(netuid)
        if sn is None: return result
        tao_in    = getattr(sn, 'tao_in',       None)
        alpha_in  = getattr(sn, 'alpha_in',     None)
        alpha_out = getattr(sn, 'alpha_out',    None)
        mov_price = getattr(sn, 'moving_price', None)
        price     = getattr(sn, 'price',        None)
        if tao_in is not None:    result["tao_reserves"]      = float(tao_in)
        if alpha_in is not None:  result["alpha_in_pool"]     = float(alpha_in)
        if alpha_out is not None: result["alpha_outstanding"] = float(alpha_out)
        if price is not None:     result["alpha_price"]       = float(price)
        elif tao_in and alpha_in and float(alpha_in) > 0:
            result["alpha_price"] = float(tao_in) / float(alpha_in)
        if mov_price is not None: result["ema_price"]         = float(mov_price)
        if result["alpha_price"] and result["alpha_outstanding"]:
            result["market_cap_tao"] = result["alpha_price"] * result["alpha_outstanding"]
        if result["alpha_price"] and result["ema_price"]:
            result["spot_ema_gap"]     = result["alpha_price"] - result["ema_price"]
            result["spot_ema_gap_pct"] = (result["spot_ema_gap"] / result["ema_price"]) * 100
        print(f"  ✅ Alpha pool state fetched")
    except Exception as e:
        print(f"  ⚠️  Alpha pool fetch failed: {e}")
    return result


# ── Quality Scoring ───────────────────────────────────────────────────────────

def compute_quality_scores(merged: list) -> list:
    active = [m for m in merged if m["invocation_count"] > 0]
    if not active: return merged
    max_div = max(m["chute_diversity"] for m in active) or 1
    for m in active:
        m["_inv_per_sec"]           = (m["invocation_count"] / m["compute_seconds"]
                                       if m["compute_seconds"] > 0 else 0)
        m["seconds_per_invocation"] = (m["compute_seconds"] / m["invocation_count"]
                                       if m["invocation_count"] > 0 else 0)
    active_with_compute = [m for m in active if m["seconds_per_invocation"] > 0]
    if active_with_compute:
        sorted_by_speed = sorted(active_with_compute, key=lambda m: m["seconds_per_invocation"])
        n = len(sorted_by_speed); fast_cut = n // 3; slow_cut = 2 * n // 3
        for i, m in enumerate(sorted_by_speed):
            if i < fast_cut:   m["efficiency_tier"] = "FAST"
            elif i < slow_cut: m["efficiency_tier"] = "MID"
            else:              m["efficiency_tier"] = "SLOW"
    for m in active:
        if "efficiency_tier" not in m: m["efficiency_tier"] = "N/A"
    max_ips = max(m["_inv_per_sec"] for m in active) or 1
    total_qs = 0.0
    for m in active:
        div_norm = m["chute_diversity"] / max_div
        eff_norm = m["_inv_per_sec"]    / max_ips
        qs = (QUALITY_WEIGHT_COMPUTE    * m["compute_share"]    +
              QUALITY_WEIGHT_INVOCATION * m["invocation_share"] +
              QUALITY_WEIGHT_DIVERSITY  * div_norm              +
              QUALITY_WEIGHT_EFFICIENCY * eff_norm)
        m["_raw_quality_score"] = qs; total_qs += qs
    for m in active:
        m["quality_score"]        = m["_raw_quality_score"] / total_qs if total_qs > 0 else 0
        m["delta_quality_weight"] = m["weight_share"]   - m["quality_score"]
        m["delta_quality_emit"]   = m["emission_share"] - m["quality_score"]
    active_keys = {m["hotkey"] for m in active}
    for m in merged:
        if m["hotkey"] not in active_keys:
            m["quality_score"] = 0.0; m["delta_quality_weight"] = 0.0
            m["delta_quality_emit"] = 0.0; m["efficiency_tier"] = "N/A"
            m["seconds_per_invocation"] = 0.0
        m.pop("_raw_quality_score", None); m.pop("_inv_per_sec", None)
    return merged


# ══════════════════════════════════════════════════════════════════════════════
# SINGLE-PASS SEQUENTIAL FETCH
# All miner aggregates, user aggregates, and perimeter forensic signal data
# collected in one sequential pass. One HTTP request at a time. Each hourly
# CSV is parsed and immediately discarded. Peak memory: ~300-400MB.
# ══════════════════════════════════════════════════════════════════════════════

def _detect_fields(header: list) -> dict:
    """Detect field column indices from CSV header row."""
    idx = {h.lower(): j for j, h in enumerate(header)}
    result = {}
    result["hi"]  = idx.get("miner_hotkey") or next(
        (j for h, j in idx.items() if "hotkey" in h), None)
    result["ui"]  = idx.get("miner_uid")  or idx.get("uid")
    result["usi"] = idx.get("chute_user_id")
    result["chi"] = idx.get("chute_id")
    result["fni"] = idx.get("function_name")
    result["imi"] = idx.get("image_id")
    result["iui"] = idx.get("image_user_id")
    result["mi"]  = idx.get("metrics")
    result["pi"]  = idx.get("parent_invocation_id")
    result["ii"]  = idx.get("instance_id")
    result["sa_i"] = idx.get("started_at")
    result["ca_i"] = idx.get("completed_at")
    for f in ("compute_time", "compute_seconds", "duration_seconds", "compute_multiplier"):
        if f in idx: result["ci"] = idx[f]; break
    else:
        result["ci"] = None
    return result


def fetch_all_data(window_days: int) -> tuple:
    """
    Single-pass sequential streaming fetch over all hourly CSVs.

    Collects in one pass:
      - Miner invocation/compute/TPS aggregates
      - User invocation aggregates
      - Perimeter forensic signals (timestamps, bigrams, token variance,
        parent IDs, instance IDs, miner selection counts)

    Returns:
        miner_demand  : { hotkey → miner stats dict }
        user_agg      : { user_id → user stats dict }
        perimeter     : perimeter signal data dict
        fields_info   : detected field names for logging
        all_chute_ids : list of all unique chute IDs seen
    """
    now = datetime.now(timezone.utc)
    urls = []
    for days_back in range(window_days):
        target = now - timedelta(days=days_back)
        for hour in range(24):
            t = target.replace(hour=hour, minute=0, second=0, microsecond=0)
            urls.append(
                f"{CHUTES_API_BASE}/invocations/exports"
                f"/{t.year}/{t.month:02d}/{t.day:02d}/{t.hour:02d}.csv"
            )

    print(f"  Streaming {len(urls)} hourly archives sequentially...")

    agg_miners = defaultdict(lambda: {
        "uid": None, "invocation_count": 0, "compute_seconds": 0.0,
        "chute_ids": set(), "tps_sum": 0.0, "tps_count": 0,
        "ttft_sum": 0.0, "ttft_count": 0, "pass_count": 0, "pass_total": 0,
    })
    agg_users = defaultdict(lambda: {
        "invocation_count": 0, "compute_seconds": 0.0,
        "chute_ids": set(), "function_names": defaultdict(int),
        "image_ids": defaultdict(int), "image_user_ids": defaultdict(int),
        "hour_buckets": defaultdict(int), "instance_ids": set(), "parent_ids": set(),
    })

    # Perimeter accumulators
    ts_reservoir    = []; ts_total_seen = 0
    bigram_counts   = Counter()
    trigram_counts  = Counter()
    fn_total        = Counter()
    TOKEN_FIELDS    = {"it", "ot", "tokens"}
    token_welford   = defaultdict(lambda: {"n": 0, "mean": 0.0, "M2": 0.0})
    u_miner_counts  = defaultdict(lambda: defaultdict(int))
    n_miner_network = Counter()
    net_instance_counts = Counter()

    fields_info = {}
    hi = ui = ci = chi = usi = fni = imi = iui = mi = pi = ii = sa_i = ca_i = None
    total_rows = 0; hours_fetched = 0; hours_failed = 0

    for i, url in enumerate(urls):
        try:
            r = requests.get(url, timeout=25)
            if r.status_code != 200 or not r.text.strip():
                hours_failed += 1; del r; continue
            text = r.text; del r
            reader = csv.reader(io.StringIO(text)); del text
            try:
                header = next(reader)
            except StopIteration:
                hours_failed += 1; continue

            if not fields_info:
                fields_info = _detect_fields(header)
                hi  = fields_info.get("hi");  ui  = fields_info.get("ui")
                ci  = fields_info.get("ci");  chi = fields_info.get("chi")
                usi = fields_info.get("usi"); fni = fields_info.get("fni")
                imi = fields_info.get("imi"); iui = fields_info.get("iui")
                mi  = fields_info.get("mi");  pi  = fields_info.get("pi")
                ii  = fields_info.get("ii");  sa_i = fields_info.get("sa_i")
                ca_i = fields_info.get("ca_i")
                fields_info["header"] = header

            if hi is None: hours_failed += 1; continue

            url_parts = url.rstrip(".csv").split("/")
            try:
                hour_bucket = (f"{url_parts[-4]}-{url_parts[-3]}-"
                               f"{url_parts[-2]}T{url_parts[-1]:0>2}:00")
            except Exception:
                hour_bucket = "unknown"

            prev_fn   = None
            hour_rows = 0

            for cols in reader:
                if hi >= len(cols): continue
                hk = cols[hi].strip()
                if not hk or len(hk) < 10: continue
                total_rows += 1; hour_rows += 1
                n_miner_network[hk] += 1

                # Compute value
                compute_val = 0.0
                if ci is not None and ci < len(cols) and cols[ci]:
                    try: compute_val = float(cols[ci])
                    except (ValueError, TypeError): pass
                elif sa_i is not None and ca_i is not None:
                    try:
                        t0 = datetime.fromisoformat(cols[sa_i].replace("Z", ""))
                        t1 = datetime.fromisoformat(cols[ca_i].replace("Z", ""))
                        compute_val = (t1 - t0).total_seconds()
                    except Exception: pass

                # Miner aggregation
                m = agg_miners[hk]
                m["invocation_count"] += 1
                m["compute_seconds"]  += compute_val
                if ui is not None and ui < len(cols) and cols[ui]:
                    m["uid"] = cols[ui]
                if chi is not None and chi < len(cols) and cols[chi]:
                    m["chute_ids"].add(cols[chi].strip())

                # Metrics parsing (TPS, TTFT, pass rate, token counts)
                m_data = {}
                if mi is not None and mi < len(cols) and cols[mi].strip() not in ("", "{}"):
                    m_data = parse_metrics(cols[mi])
                    if m_data:
                        tps = m_data.get("tps"); ttft = m_data.get("ttft"); p = m_data.get("p")
                        if tps  is not None:
                            try:   m["tps_sum"]  += float(tps);  m["tps_count"]  += 1
                            except (TypeError, ValueError): pass
                        if ttft is not None:
                            try:   m["ttft_sum"] += float(ttft); m["ttft_count"] += 1
                            except (TypeError, ValueError): pass
                        if p is not None:
                            m["pass_total"] += 1
                            if p: m["pass_count"] += 1

                # User aggregation
                uid = cols[usi].strip() if usi is not None and usi < len(cols) else None
                if uid:
                    u_miner_counts[uid][hk] += 1
                    u = agg_users[uid]
                    u["invocation_count"] += 1
                    u["compute_seconds"]  += compute_val
                    u["hour_buckets"][hour_bucket] += 1
                    if chi is not None and chi < len(cols) and cols[chi]:
                        u["chute_ids"].add(cols[chi].strip())
                    if fni is not None and fni < len(cols) and cols[fni]:
                        u["function_names"][cols[fni].strip()] += 1
                    if imi is not None and imi < len(cols) and cols[imi]:
                        u["image_ids"][cols[imi].strip()] += 1
                    if iui is not None and iui < len(cols) and cols[iui]:
                        u["image_user_ids"][cols[iui].strip()] += 1
                    if pi is not None and pi < len(cols) and cols[pi]:
                        pid = cols[pi].strip()
                        if pid: u["parent_ids"].add(pid)
                    if ii is not None and ii < len(cols) and cols[ii]:
                        iid = cols[ii].strip()
                        if iid:
                            u["instance_ids"].add(iid)
                            net_instance_counts[iid] += 1

                    # Perimeter: function sequences (full population counters)
                    if fni is not None and fni < len(cols) and cols[fni].strip():
                        fn = cols[fni].strip()
                        fn_total[fn] += 1
                        if prev_fn is not None:
                            bigram_counts[(prev_fn, fn)] += 1
                        prev_fn = fn

                    # Perimeter: timestamp reservoir (inter-arrival analysis)
                    if sa_i is not None and sa_i < len(cols) and cols[sa_i].strip():
                        try:
                            ts = datetime.fromisoformat(
                                cols[sa_i].strip().replace("Z", "+00:00"))
                            ts_total_seen += 1
                            if len(ts_reservoir) < MAX_TS:
                                ts_reservoir.append(ts)
                            else:
                                j = _random.randint(0, ts_total_seen - 1)
                                if j < MAX_TS: ts_reservoir[j] = ts
                        except Exception: pass

                    # Perimeter: token Welford online variance
                    if m_data:
                        for tf in TOKEN_FIELDS:
                            v = m_data.get(tf)
                            if v is not None:
                                try:
                                    fv = float(v)
                                    if fv > 0:
                                        w = token_welford[tf]
                                        w["n"] += 1
                                        delta   = fv - w["mean"]
                                        w["mean"] += delta / w["n"]
                                        w["M2"]   += delta * (fv - w["mean"])
                                except (TypeError, ValueError): pass

            if hour_rows > 0: hours_fetched += 1
            else:             hours_failed  += 1

        except Exception:
            hours_failed += 1

        if (i + 1) % 24 == 0 or (i + 1) == len(urls):
            gc.collect()
            print(f"  ↳ {i+1}/{len(urls)} | {hours_fetched} with data | "
                  f"{total_rows:,} rows | {len(agg_miners)} miners | {len(agg_users)} users")

    print(f"\n  Hours with data: {hours_fetched} | Hours failed/missing: {hours_failed}")
    print(f"  Total raw rows processed: {total_rows:,}")
    print(f"  Unique miners seen: {len(agg_miners)}")
    print(f"  Unique demand-side users seen: {len(agg_users)}")
    if fields_info.get("header"):
        print(f"  📋 Fields ({len(fields_info['header'])}): {fields_info['header']}")

    # Finalise miner records
    all_chute_ids = set()
    miner_demand  = {}
    for hk, data in agg_miners.items():
        all_chute_ids.update(data["chute_ids"])
        miner_demand[hk] = {
            "hotkey":           hk,
            "uid":              data["uid"],
            "invocation_count": data["invocation_count"],
            "compute_seconds":  round(data["compute_seconds"], 2),
            "chute_diversity":  len(data["chute_ids"]),
            "avg_tps":          data["tps_sum"]  / data["tps_count"]  if data["tps_count"]  > 0 else 0.0,
            "avg_ttft":         data["ttft_sum"] / data["ttft_count"] if data["ttft_count"] > 0 else 0.0,
            "pass_rate":        data["pass_count"] / data["pass_total"] if data["pass_total"] > 0 else None,
        }
    print(f"  Unique chute IDs seen: {len(all_chute_ids)}")
    tps_n = sum(1 for m in miner_demand.values() if m["avg_tps"] > 0)
    print(f"  📊 Miners with native TPS (from metrics field): {tps_n}/{len(miner_demand)}")

    # Finalise user records
    total_inv = sum(d["invocation_count"] for d in agg_users.values()) or 1
    total_cmp = sum(d["compute_seconds"]  for d in agg_users.values()) or 1
    user_agg  = {}
    for user_id, data in agg_users.items():
        user_agg[user_id] = {
            "user_id":          user_id,
            "invocation_count": data["invocation_count"],
            "compute_seconds":  round(data["compute_seconds"], 2),
            "chute_count":      len(data["chute_ids"]),
            "chute_ids":        data["chute_ids"],
            "instance_ids":     data["instance_ids"],
            "parent_ids":       data["parent_ids"],
            "inv_share":        data["invocation_count"] / total_inv,
            "compute_share":    data["compute_seconds"]  / total_cmp,
            "function_names":   dict(data["function_names"]),
            "image_ids":        dict(data["image_ids"]),
            "image_user_ids":   dict(data["image_user_ids"]),
            "hour_buckets":     dict(data["hour_buckets"]),
        }

    # Finalise Welford token CV
    token_cv = {}
    for field, w in token_welford.items():
        if w["n"] >= 10:
            variance = w["M2"] / w["n"]
            std      = variance ** 0.5
            cv       = std / w["mean"] if w["mean"] > 0 else 0
            token_cv[field] = {
                "n": w["n"], "mean": round(w["mean"], 1),
                "std": round(std, 1), "cv": round(cv, 4),
            }

    net_total      = sum(n_miner_network.values()) or 1
    network_shares = {hk: c / net_total for hk, c in n_miner_network.items()}
    ts_reservoir.sort()

    print(f"  🔍 Perimeter: {len(ts_reservoir):,} timestamps | "
          f"{sum(bigram_counts.values()):,} bigrams | token fields: {list(token_cv.keys())}")

    perimeter = {
        "dominant_timestamps":     ts_reservoir,
        "dominant_bigram_counts":  bigram_counts,
        "dominant_trigram_counts": trigram_counts,
        "dominant_fn_totals":      fn_total,
        "dominant_token_cv":       token_cv,
        "user_miner_counts":       dict(u_miner_counts),
        "network_miner_shares":    network_shares,
        "network_instance_counts": dict(net_instance_counts),
        "dominant_inv_count":      0,
        "dominant_parent_ids":     set(),
        "dominant_instance_ids":   set(),
    }
    return miner_demand, user_agg, perimeter, fields_info, list(all_chute_ids)


def populate_perimeter_dominant(perimeter: dict, user_agg: dict, dominant_user_id: str) -> dict:
    """Copy dominant user's already-collected data into perimeter dict."""
    dom = user_agg.get(dominant_user_id, {})
    perimeter["dominant_inv_count"]    = dom.get("invocation_count", 0)
    perimeter["dominant_parent_ids"]   = dom.get("parent_ids",   set())
    perimeter["dominant_instance_ids"] = dom.get("instance_ids", set())
    perimeter["user_miner_counts"] = {
        dominant_user_id: perimeter["user_miner_counts"].get(dominant_user_id, {})
    }
    return perimeter


# ── Chute Metadata ────────────────────────────────────────────────────────────

def fetch_chute_metadata(chute_ids: set) -> dict:
    results = {}; ids = list(chute_ids)[:200]
    def _fetch_one(cid):
        try:
            r = requests.get(f"{CHUTES_API_BASE}/chutes/{cid}", timeout=8)
            if r.status_code == 200:
                d = r.json()
                return cid, {
                    "name":           d.get("name", ""),
                    "description":    (d.get("description") or "")[:120],
                    "model":          d.get("model_name") or d.get("model") or "",
                    "owner_username": d.get("username") or d.get("owner", {}).get("username", ""),
                    "public":         d.get("public", True),
                    "chute_type":     d.get("chute_type") or d.get("type", ""),
                    "created_at":     d.get("created_at") or d.get("created") or d.get("creation_date"),
                }
        except Exception: pass
        return cid, None
    print(f"  Fetching metadata for {len(ids)} chutes (40 workers)...")
    fetched = 0
    with ThreadPoolExecutor(max_workers=40) as ex:
        for cid, meta in ex.map(_fetch_one, ids):
            if meta: results[cid] = meta; fetched += 1
    print(f"  ✅ Chute metadata fetched: {fetched}/{len(ids)}")
    return results


# ── Root Validators ───────────────────────────────────────────────────────────

def get_root_validator_stakes(sub) -> dict:
    print("\nLoading Root metagraph (netuid=0)...")
    root_meta   = sub.metagraph(0)
    root_stakes = {}
    for uid in range(len(root_meta.hotkeys)):
        stake = float(root_meta.stake[uid])
        if stake >= ROOT_TAO_THRESHOLD:
            root_stakes[root_meta.hotkeys[uid]] = stake
    print(f"  Root validators with {ROOT_TAO_THRESHOLD:,.0f}+ TAO: {len(root_stakes)}")
    return root_stakes


# ── Dominant User Report ──────────────────────────────────────────────────────

def analyze_dominant_user(user_data: dict, chute_meta: dict, date: str) -> dict:
    user_id         = user_data["user_id"]
    inv_count       = user_data["invocation_count"]
    cmp_secs        = user_data["compute_seconds"]
    inv_share       = user_data["inv_share"]
    cmp_share       = user_data["compute_share"]
    fn_counts       = user_data.get("function_names", {})
    img_user_counts = user_data.get("image_user_ids", {})
    hour_counts     = user_data.get("hour_buckets",   {})
    chute_ids       = user_data.get("chute_ids",      set())

    total_fn  = sum(fn_counts.values())       or 1
    total_imu = sum(img_user_counts.values()) or 1
    fn_sorted       = sorted(fn_counts.items(),       key=lambda x: x[1], reverse=True)
    img_user_sorted = sorted(img_user_counts.items(), key=lambda x: x[1], reverse=True)
    img_sorted      = sorted(user_data.get("image_ids", {}).items(), key=lambda x: x[1], reverse=True)

    hour_of_day = defaultdict(int)
    for bucket, cnt in hour_counts.items():
        try: h = int(bucket.split("T")[1].split(":")[0]); hour_of_day[h] += cnt
        except Exception: pass

    peak_hour   = max(hour_of_day, key=lambda h: hour_of_day[h]) if hour_of_day else None
    trough_hour = min(hour_of_day, key=lambda h: hour_of_day[h]) if hour_of_day else None
    hourly_vals = [hour_of_day.get(h, 0) for h in range(24)]
    mean_hourly = sum(hourly_vals) / 24 if hourly_vals else 0
    cv = 0.0
    if mean_hourly > 0:
        variance = sum((v - mean_hourly) ** 2 for v in hourly_vals) / 24
        cv = (variance ** 0.5) / mean_hourly
    peak_vol   = max(hourly_vals) if hourly_vals else 1
    active_hrs = sum(1 for v in hourly_vals if v > peak_vol * 0.1)

    if cv > 1.5:   temporal_pattern = "BURSTY"
    elif cv > 0.7: temporal_pattern = "MIXED"
    else:          temporal_pattern = "STEADY"

    model_counts = defaultdict(int); owner_counts = defaultdict(int)
    type_counts  = defaultdict(int); known_chutes = 0
    for cid in chute_ids:
        meta = chute_meta.get(cid)
        if not meta: continue
        known_chutes += 1
        if meta["model"]:          model_counts[meta["model"]] += 1
        if meta["owner_username"]: owner_counts[meta["owner_username"]] += 1
        if meta["chute_type"]:     type_counts[meta["chute_type"]] += 1

    fn_lower = {k.lower(): v for k, v in fn_counts.items()}
    workload_signals = []
    if any("embed" in f for f in fn_lower):                                            workload_signals.append("EMBEDDING")
    if any(f in fn_lower for f in ("generate", "chat", "complete", "completions")):    workload_signals.append("TEXT_GENERATION")
    if any("vision" in f or "classify_image" in f for f in fn_lower):                 workload_signals.append("VISION")
    if any("audio" in f or "speech" in f or "transcribe" in f or "speak" in f for f in fn_lower): workload_signals.append("AUDIO")
    if not workload_signals: workload_signals.append("UNKNOWN")

    print(f"\n{SEPARATOR}")
    print(f"  DOMINANT USER DEEP ANALYSIS")
    print(f"  Observable metadata fingerprint — no encrypted content accessed")
    print(THIN_SEP)
    print(f"  User ID      : {user_id}")
    print(f"  Invocations  : {inv_count:,} 
