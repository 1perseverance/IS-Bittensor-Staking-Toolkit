"""
root_analysis.py
================
Root Validator Analysis — Public Version
@im_perseverance

Analyses all root validators and ranks them by estimated nominator yield.

Root dividends are empirically driven by emission flow. Ghost validators
(stake > 0, dividend = 0) are flagged as they earn nothing for nominators.

Usage:
    python root_analysis.py --stake 100

Arguments:
    --stake     Your intended stake in TAO (required)

Output:
    root_analysis/snapshot_YYYY-MM-DD.csv
"""

import argparse
import bittensor as bt
import csv
from datetime import datetime, timezone
from pathlib import Path

# ── Config ─────────────────────────────────────────────────────────────────
OUTPUT_DIR     = Path("root_analysis")
BLOCKS_PER_DAY = 7200
MIN_STAKE      = 1000.0
SEPARATOR      = "=" * 120
THIN_SEP       = "-" * 120

# ── Helpers ────────────────────────────────────────────────────────────────

def safe_float(val, default=0.0):
    try:
        return float(val)
    except Exception:
        return default

def fmt_apy(val):
    if val is None:
        return "   N/A   "
    return f"{val*100:+.4f}%"

# ── Main ───────────────────────────────────────────────────────────────────

def run_analysis(my_stake: float):
    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    ts_str = now.strftime("%Y-%m-%d %H:%M:%S UTC")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print(SEPARATOR)
    print("  ROOT VALIDATOR ANALYSIS — Public Version")
    print("  @im_perseverance")
    print(SEPARATOR)
    print(f"\n  Connecting to Bittensor network...")

    sub = bt.Subtensor(network="finney")
    current_block = sub.get_current_block()

    print(f"  Block     : {current_block:,}")
    print(f"  Timestamp : {ts_str}")
    print(f"  Stake     : {my_stake:,.2f} TAO\n")

    # ── Root metagraph ────────────────────────────────────────────────────
    print("  Loading root metagraph (netuid=0)...")
    meta = sub.metagraph(netuid=0)
    n_uids = len(meta.uids)

    # ── Fetch take rates and delegate info ────────────────────────────────
    print("  Fetching delegate data...\n")

    results = []
    ghost_count = 0

    for uid in range(n_uids):
        stake = safe_float(meta.stake[uid])
        if stake < MIN_STAKE:
            continue

        hotkey = meta.hotkeys[uid]
        div = safe_float(meta.dividends[uid])
        tv = safe_float(meta.validator_trust[uid])
        
        is_ghost = div == 0 and stake >= MIN_STAKE
        if is_ghost:
            ghost_count += 1

        # Fetch delegate info for take and concentration
        try:
            delegate = sub.get_delegate_by_hotkey(hotkey)
            take = delegate.take if delegate else 0.18
            
            # Nominator concentration on root
            stakes = []
            owner_stake = 0.0
            owner_ss58 = getattr(delegate, "owner_ss58", None) if delegate else None
            
            if delegate and delegate.nominators:
                for coldkey, subnet_stakes in delegate.nominators.items():
                    root_balance = safe_float(subnet_stakes.get(0, 0))
                    if root_balance > 0:
                        stakes.append(root_balance)
                        if coldkey == owner_ss58:
                            owner_stake = root_balance
        except Exception:
            take = 0.18
            stakes = []
            owner_stake = 0.0

        # Calculate yield estimate
        pool_total = stake + my_stake
        your_share = my_stake / pool_total if pool_total > 0 else 0
        your_yield = your_share * div * (1 - take)

        # Concentration metrics
        if stakes:
            stakes.sort(reverse=True)
            total = sum(stakes)
            top1_pct = stakes[0] / total if total > 0 else None
            top3_pct = sum(stakes[:3]) / total if total > 0 else None
            self_pct = owner_stake / total if total > 0 else 0.0
            
            if top1_pct and top1_pct > 0.50:
                conc_flag = "🔴 HIGH"
            elif top1_pct and top1_pct > 0.25:
                conc_flag = "🟡 MODERATE"
            else:
                conc_flag = "🟢 DISTRIBUTED"
        else:
            top1_pct = top3_pct = self_pct = None
            conc_flag = "NO DATA"

        # Subnet coverage (validator permits count)
        subnet_count = None
        if delegate and hasattr(delegate, "validator_permits"):
            subnet_count = len(delegate.validator_permits) if delegate.validator_permits else 0

        results.append({
            "uid": uid,
            "hotkey": hotkey,
            "hotkey_short": hotkey[:8] + "...",
            "stake": stake,
            "div": div,
            "take": take,
            "tv": tv,
            "your_yield": your_yield,
            "is_ghost": is_ghost,
            "top1_pct": top1_pct,
            "top3_pct": top3_pct,
            "conc_flag": conc_flag,
            "self_stake_pct": self_pct,
            "subnet_count": subnet_count,
        })

    # Sort active validators by yield
    active = [r for r in results if not r["is_ghost"]]
    ghosts = [r for r in results if r["is_ghost"]]
    active.sort(key=lambda r: r["your_yield"], reverse=True)

    # ── Console report ────────────────────────────────────────────────────
    print(SEPARATOR)
    print(f"  ROOT VALIDATOR RANKINGS  |  Stake: {my_stake:,.2f} TAO")
    print(f"  Active validators (dividend > 0, stake >= {MIN_STAKE:,.0f} TAO)")
    print(THIN_SEP)
    print(f"  {'UID':<6} {'Pool Size':>14} {'Div':>10} {'Take':>7} {'TV':>7} "
          f"{'Your Yield':>12} {'Conc':>14}  {'Hotkey'}")
    print(THIN_SEP)

    for r in active:
        print(
            f"  {r['uid']:<6} {r['stake']:>14,.0f} {r['div']:>10.6f} "
            f"{r['take']:>6.1%} {r['tv']:>7.4f} "
            f"{r['your_yield']:>12.6f} {r['conc_flag']:<14}  {r['hotkey']}"
        )

    print(SEPARATOR)
    print(f"  Active validators  : {len(active)}")
    print(f"  Ghost validators   : {ghost_count} (stake > 0, dividend = 0)")

    # Ghost validator report
    if ghosts:
        ghosts_sorted = sorted(ghosts, key=lambda x: x["stake"], reverse=True)
        print(f"\n  👻 GHOST VALIDATORS (earning zero dividends)")
        print(THIN_SEP)
        print(f"  {'UID':<6} {'Pool Size':>14} {'Hotkey'}")
        print(THIN_SEP)
        for g in ghosts_sorted[:15]:
            print(f"  {g['uid']:<6} {g['stake']:>14,.0f}  {g['hotkey']}")
        if len(ghosts) > 15:
            print(f"  ... and {len(ghosts) - 15} more")

    # Zero self-stake warning
    no_skin = [r for r in active if r["self_stake_pct"] is not None and r["self_stake_pct"] == 0]
    if no_skin:
        print(f"\n  ⚠️  ZERO SELF-STAKE ON ROOT (no skin in the game)")
        print(THIN_SEP)
        for r in no_skin[:10]:
            print(f"  UID {r['uid']} {r['hotkey_short']}  |  Take: {r['take']:.1%}  |  Yield: {r['your_yield']:.6f}")

    # Recommendation
    if active:
        best = active[0]
        print(f"\n  ✅ TOP: UID {best['uid']}  {best['hotkey']}")
        print(f"     Estimated yield    : {best['your_yield']:.6f}")
        print(f"     Dividend           : {best['div']:.6f}")
        print(f"     Take               : {best['take']:.1%}")
        print(f"     Validator Trust    : {best['tv']:.4f}")
        print(f"     Pool size          : {best['stake']:,.0f} TAO")
        if best["top1_pct"] is not None:
            print(f"     Concentration      : {best['conc_flag']} (Top1: {best['top1_pct']:.1%})")
        if best["subnet_count"] is not None:
            print(f"     Subnet coverage    : {best['subnet_count']} subnet(s)")

    print(f"\n  Snapshot block : {current_block:,}")
    print(f"  Timestamp      : {ts_str}")
    print(SEPARATOR)

    # ── Save CSV ──────────────────────────────────────────────────────────
    csv_path = OUTPUT_DIR / f"snapshot_{date_str}.csv"
    fieldnames = [
        "uid", "hotkey", "hotkey_short", "stake", "div", "take", "tv",
        "your_yield", "is_ghost", "top1_pct", "top3_pct", "conc_flag",
        "self_stake_pct", "subnet_count",
    ]
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(active + ghosts)

    print(f"💾  Snapshot saved: {csv_path}\n")

# ── Entry point ────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Root Validator Analysis — Public Version"
    )
    parser.add_argument("--stake", type=float, required=True,
                        help="Your intended stake in TAO")
    args = parser.parse_args()
    run_analysis(my_stake=args.stake)

if __name__ == "__main__":
    main()
