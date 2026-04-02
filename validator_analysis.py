"""
validator_analysis.py
=====================
Subnet Validator Analysis — Public Version
@im_perseverance

"APY without dilution is signal. Everything else is noise."

Analyses all active validators on a given subnet and ranks them by
estimated nominator APY (emission-only). Designed to be used after
subnet_analysis.py has identified a target subnet.

Layer 2 of the routing framework:
  - Subnet tool   → filters *where to play* (inflation traps)
  - Validator tool → optimizes *how to play* (execution efficiency)
  - Root tool     → defines *base layer allocation* (dead capital)

Usage:
    python validator_analysis.py --netuid 64 --stake 100

Arguments:
    --netuid    Subnet ID to analyse (required)
    --stake     Your intended stake in TAO (required)

Output:
    validator_analysis/SN{netuid}/snapshot_YYYY-MM-DD.csv
"""

import argparse
import bittensor as bt
import csv
from datetime import datetime, timezone
from pathlib import Path

# ── Config ─────────────────────────────────────────────────────────────────
OUTPUT_DIR      = Path("validator_analysis")
BLOCKS_PER_DAY  = 7200
BLOCKS_PER_YEAR = BLOCKS_PER_DAY * 365
MIN_TV          = 0.5
SEPARATOR       = "=" * 120
THIN_SEP        = "-" * 120

# ── Helpers ────────────────────────────────────────────────────────────────

def safe_float(val, default=0.0):
    try:
        return float(val)
    except Exception:
        return default

def fmt_apy(val):
    if val is None:
        return "   N/A   "
    return f"{val*100:+.1f}%"

def fmt_pct(val, decimals=1):
    if val is None:
        return "  N/A  "
    return f"{val*100:+.{decimals}f}%"

def get_delegate_data(sub, hotkey, netuid, price):
    """Get take rate, concentration, nominator count, and self-stake for a validator."""
    try:
        delegate = sub.get_delegate_by_hotkey(hotkey)
        if not delegate:
            return 0.18, None, None, "NO DATA", None, None, None
        
        take = delegate.take if hasattr(delegate, 'take') else 0.18
        
        # Nominator concentration for this subnet
        stakes = []
        owner_stake = 0.0
        owner_ss58 = getattr(delegate, "owner_ss58", None)
        
        for coldkey, subnet_stakes in delegate.nominators.items():
            alpha = safe_float(subnet_stakes.get(netuid, 0))
            tao_equiv = alpha * price
            if tao_equiv > 0:
                stakes.append(tao_equiv)
                if coldkey == owner_ss58:
                    owner_stake = tao_equiv
        
        if not stakes:
            return take, None, None, "NO DATA", None, None, None
        
        stakes.sort(reverse=True)
        total = sum(stakes)
        top1_pct = stakes[0] / total
        top3_pct = sum(stakes[:3]) / total
        self_stake_pct = owner_stake / total if total > 0 else 0.0
        nominator_count = len(stakes)
        
        if top1_pct > 0.50:
            flag = "🔴 HIGH"
        elif top1_pct > 0.25:
            flag = "🟡 MODERATE"
        else:
            flag = "🟢 DISTRIBUTED"
        
        return take, top1_pct, top3_pct, flag, self_stake_pct, nominator_count, top1_pct
    except Exception:
        return 0.18, None, None, "NO DATA", None, None, None

# ── Main ───────────────────────────────────────────────────────────────────

def run_analysis(netuid: int, my_stake: float):
    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    ts_str = now.strftime("%Y-%m-%d %H:%M:%S UTC")

    subnet_dir = OUTPUT_DIR / f"SN{netuid}"
    subnet_dir.mkdir(parents=True, exist_ok=True)

    print(SEPARATOR)
    print("  VALIDATOR ANALYSIS — Public Version")
    print("  @im_perseverance")
    print('  "APY without dilution is signal. Everything else is noise."')
    print(SEPARATOR)
    print(f"\n  Connecting to Bittensor network...")

    sub = bt.Subtensor(network="finney")
    current_block = sub.get_current_block()

    print(f"  Block     : {current_block:,}")
    print(f"  Timestamp : {ts_str}")
    print(f"  Target    : SN{netuid}")
    print(f"  Stake     : {my_stake:,.2f} TAO\n")

    # ── Subnet pool state ─────────────────────────────────────────────────
    all_subnets = sub.all_subnets()
    subnet = next((s for s in all_subnets if s.netuid == netuid), None)
    if not subnet:
        print(f"  ❌ SN{netuid} not found")
        return

    tao_per_block = safe_float(subnet.tao_in_emission)
    spot_price = safe_float(subnet.price)
    name = getattr(subnet, "subnet_name", f"SN{netuid}") or f"SN{netuid}"

    print(f"  {THIN_SEP}")
    print(f"  SN{netuid} — {name}")
    print(f"  {THIN_SEP}")
    print(f"  TAO / block      : {tao_per_block:.6f}")
    print(f"  Alpha price      : {spot_price:.6f} TAO")
    print()

    # ── Metagraph ─────────────────────────────────────────────────────────
    print(f"  Loading SN{netuid} metagraph...")
    meta = sub.metagraph(netuid=netuid)
    n_uids = len(meta.uids)

    # ── Validator filtering ───────────────────────────────────────────────
    validators = []
    total_stake = 0.0
    
    for uid in range(n_uids):
        if not meta.validator_permit[uid]:
            continue
        
        tv = safe_float(meta.validator_trust[uid])
        div = safe_float(meta.dividends[uid])
        stake = safe_float(meta.stake[uid])
        e_val = safe_float(meta.incentive[uid])
        
        if tv < MIN_TV or div <= 0 or stake <= 0:
            continue
        
        validators.append({
            "uid": uid,
            "hotkey": meta.hotkeys[uid],
            "stake": stake,
            "div": div,
            "e_val": e_val,
            "tv": tv,
        })
        total_stake += stake

    if not validators:
        print(f"  ❌ No qualifying validators on SN{netuid} (TV >= {MIN_TV}, dividend > 0)")
        return

    total_e = sum(v["e_val"] for v in validators)
    if total_e <= 0:
        total_e = 1.0

    print(f"  Validators found: {len(validators)}\n")

    # ── Calculate yields and rank ─────────────────────────────────────────
    results = []
    
    for v in validators:
        take, top1_pct, top3_pct, conc_flag, self_stake_pct, nom_count, _ = get_delegate_data(
            sub, v["hotkey"], netuid, spot_price
        )
        
        e_share = v["e_val"] / total_e
        
        # Emission routing efficiency = e_share / (stake / total_stake)
        stake_weight = v["stake"] / total_stake if total_stake > 0 else 0
        efficiency = e_share / stake_weight if stake_weight > 0 else 0
        
        # Post-entry yield calculation
        pool_total = v["stake"] + my_stake
        your_share = my_stake / pool_total if pool_total > 0 else 0
        
        validator_tao_per_block = tao_per_block * e_share
        your_tao_per_day = validator_tao_per_block * (1 - take) * your_share * BLOCKS_PER_DAY
        emission_apy = (your_tao_per_day * 365 / my_stake) if my_stake > 0 else 0
        
        # Div/Inc ratio (pure validator vs validator-miner)
        div_inc_ratio = v["div"] / v["e_val"] if v["e_val"] > 0 else None
        
        results.append({
            "uid": v["uid"],
            "hotkey": v["hotkey"],
            "hotkey_short": v["hotkey"][:8] + "...",
            "stake": v["stake"],
            "div": v["div"],
            "e_share": e_share,
            "efficiency": efficiency,
            "take": take,
            "tv": v["tv"],
            "emission_apy": emission_apy,
            "top1_pct": top1_pct,
            "top3_pct": top3_pct,
            "conc_flag": conc_flag,
            "self_stake_pct": self_stake_pct,
            "nominator_count": nom_count,
            "div_inc_ratio": div_inc_ratio,
        })

    # Sort by emission APY (post-entry, emission-only)
    results.sort(key=lambda r: r["emission_apy"], reverse=True)

    # ── Console report ────────────────────────────────────────────────────
    print(SEPARATOR)
    print(f"  VALIDATOR RANKINGS — SN{netuid} {name}  |  Stake: {my_stake:,.2f} TAO")
    print(f"  Sorted by: Emission APY (post-entry, emission-only)")
    print(THIN_SEP)
    print(f"  {'UID':<6} {'Stake':>12} {'E Share':>8} {'Eff':>6} {'Take':>6} {'TV':>6} "
          f"{'D/I':>6} {'Emiss APY':>11} {'Stake Dist (Noms)':>18} {'Self%':>6}  {'Hotkey'}")
    print(THIN_SEP)

    # Concentration labels for stake distribution
    conc_labels = {
        "🔴 HIGH":     "🔴 High",
        "🟡 MODERATE": "🟡 Moderate",
        "🟢 DISTRIBUTED": "🟢 Distributed",
        "NO DATA":     "  N/A",
    }

    for r in results:
        eff_str = f"{r['efficiency']:.2f}x" if r['efficiency'] > 0 else "N/A"
        take_str = f"{r['take']:.0%}" if r['take'] is not None else "?%"
        di_str = f"{r['div_inc_ratio']:.1f}" if r['div_inc_ratio'] is not None else "N/A"
        
        # Combine concentration flag with nominator count
        if r['nominator_count'] is not None:
            conc_display = f"{conc_labels.get(r['conc_flag'], r['conc_flag'][:10])} ({r['nominator_count']})"
        else:
            conc_display = conc_labels.get(r['conc_flag'], r['conc_flag'][:10])
        
        self_str = f"{r['self_stake_pct']:.0%}" if r['self_stake_pct'] is not None else "N/A"
        
        print(
            f"  {r['uid']:<6} {r['stake']:>12,.0f} {r['e_share']:>8.4f} {eff_str:>6} "
            f"{take_str:>6} {r['tv']:>6.3f} {di_str:>6} "
            f"{fmt_apy(r['emission_apy']):>11} {conc_display:>18} {self_str:>6}  {r['hotkey']}"
        )

    print(SEPARATOR)
    
    # Methodology notes
    print("  📐 METHODOLOGY NOTES")
    print(THIN_SEP)
    print("  • APY accounts for your stake impact (post-entry dilution)")
    print("  • E Share = validator share of subnet emissions (routing weight)")
    print("  • Efficiency = E Share / Stake Weight (>1x = overperformer)")
    print("  • Stake Dist = concentration + nominator count (e.g., \"🔴 High (3)\" = 3 nominators, one dominant)")
    print("  • D/I = Div/Inc ratio (high = pure validator, low = validator-miner)")
    print("  • Validator ranking is emission-only. Subnet selection determines dilution.")
    print(THIN_SEP)
    print()

    if results:
        best = results[0]
        print(f"  ✅ TOP: UID {best['uid']}  {best['hotkey']}")
        print(f"     Estimated emission APY : {fmt_apy(best['emission_apy'])}")
        print(f"     Take                   : {best['take']:.1%}")
        print(f"     Validator Trust        : {best['tv']:.4f}")
        print(f"     Pool size              : {best['stake']:,.0f} TAO")
        print(f"     Efficiency             : {best['efficiency']:.2f}x emission vs stake weight")
        if best['top1_pct'] is not None:
            print(f"     Stake distribution     : {best['conc_flag']} (Top1: {best['top1_pct']:.1%}, {best['nominator_count']} nominators)")
        if best['self_stake_pct'] is not None:
            print(f"     Self-stake             : {best['self_stake_pct']:.1%}")
        if best['div_inc_ratio'] is not None:
            role = "pure validator" if best['div_inc_ratio'] > 5 else "validator-miner" if best['div_inc_ratio'] < 1 else "mixed"
            print(f"     Role                   : {role} (D/I = {best['div_inc_ratio']:.1f})")
    
    print(f"\n  Snapshot block : {current_block:,}")
    print(f"  Timestamp      : {ts_str}")
    print(SEPARATOR)

    # ── Save CSV ──────────────────────────────────────────────────────────
    csv_path = subnet_dir / f"snapshot_{date_str}.csv"
    fieldnames = [
        "uid", "hotkey", "hotkey_short", "stake", "div", "e_share",
        "efficiency", "take", "tv", "emission_apy", 
        "top1_pct", "top3_pct", "conc_flag", "self_stake_pct", 
        "nominator_count", "div_inc_ratio",
    ]
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(results)

    print(f"💾  Snapshot saved: {csv_path}\n")

# ── Entry point ────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Subnet Validator Analysis — Public Version"
    )
    parser.add_argument("--netuid", type=int, required=True,
                        help="Subnet ID to analyse")
    parser.add_argument("--stake", type=float, required=True,
                        help="Your intended stake in TAO")
    args = parser.parse_args()
    run_analysis(netuid=args.netuid, my_stake=args.stake)

if __name__ == "__main__":
    main()
