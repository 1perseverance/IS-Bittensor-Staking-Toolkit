"""
IS_subnet_analysis.py
==================
Subnet Staking Snapshot — Public Version
@im_perseverance

"APY without dilution is signal. Everything else is noise."

For each active subnet, computes:
  - Emission APY (TAO emission / TAO reserves, annualised)
  - Gross inflation (alpha minting rate, annualised)
  - Net supply delta (alpha supply change, requires 2+ runs)
  - Nominal APY (Emission + Price momentum — not dilution-adjusted)
  - Liquidation haircut (loss/gain if subnet deregistered)
  - EMA band / lag trap (spot vs moving_price)

Ranking: Emission APY minus positive dilution only (conservative real yield proxy)

Usage:
    python subnet_analysis.py

Output:
    subnet_analysis_snapshot_YYYY-MM-DD.csv
"""

import bittensor as bt
import csv
from datetime import datetime, timezone
from pathlib import Path

# ── Config ─────────────────────────────────────────────────────────────────
BLOCKS_PER_DAY    = 7200
BLOCKS_PER_YEAR   = BLOCKS_PER_DAY * 365
MIN_TAO_EMISSION  = 0.001
EMA_LAG_THRESHOLD = -0.15

OUTPUT_DIR = Path("subnet_analysis")
SEPARATOR  = "=" * 100
THIN_SEP   = "-" * 100

# ── Helpers ────────────────────────────────────────────────────────────────

def safe_float(val, default=0.0):
    try:
        return float(val)
    except Exception:
        return default

def fmt_pct(val, decimals=1):
    if val is None:
        return "  N/A  "
    return f"{val*100:+.{decimals}f}%"

def fmt_apy(val):
    if val is None:
        return "   N/A   "
    return f"{val*100:+.1f}%"

def load_previous_snapshot(date_str):
    """Load alpha_outstanding from most recent previous CSV."""
    csv_files = sorted(OUTPUT_DIR.glob("subnet_analysis_snapshot_*.csv"), reverse=True)
    for f in csv_files:
        if date_str not in f.name:
            prev_data = {}
            try:
                with open(f) as csvfile:
                    reader = csv.DictReader(csvfile)
                    for row in reader:
                        netuid = row.get("netuid")
                        alpha = row.get("alpha_outstanding")
                        if netuid and alpha:
                            prev_data[int(netuid)] = float(alpha)
                return prev_data, f.name
            except Exception:
                continue
    return None, None

# ── Real yield proxy (conservative, no deflation boost) ───────────────────

def real_yield_proxy(r):
    """Ranking key: emission APY minus positive dilution only."""
    emit = r["emission_apy"]
    if emit is None:
        return -999
    
    net = r["net_supply_delta"]
    if net is None or net < 0:
        net = 0
    
    return emit - net

# ── Main ───────────────────────────────────────────────────────────────────

def run_snapshot():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    ts_str = now.strftime("%Y-%m-%d %H:%M:%S UTC")

    print(SEPARATOR)
    print("  SUBNET STAKING SNAPSHOT — Public Version")
    print("  @im_perseverance")
    print('  "APY without dilution is signal. Everything else is noise."')
    print(SEPARATOR)
    print(f"\n  Connecting to Bittensor network...")

    sub = bt.Subtensor(network="finney")
    current_block = sub.get_current_block()
    all_subnets = sub.all_subnets()

    print(f"  Block     : {current_block:,}")
    print(f"  Timestamp : {ts_str}")
    print(f"  Subnets   : {len(all_subnets)} total\n")

    # Load previous snapshot for net supply delta
    prev_alpha_map, prev_file = load_previous_snapshot(date_str)
    if prev_alpha_map:
        print(f"  📂 Previous snapshot: {prev_file}")
        print(f"     Net supply delta will be calculated for subnets with prior data.\n")
    else:
        print(f"  ℹ️  No previous snapshot found.")
        print(f"     Net supply delta requires 2+ runs. Run again tomorrow.\n")

    # Methodology notice (printed once)
    print(THIN_SEP)
    print("  📐 METHODOLOGY NOTES (Public Version)")
    print(THIN_SEP)
    print("  • Rankings sorted by: Emission APY minus positive dilution only")
    print("    (deflation is not added back — conservative real yield proxy)")
    print("")
    print("  • High nominal APY can be offset by high dilution")
    print("")
    print("  • Net supply delta requires 2+ runs. Annualisation assumes ~1 day gap.")
    print("    Run again tomorrow for dilution-adjusted metrics.")
    print("")
    print("  • 'Nominal APY' = Emission + Price momentum (not dilution-adjusted)")
    print("")
    print("  • Gross Inflation = raw alpha minting rate (before buybacks/burns)")
    print(THIN_SEP)
    print()

    # Delegate take cache
    delegate_takes = {}
    _take_misses = set()

    def get_take(hotkey):
        if hotkey in delegate_takes:
            return delegate_takes[hotkey]
        if hotkey in _take_misses:
            return None
        try:
            take = sub.get_delegate_take(hotkey)
            delegate_takes[hotkey] = take
            return take
        except Exception:
            _take_misses.add(hotkey)
            return None

    results = []

    for s in all_subnets:
        netuid = s.netuid
        if netuid == 0:
            continue

        tao_emission = safe_float(s.tao_in_emission)
        low_emission = tao_emission < MIN_TAO_EMISSION

        spot_price    = safe_float(s.price)
        moving_price  = safe_float(s.moving_price)
        tao_reserves  = safe_float(s.tao_in)
        alpha_out     = safe_float(s.alpha_out)
        alpha_out_emission = safe_float(s.alpha_out_emission)
        name = getattr(s, "subnet_name", f"SN{netuid}") or f"SN{netuid}"

        # EMA momentum and band
        momentum_30d = (spot_price - moving_price) / moving_price if moving_price > 0 else None
        ema_lag_flag = momentum_30d is not None and momentum_30d < EMA_LAG_THRESHOLD

        if momentum_30d is None:
            ema_band = "N/A"
        elif momentum_30d > 0.20:
            ema_band = "PREMIUM"
        elif momentum_30d < -0.20:
            ema_band = "DISCOUNT"
        elif ema_lag_flag:
            ema_band = "⚠️ LAG TRAP"
        else:
            ema_band = "IN BAND"

        # Emission APY
        emission_apy = None
        if tao_reserves > 0 and tao_emission > 0:
            emission_apy = (tao_emission * BLOCKS_PER_YEAR) / tao_reserves

        # Gross inflation (protocol-native dilution)
        gross_inflation = None
        if alpha_out > 0 and alpha_out_emission > 0:
            gross_inflation = (alpha_out_emission * BLOCKS_PER_YEAR) / alpha_out

        # Price APY (30d)
        price_apy_30d = momentum_30d * (365 / 30) if momentum_30d is not None else None
        
        # Nominal APY (emission + price momentum — not dilution-adjusted)
        nominal_apy = (emission_apy + price_apy_30d) if emission_apy is not None and price_apy_30d is not None else None

        # Liquidation price & haircut
        liquidation_price = tao_reserves / alpha_out if alpha_out > 0 else None
        liquidation_haircut = (
            (spot_price - liquidation_price) / spot_price
            if spot_price > 0 and liquidation_price is not None
            else None
        )

        # Net supply delta (requires previous snapshot)
        net_supply_delta = None
        if prev_alpha_map and netuid in prev_alpha_map and prev_alpha_map[netuid] > 0:
            prev_alpha = prev_alpha_map[netuid]
            delta_pct = (alpha_out - prev_alpha) / prev_alpha
            net_supply_delta = delta_pct * 365  # annualised (assumes ~1 day gap)

        # Best validator (simple, no trajectory)
        best_val = None
        if not low_emission:
            try:
                meta = sub.metagraph(netuid)
                n_uids = len(meta.uids)
                total_dividends = sum(safe_float(meta.dividends[i]) for i in range(n_uids))
                vali_uids = [i for i, vp in enumerate(meta.validator_permit) if vp]
                best_apy = -999
                for uid in vali_uids:
                    stake = safe_float(meta.stake[uid])
                    if stake < 1000:
                        continue
                    div = safe_float(meta.dividends[uid])
                    if total_dividends > 0:
                        raw_apy = (div / stake) * BLOCKS_PER_YEAR if stake > 0 else 0
                        take = get_take(meta.hotkeys[uid])
                        est_apy = raw_apy * (1 - take) if take is not None else raw_apy
                        if est_apy > best_apy:
                            best_apy = est_apy
                            best_val = {
                                "uid": uid,
                                "hotkey": meta.hotkeys[uid][:8] + "...",
                                "take": take,
                                "apy": est_apy,
                            }
            except Exception:
                pass

        results.append({
            "netuid": netuid,
            "name": name,
            "tao_emission": tao_emission,
            "low_emission": low_emission,
            "spot_price": spot_price,
            "moving_price": moving_price,
            "momentum_30d": momentum_30d,
            "ema_band": ema_band,
            "ema_lag_flag": ema_lag_flag,
            "emission_apy": emission_apy,
            "price_apy_30d": price_apy_30d,
            "nominal_apy": nominal_apy,
            "gross_inflation": gross_inflation,
            "net_supply_delta": net_supply_delta,
            "liquidation_haircut": liquidation_haircut,
            "alpha_outstanding": alpha_out,
            "best_val_uid": best_val["uid"] if best_val else None,
            "best_val_hotkey": best_val["hotkey"] if best_val else None,
            "best_val_take": best_val["take"] if best_val else None,
            "best_val_apy": best_val["apy"] if best_val else None,
        })

    # Sort by real yield proxy (emission minus positive dilution)
    results.sort(key=real_yield_proxy, reverse=True)

    # Console output
    print(SEPARATOR)
    print("  SUBNET RANKINGS — Sorted by Emission APY (net of positive dilution)")
    print(THIN_SEP)
    print(f"  {'#':<4} {'SN':<6} {'Name':<24} {'Emiss APY':>10} {'Nominal':>10} "
          f"{'Gross Inf':>10} {'Net Δ':>8} {'Liq':>5} {'EMA Band':>12} {'Best Val'}")
    print(THIN_SEP)

    active_results = [r for r in results if not r["low_emission"]]
    for i, r in enumerate(active_results[:50], 1):
        net_str = fmt_pct(r["net_supply_delta"]) if r["net_supply_delta"] is not None else "  N/A  "
        lh = r["liquidation_haircut"]
        liq_flag = "🟢" if lh is not None and lh < -0.1 else "🔴" if lh is not None and lh > 0.5 else "  "
        band_str = f"⚠️{r['ema_band']}" if r["ema_lag_flag"] else r["ema_band"]
        best_str = f"{r['best_val_hotkey']} ({r['best_val_apy']*100:.1f}%)" if r["best_val_apy"] else "N/A"
        print(
            f"  #{i:<3} SN{r['netuid']:<4} {r['name'][:24]:<24} "
            f"{fmt_apy(r['emission_apy']):>10} {fmt_apy(r['nominal_apy']):>10} "
            f"{fmt_pct(r['gross_inflation']):>10} {net_str:>8} {liq_flag:>4} {band_str:>12}  {best_str}"
        )

    # Low emission subnets
    low_results = [r for r in results if r["low_emission"]]
    if low_results:
        print(f"\n{THIN_SEP}")
        print(f"  LOW EMISSION SUBNETS (<{MIN_TAO_EMISSION} TAO/block) — {len(low_results)}")
        print(THIN_SEP)
        for r in low_results[:20]:
            print(f"  SN{r['netuid']:<4} {r['name'][:30]:<30} {r['tao_emission']:.6f} TAO/block")

    print(SEPARATOR)
    print(f"  Subnets analysed: {len(results)} total, {len(active_results)} active, {len(low_results)} low emission")
    if not prev_alpha_map:
        print(f"\n  💡 Net supply delta will appear after a second run.")
        print(f"     Run again tomorrow with the same output directory.")
    print(SEPARATOR)

    # CSV output
    csv_path = OUTPUT_DIR / f"subnet_analysis_snapshot_{date_str}.csv"
    fieldnames = [
        "netuid", "name", "tao_emission", "low_emission",
        "spot_price", "moving_price", "momentum_30d", "ema_band", "ema_lag_flag",
        "emission_apy", "price_apy_30d", "nominal_apy",
        "gross_inflation", "net_supply_delta", "liquidation_haircut", "alpha_outstanding",
        "best_val_uid", "best_val_hotkey", "best_val_take", "best_val_apy",
    ]
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(results)

    print(f"\n💾  Snapshot saved: {csv_path}")
    print(f"\n  APY without dilution is signal. Everything else is noise.\n")

if __name__ == "__main__":
    run_snapshot()
