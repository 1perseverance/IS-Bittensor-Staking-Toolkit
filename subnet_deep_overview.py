 import bittensor as bt
from datetime import datetime, timezone

POOL_CUTOFF = 1000
BLOCKS_PER_DAY = 7200
MIN_EMISSION = 0.001

my_stake = float(input("Enter your stake amount in TAO: "))

print("\nConnecting to Bittensor network...")
sub = bt.Subtensor()
current_block = sub.get_current_block()
timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
print(f"Block: {current_block} | Timestamp: {timestamp}\n")

print("Loading all subnet data...")
all_s = sub.all_subnets()

print("Loading root metagraph for take rates...")
root_meta = sub.metagraph(netuid=0)

take_rates = {}
for uid in range(len(root_meta.hotkeys)):
    hotkey = root_meta.hotkeys[uid]
    try:
        delegate_info = sub.get_delegate_by_hotkey(hotkey)
        take_rates[hotkey] = delegate_info.take if delegate_info else 0.18
    except:
        take_rates[hotkey] = 0.18

def get_subnet_concentration(sub, metagraph, netuid, price):
    try:
        subnet_stakes = {}
        for uid in range(len(metagraph.hotkeys)):
            hotkey = metagraph.hotkeys[uid]
            try:
                delegate = sub.get_delegate_by_hotkey(hotkey)
                if not delegate or not delegate.nominators:
                    continue
                for coldkey, subnet_stakes_dict in delegate.nominators.items():
                    alpha = float(subnet_stakes_dict.get(netuid, 0))
                    tao_equiv = alpha * price
                    if tao_equiv > 0:
                        subnet_stakes[coldkey] = subnet_stakes.get(coldkey, 0) + tao_equiv
            except:
                continue
        if not subnet_stakes:
            return None, None, None, "NO DATA"
        sorted_stakes = sorted(subnet_stakes.values(), reverse=True)
        total = sum(sorted_stakes)
        if total == 0:
            return None, None, None, "NO DATA"
        top1_pct = sorted_stakes[0] / total
        top3_pct = sum(sorted_stakes[:3]) / total
        top5_pct = sum(sorted_stakes[:5]) / total
        if top1_pct > 0.50 or top5_pct > 0.70:
            flag = "🔴 HIGH RISK"
        elif top1_pct > 0.30:
            flag = "🟡 MODERATE"
        else:
            flag = "🟢 DISTRIBUTED"
        return top1_pct, top3_pct, top5_pct, flag
    except:
        return None, None, None, "NO DATA"

subnet_input = input("\nScan all subnets? Press Enter, or enter specific IDs (e.g. 64,44,120): ").strip()

if subnet_input:
    target_netuids = [int(x.strip()) for x in subnet_input.split(",")]
    active_subnets = [s for s in all_s if s.netuid in target_netuids]
    print(f"Scanning {len(active_subnets)} selected subnets: {target_netuids}")
else:
    active_subnets = [
        s for s in all_s
        if s.netuid != 0 and float(s.tao_in_emission) >= MIN_EMISSION
    ]
    print(f"Found {len(active_subnets)} subnets with emission >= {MIN_EMISSION} TAO/block")

print("Scanning metagraphs...\n")

results = []
for subnet in active_subnets:
    netuid = subnet.netuid
    tao_per_block = float(subnet.tao_in_emission)
    current_price = float(subnet.price)
    moving_price = float(subnet.moving_price)
    name = subnet.subnet_name or f"SN{netuid}"

    price_momentum = (current_price - moving_price) / moving_price if moving_price > 0 else 0
    price_apy = price_momentum * (365 / 30) * 100

    try:
        metagraph = sub.metagraph(netuid=netuid)
    except Exception as e:
        print(f"  SN{netuid} metagraph failed: {e}")
        continue

    div_uids = [
        (uid, float(metagraph.D[uid]), float(metagraph.E[uid]), float(metagraph.S[uid]), float(metagraph.Tv[uid]), metagraph.hotkeys[uid])
        for uid in range(len(metagraph.hotkeys))
        if float(metagraph.D[uid]) > 0
        and float(metagraph.E[uid]) > 0
        and float(metagraph.Tv[uid]) > 0.5
    ]

    if not div_uids:
        print(f"  SN{netuid:<4} {name:<20} — no active validators, skipping")
        continue

    total_E_validators = sum(e for _, _, e, _, _, _ in div_uids)

    best = None
    for uid, div, e_val, stake, tv, hotkey in div_uids:
        take = take_rates.get(hotkey, 0.18)
        e_share = e_val / total_E_validators if total_E_validators > 0 else 0
        validator_tao_per_block = tao_per_block * e_share
        pool_total = stake + my_stake
        your_share = my_stake / pool_total
        your_tao_per_day = validator_tao_per_block * (1 - take) * your_share * BLOCKS_PER_DAY
        emission_apy = (your_tao_per_day * 365 / my_stake) * 100 if my_stake > 0 else 0
        combined_apy = emission_apy + price_apy
        status = "Large Pool" if stake >= POOL_CUTOFF else "Small Pool"

        if best is None or combined_apy > best[6]:
            best = (uid, stake, take, tv, your_tao_per_day, emission_apy, combined_apy, status, hotkey)

    if best:
        best_uid, best_stake, best_take, best_tv, best_tao_day, best_emission_apy, best_combined_apy, best_status, best_hk = best
        top1_pct, top3_pct, top5_pct, conc_flag = get_subnet_concentration(sub, metagraph, netuid, current_price)
        top1_str = f"{top1_pct:.1%}" if top1_pct is not None else "N/A"
        top3_str = f"{top3_pct:.1%}" if top3_pct is not None else "N/A"
        top5_str = f"{top5_pct:.1%}" if top5_pct is not None else "N/A"
        results.append((netuid, name, len(div_uids), best_uid, best_stake, best_take, best_tv, best_tao_day, best_emission_apy, price_apy, best_combined_apy, best_status, best_hk, tao_per_block, top1_str, top3_str, top5_str, conc_flag))
        print(f"  SN{netuid:<4} {name:<20} — combined APY: {best_combined_apy:.4f}% | {conc_flag}")

results.sort(key=lambda x: x[10], reverse=True)

print(f"\n{'='*220}")
print(f"Cross-Subnet Yield Rankings for {my_stake} TAO\n")
print(f"{'SN':<6} {'Name':<22} {'Vals':<6} {'Best UID':<10} {'Pool Stake':<14} {'Take':<7} {'Tv':<8} {'Emission APY':<14} {'Price APY':<12} {'Combined APY':<14} {'Top1':<8} {'Top3':<8} {'Top5':<8} {'Concentration':<16} {'TAO/block'}")
print("-" * 220)

for netuid, name, n_validators, best_uid, best_stake, best_take, best_tv, best_tao_day, best_emission_apy, price_apy, best_combined_apy, best_status, best_hk, tao_per_block, top1_str, top3_str, top5_str, conc_flag in results:
    print(f"SN{netuid:<4} {name:<22} {n_validators:<6} {best_uid:<10} {best_stake:<14,.0f} {best_take:<7.1%} {best_tv:<8.4f} {best_emission_apy:<14.4f} {price_apy:<+12.4f} {best_combined_apy:<14.4f} {top1_str:<8} {top3_str:<8} {top5_str:<8} {conc_flag:<16} {tao_per_block:.6f}")

print(f"\nTotal subnets scanned: {len(active_subnets)} | Subnets with active validators: {len(results)}")
if results:
    print(f"\nBest opportunity: SN{results[0][0]} ({results[0][1]}) — {results[0][10]:.4f}% combined APY via UID {results[0][3]}")
    print(f"  Emission: {results[0][8]:.4f}% | Price momentum: {results[0][9]:+.4f}% | Concentration: {results[0][17]}")
    print(f"\nNote: Price APY is estimated from current vs 30d EMA momentum. Past momentum does not guarantee future appreciation.")
