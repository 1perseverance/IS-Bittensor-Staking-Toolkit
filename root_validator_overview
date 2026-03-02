import bittensor as bt

POOL_CUTOFF = 1000

my_stake = float(input("Enter your stake amount in TAO: "))

sub = bt.Subtensor()
mg = sub.metagraph(netuid=0)
current_block = sub.get_current_block()

takes = {
    uid: (lambda d: d.take if d else 0)(sub.get_delegate_by_hotkey(mg.hotkeys[uid]))
    for uid in range(len(mg.hotkeys))
}

last_updates = sub.query_map_subtensor("LastUpdate")
last_update_map = {int(uid): block.value[0] for uid, block in last_updates}

print(f"\nPersonal yield estimate for {my_stake} TAO\n")
print(f"{'UID':<5} {'Pool Stake':<12} {'Dividend':<10} {'Take':<6} {'Your Yield':<12} {'Days Ago':<10} {'Activity':<18} {'Hotkey'}")
print("-" * 155)

results = [
    (uid, float(mg.S[uid]), float(mg.D[uid]), takes.get(uid, 0), mg.hotkeys[uid])
    for uid in range(len(mg.hotkeys))
    if float(mg.S[uid]) >= POOL_CUTOFF
]

results = sorted(
    [
        (
            uid, s, d, t,
            (my_stake / (s + my_stake)) * d * (1 - t),
            (current_block - last_update_map.get(uid, 0)) / 7200,
            "Active (Yuma)" if d > 0 else "Swap only",
            hk,
        )
        for uid, s, d, t, hk in results
    ],
    key=lambda x: x[4],
    reverse=True,
)

for uid, s, d, t, yld, days_ago, status, hk in results:
    if days_ago < 1:
        activity = "< 1 day ago"
    elif days_ago < 7:
        activity = f"{days_ago:.1f} days ago"
    elif days_ago < 30:
        activity = f"{days_ago:.0f} days ago"
    else:
        activity = f"STALE {days_ago:.0f}d"
    print(f"{uid:<5} {s:<12,.0f} {d:<10.6f} {t:<6.1%} {yld:<12.6f} {days_ago:<10.1f} {activity:<18} {hk}")
