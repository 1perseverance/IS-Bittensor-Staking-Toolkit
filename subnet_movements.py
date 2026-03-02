import bittensor as bt
from datetime import datetime, timezone

sub = bt.Subtensor()
all_s = sub.all_subnets()
current_block = sub.get_current_block()
timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')

print(f'Bittensor Subnet Flow Cycle Snapshot')
print(f'Block: {current_block} | Timestamp: {timestamp}')
print(f'')
print(f"{'SN':<6} {'Name':<24} {'Price':<12} {'Moving Price':<14} {'Momentum':<12} {'TAO/block':<12} {'TAO Reserves':<18} {'Alpha Reserves':<18} {'Volume'}")
print('-' * 140)

for s in sorted(all_s, key=lambda x: float(x.tao_in_emission), reverse=True):
    if s.netuid == 0:
        continue
    if float(s.tao_in_emission) < 0.001:
        continue
    p = float(s.price)
    mp = float(s.moving_price)
    momentum = (p - mp) / mp if mp > 0 else 0
    tao_in = float(s.tao_in_emission)
    tao_reserves = float(s.tao_in)
    alpha_reserves = float(s.alpha_out)
    volume = float(s.subnet_volume)
    print(f"SN{s.netuid:<4} {s.subnet_name:<24} {p:<12.6f} {mp:<14.6f} {momentum:<+12.2%} {tao_in:<12.6f} {tao_reserves:<18,.2f} {alpha_reserves:<18,.2f} {volume:,.2f}")
