# Intelligence Sovereignty — Bittensor Staking Toolkit

Reference code for on-chain Bittensor yield analysis. Published as a static reference library for anyone who wants to verify the numbers behind subnet staking decisions.

This is not a maintained project. No updates are guaranteed. Use it as a starting point, not a dependency.

---

## Tools — Three-Layer Framework

### `IS_subnet_analysis.py`

Network wide snapshot of all active subnets. Ranks by emission APY minus positive dilution (conservative real yield proxy).
Use this first to get a read on which subnets to stake in.

Shows:
- Emission APY (raw yield)
- Gross inflation (dilution pressure)
- Net supply delta (actual dilution — requires 2+ runs)
- Nominal APY (emission + price momentum — labeled as incomplete, still need to account for actual dilution)
- Liquidation haircut (deregistration risk)
- EMA band / lag trap


### `IS_validator_analysis.py`

Single subnet validator breakdown. Takes a subnet ID and your stake size, ranks validators by emission APY (post-entry). 
Use this to make the final validator section within a subnet. It is recommended to track the ranks regularly for any deviations.

Shows:
- E Share (routing weight)
- Efficiency (E Share vs stake weight — >1x = overperformer)
- Stake distribution (concentration + nominator count)
- D/I ratio (pure validator vs validator-miner)
- Take rate, TV, self-stake


### `IS_root_analysis.py`

Root network baseline. No AMM layer, no Alpha exposure.
Use this to decide and monitor with which validator to stake with.

Shows:
- Pool stakes, dividends, take rates
- Your yield estimate
- Ghost validators (stake > 0, dividend = 0)
- Stake concentration + subnet coverage

Root is not simply lower yield, it is structural capital. 
TAO functions as a layered portfolio: Root is the base allocation, subnet Alpha positions are higher-volatility overlays on top. Root stake stays denominated in TAO, compounds steadily, and can be redeployed without AMM slippage when opportunity emerges. Root is the foundation that makes subnets' structure possible.

---

## Suggested Workflow
```
IS_subnet_analysis.py     →    IS_validator_analysis.py
(cross-subnet ranking)          (validator selection)

                          |
                  IS_root_analysis.py
                    (root baseline)
```

---

## Falsifiability Work

### `IS_chutes_sn64_analysis.py`

SN64 Chutes intelligence market hypothesis test. Fetches 7-day invocation exports from api.chutes.ai in a single-pass sequential stream, merges with on-chain metagraph data, and tests whether validator weight allocations reflect real demand. Outputs Spearman and Pearson correlation analysis, divergence tables, demand-side user concentration, Root × SN64 validator overlap, and miner incentive concentration metrics. Includes an eight-signal E2EE perimeter forensics layer that evaluates demand authenticity at the metadata boundary without accessing encrypted content. Signal weights and longitudinal tracking are not included in this reference implementation.

---

## Installation
```
pip install bittensor requests
```

Run any script directly. Outputs CSV snapshots to respective directories:
```
- `subnet_analysis/subnet_analysis_snapshot_YYYY-MM-DD.csv`
- `validator_analysis/SN{netuid}/snapshot_YYYY-MM-DD.csv`
- `root_analysis/snapshot_YYYY-MM-DD.csv`
- `chutes_analysis/chutes_sn64_analysis_YYYY-MM-DD.csv`
```

> **Note:** `chutes_sn64_analysis.py` fetches 168 hourly CSV archives sequentially — expect runtime of 20-50 minutes depending on connection speed.

---

## Disclaimer

This code is provided as-is, for reference and educational purposes only.

Forks of this repository are not affiliated with or endorsed by the author. The author takes no responsibility for any modifications made in forks or downstream uses of this code.

This is not financial advice. All yield estimates are point-in-time calculations based on on-chain data at the moment of execution. Past momentum does not imply future returns.

---

## Articles

These tools were built alongside the following research:

* [Bittensor Emissions: What the Table Doesn't Tell You](https://x.com/im_perseverance/status/2022673949277016244)
* [Root Staking on Bittensor: The Structural Yield Analysis](https://x.com/im_perseverance/status/2025974805132882194)
* [Subnet Staking on Bittensor: The Structural Yield Analysis](https://x.com/im_perseverance/status/2028546102119780484)
* [Chutes SN64: Empirical Case Study](https://x.com/im_perseverance/status/2033214446810669137)

---

## License

Apache 2.0. See `LICENSE` for full terms.
