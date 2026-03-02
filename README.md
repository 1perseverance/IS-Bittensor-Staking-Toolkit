# Intelligence Sovereignty — Bittensor Yield Tools

Reference code for on-chain Bittensor yield analysis. Published as a static reference library for anyone who wants to verify the numbers behind subnet staking decisions.

This is not a maintained project. No updates are guaranteed. Use it as a starting point, not a dependency.

---

## Tools

### `subnet_movements.py`
Network-wide snapshot of all active subnets. Shows current Alpha price vs 30-day EMA, price momentum, TAO/block emission, TAO and Alpha reserves, and volume. Use this first to get a read on where each subnet sits in its flow cycle before going deeper.

### `subnet_deep_overview.py`
Cross-subnet yield scanner. Scans all active subnets (or a custom list) and ranks them by best available combined APY — emission + price — surfacing the top validator per subnet alongside concentration flags. Use this to narrow down which subnets are worth a closer look.

### `subnet_validator_overview.py`
Single-subnet validator breakdown. Takes a subnet ID and your stake size, then outputs every active validator's emission APY, price APY, combined APY, take rate, Tv score, and pool concentration. Use this to make the final validator selection within a subnet.

### `root_validator_overview.py`
Root network equivalent. No AMM layer, no Alpha exposure. Shows validator pool stakes, dividends, take rates, your personal yield estimate, and last activity timestamp.
Root is not simply lower yield — it is structural capital. TAO functions as a layered portfolio: Root is the base allocation, subnet Alpha positions are higher-volatility overlays on top. Root stake stays denominated in TAO, compounds steadily, and can be redeployed without AMM slippage when opportunity emerges. Use this as a foundation to make subnets' structure possible.

---

## Suggested Workflow
```
subnet_movements.py      →    subnet_deep_overview.py    →    subnet_validator_overview.py
(network overview)             (cross-subnet ranking)          (validator selection)

                                          |
                              root_validator_overview.py
                                    (root baseline)
```

---

## Installation
```bash
pip install bittensor
```

Run any script directly:
```bash
python subnet_movements.py
python subnet_deep_overview.py
python subnet_validator_overview.py
python root_validator_overview.py
```

> **Note:** `subnet_deep_overview.py` and `subnet_validator_overview.py` make repeated on-chain calls per validator. On subnets with many active validators, expect runtime of several minutes.

---

## Disclaimer

This code is provided as-is, for reference and educational purposes only.

Forks of this repository are not affiliated with or endorsed by the author. The author takes no responsibility for any modifications made in forks or downstream uses of this code.

This is not financial advice. All yield estimates are point-in-time calculations based on on-chain data at the moment of execution. Past momentum does not imply future returns.

---

## License

Apache 2.0. See `LICENSE` for full terms.

---
