# DeepValue-Systematic-Investing-IBKR
Python script that filters equities for deep value / GARP characteristics and routes orders via the Interactive Brokers API. Also generates xlxs files for automated reporting.

## Core Architecture
1. **Signal Generation:** Filters universe based on forward P/E, PEG ratios, and earnings credibility scores, isolating organic growth.
2. **Execution Gateway:** Connects to IBKR TWS/Gateway using ib_async. Handles dynamic order routing (e.g., MOO orders with TIF='OPG' during closed sessions) and tracks precise commission fees.
3. **Immutable Ledger:** Generates persistent snapshots logging entry/exit regimes, capital deployed, and metric states at execution.

## Performance Metrics (Benchmark-Relative)
*Note: The following tables reflect the output of the statistical audit engine.*

| Metric | Forward P/E Strategy | S&P 500 (Cap-Weighted) | S&P 500 (Equal-Weighted) |
| :--- | :--- | :--- | :--- |
| CAGR | [0.00]% | [0.00]% | [0.00]% |
| Maximum Drawdown | -[0.00]% | -[0.00]% | -[0.00]% |
| Annual Volatility | [0.00]% | [0.00]% | [0.00]% |
| Sharpe Ratio | [0.00] | [0.00] | [0.00] |
| Sortino Ratio | [0.00] | [0.00] | [0.00] |
| Calmar Ratio | [0.00] | [0.00] | [0.00] |
| Final Equity | $[0.00] | $[0.00] | $[0.00] |

## Year-by-Year Performance Breakdown
| Year | Strategy Return | Benchmark Return | Performance Difference |
| :--- | :--- | :--- | :--- |
| 2026 | [0.00]% | [0.00]% | [0.00]% |
| 2025 | [0.00]% | [0.00]% | [0.00]% |

## Trade Log (State Transitions Only)
| Date | Action | Ticker | Details |
| :--- | :--- | :--- | :--- |
| YYYY-MM-DD | REGIME CHANGE | N/A | Initiated Rebalance #X |
| YYYY-MM-DD | POSITION CLOSED | [TICKER] | Exit Price: $[0.00] |
| YYYY-MM-DD | NEW POSITION | [TICKER] | Entry Price: $[0.00] |

## Usage
Requires Python 3.8+ and an active IBKR TWS or IB Gateway instance. 
Execute `python v21.py` and follow terminal prompts for universe selection and equity allocation.
