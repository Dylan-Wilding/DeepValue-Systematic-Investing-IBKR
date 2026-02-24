# DeepValue-Systematic-Investing-IBKR
Python script that filters equities for deep value / GARP characteristics and routes orders via the Interactive Brokers API. Also generates xlxs files for automated reporting.

## Core Architecture
1. **Signal Generation:** Filters universe based on forward P/E, forward PEG ratios, and past earnings surprise% scores, determining how predictable the company's earnings are. 
2. **Execution Gateway:** Connects to IBKR Gateway using ib_async. Handles dynamic order routing and tracks precise commission fees, generating reconciliation and fee reports.
3. **Immutable Ledger:** Generates persistent snapshots logging entry/exit regimes, capital deployed, and metric states at execution in tabular xlxs formats. Reporting is fully automated, and allows for ad-hoc analysis. 

## Usage
Requires Python 3.8+ and an active IBKR TWS or IB Gateway instance. 
Execute `systematic-invest-v21.py` and follow terminal prompts for universe selection and equity allocation.
