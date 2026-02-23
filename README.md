# DeepValue-Systematic-Investing-IBKR
Python script that filters equities for deep value / GARP characteristics and routes orders via the Interactive Brokers API. Also generates xlxs files for automated reporting.

# Automated Equity Valuation Model

## Overview
This repository contains a Python-based valuation engine designed to automate fundamental equity analysis. The script ingests financial data via external APIs, processes historical and forward-looking earnings metrics, and programmatically generates formatted, institutional-grade Excel dashboards for scenario analysis.

## Methodology
The model evaluates equities based on earnings credibility, historical multiple compression, and forward growth projections. 

1. Data Ingestion & Normalization
The engine queries Yahoo Finance and Financial Modeling Prep (FMP) APIs to extract both GAAP (Basic) and Adjusted (Street) Earnings Per Share (EPS). It reconstructs trailing twelve-month (TTM) earnings and normalizes cross-border currencies to ensure standardized comparative analysis.

2. Multiple Expansion & Compression
Historical P/E ratios are calculated using a 2-year lookback period to establish baseline valuation floors and ceilings. The system evaluates current market pricing against these historical ranges to quantify downside risk and required safety cushions.

3. The Holden Score
The model calculates a proprietary upside efficiency metric. It defines the relationship between realizable upside and the forward Price/Earnings-to-Growth (PEG) ratio, establishing a strict statistical threshold for capital deployment.

## System Architecture
The script is structured to separate data retrieval from output generation:

* Data Pipeline: Utilizes `yfinance` and `requests` for data extraction, handling rate limits and API fallbacks automatically.
* Diagnostic Logic: Categorizes earnings growth as either "Organic" or a "Cyclical Rebound" based on prior-year performance metrics and analyst estimate dispersion.
* Output Generation: Employs `xlsxwriter` to build dynamic Excel workbooks. The output includes conditional formatting, interactive dropdowns for scenario testing (e.g., toggling between GAAP and Street EPS), and a centralized valuation comparison table.

## Execution
Ensure all dependencies are installed before execution. An active API key for Financial Modeling Prep is required for historical multiple retrieval.

Requirements:
pandas
numpy
xlsxwriter
yfinance
requests

Run the script from the terminal to generate the valuation workbook:
python v9_PE_low_high.py
