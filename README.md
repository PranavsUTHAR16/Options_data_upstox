# Options Data Upstox

A simplified EOD (End of Day) options data downloader for NIFTY, fetching data via the Upstox API. This tool allows you to fetch historical options data, consolidate it into parquet format, and handles batch downloads for multiple days.

## Credits & Attribution

The authentication logic in this project is based on the work of **Data For Traders**.
- **Source Video:** [Upstox API V2 - Automated Login using Python](https://www.youtube.com/watch?v=0XQCb4aSXDQ&list=PLtq1ftYrDPQbPcHdYLsbspmmD_uZox-OR&index=5)
- **YouTube Channel:** [Data For Traders](https://www.youtube.com/@DataForTraders)

## Features

- **Automated Authentication**: Hands-free login and token generation using Playwright.
- **Multi-Index Support**: Fetch data for NIFTY, BANKNIFTY, FINNIFTY, and SENSEX.
- **Automated Strike Steps**: Automatically adjusts strike intervals based on the selected index.
- **Batch Processing**: Fetch data for a single day or a range of dates.
- **CSV Consolidation**: Automatically merges index spot and options data into efficient Parquet files.
- **Weekend Awareness**: Automatically skips non-trading days in date ranges.
- **Environment Driven**: No hardcoded credentials; uses `.env` for security.

## Setup

1. **Clone the repository**:
   ```bash
   git clone https://github.com/PranavsUTHAR16/Options_data_upstox
   cd Options_data_upstox
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   playwright install chromium
   ```

3. **Configure Environment**:
   Copy `.env.example` to `.env` and fill in your Upstox credentials:
   ```bash
   cp .env.example .env
   ```
   Required variables:
   - `UPSTOX_API_KEY`
   - `UPSTOX_SECRET_KEY`
   - `UPSTOX_REDIRECT_URI`
   - `UPSTOX_TOTP_KEY`
   - `UPSTOX_MOBILE_NO`
   - `UPSTOX_PIN`

## Usage

### 1. Authenticate
Run this once to generate an `access_token.json` or set `UPSTOX_ACCESS_TOKEN` in your environment.
```bash
python auth.py
```

### 2. Run Pipeline
Download and consolidate data for a specific date and index:
```bash
python eod_pipeline.py --date 2026-03-04 --index BANKNIFTY
```

Batch download a range of dates with a custom strike buffer:
```bash
python eod_pipeline.py --from 2026-03-01 --till 2026-03-04 --index NIFTY --buffer 1000
```

### Supported Indices
- `NIFTY` (Step: 50)
- `BANKNIFTY` (Step: 100)
- `FINNIFTY` (Step: 100)
- `SENSEX` (Step: 100)

## Data Output
- `eod_data/raw/`: Temporary CSV files (automatically cleaned up).
- `eod_data/parquet/`: Consolidated Parquet files containing both spot and options data.
- `eod_data/logs/`: Detailed execution logs.

## License
MIT
