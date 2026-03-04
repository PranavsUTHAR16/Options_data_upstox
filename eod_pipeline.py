#!/usr/bin/env python3
"""
EOD Options Data Pipeline
1. Fetch options data from Upstox API for a date range
2. Consolidate CSVs into parquet format
"""
import os
import re
import json
import time
import shutil
import logging
import argparse
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta, date as dt_date
from tqdm import tqdm

import requests
import upstox_client
from upstox_client.rest import ApiException
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ==================== CONFIGURATION ====================
SCRIPT_DIR = Path(__file__).parent
OUTPUT_DIR = SCRIPT_DIR / "eod_data"
RAW_DIR = OUTPUT_DIR / "raw"
PARQUET_DIR = OUTPUT_DIR / "parquet"
LOGS_DIR = OUTPUT_DIR / "logs"

# Index-specific configurations
INDEX_CONFIG = {
    "NIFTY": {
        "index_key": "NSE_INDEX|Nifty 50",
        "symbol": "NIFTY",
        "strike_step": 50
    },
    "BANKNIFTY": {
        "index_key": "NSE_INDEX|Nifty Bank",
        "symbol": "BANKNIFTY",
        "strike_step": 100
    },
    "FINNIFTY": {
        "index_key": "NSE_INDEX|Nifty Fin Service",
        "symbol": "FINNIFTY",
        "strike_step": 100
    },
    "SENSEX": {
        "index_key": "BSE_INDEX|SENSEX",
        "symbol": "SENSEX",
        "strike_step": 100
    }
}

RATE_LIMIT_DELAY = 0.025


def setup_logging(date_str):
    """Setup logging to file and console"""
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOGS_DIR / f"eod_pipeline_{date_str}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


def setup_directories():
    """Create output directories"""
    for directory in [OUTPUT_DIR, RAW_DIR, PARQUET_DIR, LOGS_DIR]:
        directory.mkdir(parents=True, exist_ok=True)


def load_access_token():
    """Load Upstox access token from environment or JSON file"""
    # Try environment variable first
    token = os.getenv('UPSTOX_ACCESS_TOKEN')
    if token:
        return token
        
    token_file = SCRIPT_DIR / "access_token.json"
    if not token_file.exists():
        raise FileNotFoundError("access_token.json not found and UPSTOX_ACCESS_TOKEN env var not set")
    
    with open(token_file, 'r') as f:
        token_data = json.load(f)
    return token_data.get('access_token')


def rate_limited_request(func, *args, **kwargs):
    """Rate-limited wrapper for API calls"""
    start_time = time.time()
    result = func(*args, **kwargs)
    elapsed = time.time() - start_time
    if elapsed < RATE_LIMIT_DELAY:
        time.sleep(RATE_LIMIT_DELAY - elapsed)
    return result


# ==================== STEP 1: FETCH DATA ====================
class DataFetcher:
    """Fetch options data from Upstox API"""
    
    def __init__(self, access_token, logger, index_name="NIFTY", strike_buffer=2000):
        self.access_token = access_token
        self.logger = logger
        self.index_name = index_name.upper()
        self.config = INDEX_CONFIG.get(self.index_name, INDEX_CONFIG["NIFTY"])
        self.strike_buffer = strike_buffer
        self.symbol_df = None
        self.api_instance = None
        self.api_client = None
        
    def cleanup(self):
        """Cleanup API client resources"""
        if self.api_client is not None:
            try:
                self.api_client.close()
            except Exception:
                pass  # Ignore cleanup errors
            self.api_client = None
            self.api_instance = None
    
    def __del__(self):
        """Destructor to cleanup resources"""
        self.cleanup()
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.cleanup()
        return False
        
    def setup(self):
        """Initialize API and load symbol master"""
        # Setup API
        config = upstox_client.Configuration()
        config.access_token = self.access_token
        self.api_client = upstox_client.ApiClient(config)
        self.api_instance = upstox_client.HistoryApi(self.api_client)
        
        # Load symbol master
        self.logger.info("Loading Upstox instrument master...")
        fileUrl = 'https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz'
        self.symbol_df = pd.read_csv(fileUrl)
        self.symbol_df['expiry'] = pd.to_datetime(self.symbol_df['expiry']).apply(
            lambda x: x.date() if pd.notna(x) else None
        )
        self.logger.info(f"Loaded {len(self.symbol_df)} instruments")
        
    def get_historical_candles(self, instrument_key, target_date, is_today=False):
        """Fetch historical candle data"""
        try:
            if is_today:
                response = rate_limited_request(
                    self.api_instance.get_intra_day_candle_data,
                    instrument_key, '1minute', '2.0'
                )
            else:
                # Try regular historical first
                response = rate_limited_request(
                    self.api_instance.get_historical_candle_data1,
                    instrument_key, '1minute', target_date, target_date, '2.0'
                )
            
            # Optimization: If expiry is more than 6 days old relative to TODAY, 
            # it is definitely expired in Upstox active pool.
            # We can skip the standard API and go straight to expired endpoint.
            # But since we don't have the expiry date here easily, we'll stick to the fallback.
            # However, we can use target_date to guess.
            
            if response.status == 'success' and response.data and response.data.candles:
                columns = ['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume', 'OI']
                df = pd.DataFrame(response.data.candles, columns=columns)
                df['Timestamp'] = pd.to_datetime(df['Timestamp']).dt.tz_localize(None)
                return df
            
            # If failed and not today, try expired API
            if not is_today:
                return self.get_expired_candles(instrument_key, target_date)
                
            return None
        except Exception as e:
            # Try expired fallback
            if not is_today:
                return self.get_expired_candles(instrument_key, target_date)
            return None

    def get_expired_candles(self, instrument_key, target_date):
        """Fallback for expired instruments using raw requests"""
        try:
            from urllib.parse import quote
            encoded_key = quote(instrument_key, safe='')
            url = f"https://api.upstox.com/v2/expired-instruments/historical-candle/{encoded_key}/1minute/{target_date}/{target_date}"
            
            headers = {
                'Accept': 'application/json',
                'Authorization': f'Bearer {self.access_token}'
            }
            
            response = requests.get(url, headers=headers, timeout=30)
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 'success' and 'data' in data and 'candles' in data['data']:
                    candles = data['data']['candles']
                    if candles:
                        columns = ['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume', 'OI']
                        df = pd.DataFrame(candles, columns=columns)
                        df['Timestamp'] = pd.to_datetime(df['Timestamp']).dt.tz_localize(None)
                        return df
            return None
        except Exception as e:
            self.logger.error(f"Expired candle fetch failed for {instrument_key}: {e}")
            return None

    def get_expired_expiries(self, instrument_key=None):
        """Get expired expiry dates"""
        if instrument_key is None:
            instrument_key = self.config["index_key"]
        try:
            from urllib.parse import quote
            encoded_key = quote(instrument_key, safe='')
            url = f"https://api.upstox.com/v2/expired-instruments/expiries?instrument_key={encoded_key}"
            
            headers = {
                'Accept': 'application/json',
                'Authorization': f'Bearer {self.access_token}'
            }
            
            response = requests.get(url, headers=headers, timeout=30)
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 'success':
                    return data.get('data', [])
            return []
        except Exception as e:
            self.logger.error(f"Failed to fetch expired expiries: {e}")
            return []

    def get_expired_contracts(self, instrument_key, expiry_date):
        """Get expired option contracts for an expiry"""
        try:
            from urllib.parse import quote
            encoded_key = quote(instrument_key, safe='')
            url = f"https://api.upstox.com/v2/expired-instruments/option/contract?instrument_key={encoded_key}&expiry_date={expiry_date}"
            
            headers = {
                'Accept': 'application/json',
                'Authorization': f'Bearer {self.access_token}'
            }
            
            response = requests.get(url, headers=headers, timeout=30)
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 'success':
                    return data.get('data', [])
            return []
        except Exception as e:
            self.logger.error(f"Failed to fetch expired contracts for {expiry_date}: {e}")
            return []
    
    def get_strike_range(self, index_df):
        """Calculate strike range from Index high/low"""
        high = index_df['High'].max()
        low = index_df['Low'].min()
        
        step = self.config["strike_step"]
        high_strike = int(round(high / step) * step)
        low_strike = int(round(low / step) * step)
        
        upper = high_strike + self.strike_buffer
        lower = low_strike - self.strike_buffer
        
        return list(range(lower, upper + step, step))
    
    def get_option_key(self, expiry_date, strike, option_type):
        """Get instrument key for an option"""
        options = self.symbol_df[
            (self.symbol_df['name'] == self.config["symbol"]) &
            (self.symbol_df['instrument_type'] == 'OPTIDX') &
            (self.symbol_df['exchange'] == 'NSE_FO') &
            (self.symbol_df['option_type'] == option_type) &
            (self.symbol_df['expiry'] == expiry_date) &
            (self.symbol_df['strike'] == float(strike))
        ]
        
        if not options.empty:
            return options.iloc[0]['instrument_key']
        return None
    
    def find_closest_expiries(self, reference_date, num=5):
        """Find closest N expiries relative to reference_date"""
        index_options = self.symbol_df[
            (self.symbol_df['name'] == self.config["symbol"]) &
            (self.symbol_df['instrument_type'] == 'OPTIDX') &
            (self.symbol_df['exchange'] == 'NSE_FO')
        ]
        
        future = index_options['expiry'].dropna()
        future = future[future >= reference_date]
        return sorted(future.unique())[:num]
    
    def fetch_data(self, target_date):
        """Main data fetching function with expired data support"""
        self.logger.info(f"Fetching data for {target_date}")
        
        today_str = datetime.now().strftime("%Y-%m-%d")
        is_today = (target_date == today_str)
        target_dt = datetime.strptime(target_date, "%Y-%m-%d").date()
        date_folder_name = datetime.strptime(target_date, "%Y-%m-%d").strftime("%d-%m-%Y")
        
        # Fetch Index data
        index_df = self.get_historical_candles(self.config["index_key"], target_date, is_today)
        if index_df is None or index_df.empty:
            self.logger.error(f"Failed to fetch {self.index_name} data")
            return None
        
        self.logger.info(f"Fetched {len(index_df)} {self.index_name} candles")
        
        # Determine expiries
        # First try active instruments
        strikes = self.get_strike_range(index_df)
        active_expiries = [e for e in self.find_closest_expiries(target_dt, 10)]
        
        # Also check expired API for expiries relevant to target_date
        self.logger.info("Checking expired expiries API...")
        expired_expiries_raw = self.get_expired_expiries(self.config["index_key"])
        expired_expiries = []
        for e_str in expired_expiries_raw:
            try:
                e_dt = datetime.strptime(e_str, "%Y-%m-%d").date()
                # Expiry must be on or after target_date
                if e_dt >= target_dt:
                    expired_expiries.append(e_dt)
            except Exception as e:
                continue
        
        # Combine and take closest 5 unique expiries
        all_expiries = sorted(list(set(active_expiries + expired_expiries)))[:5]
        
        if not all_expiries:
            self.logger.error(f"No expiries found for {target_date}")
            return None
        
        self.logger.info(f"Using expiries: {[e.strftime('%Y-%m-%d') for e in all_expiries]}")
        
        # Create output folder
        date_folder = RAW_DIR / date_folder_name
        date_folder.mkdir(parents=True, exist_ok=True)
        
        total_saved = 0
        
        for expiry in all_expiries:
            expiry_str = expiry.strftime("%d%b%Y").upper()
            expiry_date_str = expiry.strftime("%Y-%m-%d")
            self.logger.info(f"Processing expiry: {expiry_str}")
            
            expiry_folder = date_folder / expiry_str
            expiry_folder.mkdir(parents=True, exist_ok=True)
            
            # Save Index data
            index_df.to_csv(expiry_folder / f"{self.index_name}.csv", index=False)
            
            # Get contract map for this expiry (especially if expired)
            # Try to build a local map of strike -> type -> key
            contract_map = {}
            
            # Add from active symbols
            active_contracts = self.symbol_df[
                (self.symbol_df['name'] == self.config["symbol"]) &
                (self.symbol_df['expiry'] == expiry)
            ]
            for _, row in active_contracts.iterrows():
                s = int(row['strike'])
                t = row['option_type']
                contract_map[(s, t)] = row['instrument_key']
            
            # Add from expired symbols if target_date is in past
            if not is_today:
                expired_contracts = self.get_expired_contracts(self.config["index_key"], expiry_date_str)
                for c in expired_contracts:
                    s = int(c.get('strike_price', 0))
                    t = c.get('instrument_type')
                    if s and t:
                        contract_map[(s, t)] = c.get('instrument_key')

            # Fetch options
            with tqdm(total=len(strikes) * 2, desc=expiry_str, leave=False) as pbar:
                for strike in strikes:
                    for opt_type in ['CE', 'PE']:
                        key = contract_map.get((strike, opt_type))
                        if key:
                            opt_df = self.get_historical_candles(key, target_date, is_today)
                            if opt_df is not None and not opt_df.empty:
                                opt_df.to_csv(expiry_folder / f"{strike}_{opt_type}.csv", index=False)
                                total_saved += 1
                        pbar.update(1)
        
        self.logger.info(f"Saved {total_saved} option files")
        return date_folder_name


# ==================== STEP 2: CONVERT TO PARQUET ====================
def consolidate_to_parquet(date_str, logger, index_name="NIFTY"):
    """Consolidate CSVs to parquet format"""
    logger.info(f"Consolidating {date_str} to parquet for {index_name}...")
    
    date_folder = RAW_DIR / date_str
    if not date_folder.exists():
        logger.error(f"Folder not found: {date_folder}")
        return None
    
    all_data = []
    index_name = index_name.upper()
    
    for expiry_folder in [d for d in date_folder.iterdir() if d.is_dir()]:
        expiry_date = expiry_folder.name
        
        index_file = expiry_folder / f"{index_name}.csv"
        if not index_file.exists():
            continue
        
        index_df = pd.read_csv(index_file)
        index_df['Timestamp'] = pd.to_datetime(index_df['Timestamp'])
        index_df.rename(columns={
            'Open': 'S_Open', 'High': 'S_High', 'Low': 'S_Low',
            'Close': 'S_Close', 'Volume': 'S_Volume'
        }, inplace=True)
        
        option_files = list(expiry_folder.glob("*_CE.csv")) + list(expiry_folder.glob("*_PE.csv"))
        
        for opt_file in option_files:
            match = re.search(r'(\d+)_(CE|PE)\.csv', opt_file.name)
            if not match:
                continue
            
            strike = float(match.group(1))
            opt_type = match.group(2)
            
            opt_df = pd.read_csv(opt_file)
            if opt_df.empty:
                continue
            
            opt_df['Timestamp'] = pd.to_datetime(opt_df['Timestamp'])
            opt_df['Strike'] = strike
            opt_df['OptionType'] = opt_type
            opt_df['ExpiryDate'] = expiry_date
            opt_df['Date'] = date_str
            opt_df['Index'] = index_name
            
            opt_df.rename(columns={
                'Open': 'O_Open', 'High': 'O_High', 'Low': 'O_Low',
                'Close': 'O_Close', 'Volume': 'O_Volume', 'OI': 'O_OI'
            }, inplace=True)
            
            merged = pd.merge(opt_df, index_df, on='Timestamp', how='inner')
            all_data.append(merged)
    
    if not all_data:
        logger.error("No data to consolidate")
        return None
    
    combined = pd.concat(all_data, ignore_index=True)
    
    # Save parquet
    output_path = PARQUET_DIR / f"{date_str}_consolidated.parquet"
    combined.to_parquet(output_path, index=False, engine='pyarrow')
    
    logger.info(f"Created {output_path.name} ({len(combined)} rows)")
    return output_path


# ==================== STEP 3: CALCULATE GREEKS ====================
# ==================== MAIN PIPELINE ====================
def run_pipeline(start_date=None, end_date=None, dry_run=False, index_name="NIFTY", strike_buffer=2000):
    """Run the complete EOD pipeline for a date range"""
    
    # Setup
    setup_directories()
    
    index_name = index_name.upper()
    if index_name not in INDEX_CONFIG:
        print(f"Index {index_name} not supported. Using NIFTY.")
        index_name = "NIFTY"
        start_date = datetime.now().strftime("%Y-%m-%d")
    if end_date is None:
        end_date = start_date
        
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    
    # Generate list of dates
    dates = []
    current = start_dt
    while current <= end_dt:
        # Skip weekends (Saturday=5, Sunday=6)
        if current.weekday() < 5:
            dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    
    if not dates:
        print("No valid trading days in range.")
        return

    logger = setup_logging(datetime.now().strftime("%d-%m-%Y"))
    
    logger.info("=" * 60)
    logger.info("🚀 EOD OPTIONS DATA PIPELINE")
    logger.info("=" * 60)
    logger.info(f"Index: {index_name}")
    logger.info(f"Date range: {start_date} to {end_date}")
    logger.info(f"Strike buffer: {strike_buffer}")
    logger.info(f"Total days to process: {len(dates)}")
    
    access_token = load_access_token()
    logger.info("✅ Access token loaded")
    
    for target_date in dates:
        logger.info(f"\nProcessing date: {target_date}")
        start_time = time.time()
        
        try:
            # Step 1: Fetch data
            logger.info(f"📥 STEP 1: Fetching options data for {index_name}...")
            with DataFetcher(access_token, logger, index_name=index_name, strike_buffer=strike_buffer) as fetcher:
                fetcher.setup()
                fetched_date = fetcher.fetch_data(target_date)
            
            if not fetched_date:
                logger.error(f"Data fetch failed for {target_date}")
                continue
            
            # Step 2: Consolidate to parquet
            logger.info(f"📦 STEP 2: Consolidating to parquet for {index_name}...")
            parquet_path = consolidate_to_parquet(fetched_date, logger, index_name=index_name)
            
            if not parquet_path:
                logger.error(f"Parquet consolidation failed for {target_date}")
                continue
            
            # Step 3: Cleanup raw CSVs
            logger.info("🧹 STEP 3: Cleaning up raw CSVs...")
            raw_folder = RAW_DIR / fetched_date
            if raw_folder.exists():
                shutil.rmtree(raw_folder)
                logger.info(f"Deleted {raw_folder}")
            
            elapsed = time.time() - start_time
            logger.info(f"✅ Finished {target_date} in {elapsed:.1f}s")
            
        except Exception as e:
            logger.error(f"Failed for {target_date}: {e}")
            continue

    logger.info("\n" + "=" * 60)
    logger.info("✅ PIPELINE COMPLETED")
    logger.info("=" * 60)


def main():
    """CLI entry point"""
    parser = argparse.ArgumentParser(description="EOD Options Data Pipeline")
    parser.add_argument('--date', help='Target date (YYYY-MM-DD), default: today')
    parser.add_argument('--from', dest='from_date', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--till', dest='till_date', help='End date (YYYY-MM-DD)')
    parser.add_argument('--index', default='NIFTY', choices=['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'SENSEX'], help='Index to fetch data for')
    parser.add_argument('--buffer', type=int, default=2000, help='Strike buffer (default: 2000)')
    parser.add_argument('--dry-run', action='store_true', help='Test mode')
    
    args = parser.parse_args()
    
    start_date = args.from_date if args.from_date else args.date
    end_date = args.till_date if args.till_date else start_date
    
    run_pipeline(
        start_date=start_date,
        end_date=end_date,
        dry_run=args.dry_run,
        index_name=args.index,
        strike_buffer=args.buffer
    )


if __name__ == "__main__":
    main()
