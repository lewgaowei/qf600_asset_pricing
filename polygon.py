import requests
import json
from datetime import datetime, timedelta
from pathlib import Path

class PolygonStockData:
    def __init__(self, api_key, data_folder=None):
        self.api_key = api_key
        self.base_url = "https://api.polygon.io"
        
        # Create json folder in the same directory as this script
        if data_folder is None:
            script_dir = Path(__file__).parent
            self.data_folder = script_dir / "json"
        else:
            self.data_folder = Path(data_folder)
        
        self.data_folder.mkdir(exist_ok=True)
    
    def get_stock_data(self, symbol, start_date, end_date=None, timeframe="daily"):
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        
        all_data = []
        current_start = start_date
        limit = 1000
        
        while True:
            url = f"{self.base_url}/v2/aggs/ticker/{symbol}/range/1/{timeframe}/{current_start}/{end_date}"
            params = {
                "apikey": self.api_key, 
                "limit": limit,
                "sort": "asc"
            }
            
            print(f"Making request to: {url}")
            response = requests.get(url, params=params)
            print(f"Response status: {response.status_code}")
            
            data = response.json()
            print(f"API Response status: {data.get('status')}")
            
            if data.get('status') != 'OK':
                raise Exception(f"API Error: {data.get('message', 'Unknown error')} - Full response: {data}")
            
            if not data.get('results') or len(data['results']) == 0:
                break
            print("A")
            
            all_data.extend(data['results'])
            
            first_date = datetime.fromtimestamp(data['results'][0]['t'] / 1000).strftime('%Y-%m-%d')
            last_date = datetime.fromtimestamp(data['results'][-1]['t'] / 1000).strftime('%Y-%m-%d')
            print(f"First day: {first_date}, Last day: {last_date}")
            
            print(f"Fetched {len(data['results'])} records, total: {len(all_data)}")
            
            if len(data['results']) < limit:
                print(len(data['results']))
                break
            
            last_timestamp = data['results'][-1]['t']
            last_date = datetime.fromtimestamp(last_timestamp / 1000).strftime('%Y-%m-%d')
            current_start = (datetime.strptime(last_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
            
            print(last_date, current_start)
            if current_start > end_date:
                break
        
        result = {
            'symbol': symbol,
            'timeframe': timeframe,
            'start_date': start_date,
            'end_date': end_date,
            'data': []
        }
        
        for item in all_data:
            stock_data = {
                'date': datetime.fromtimestamp(item['t'] / 1000).strftime('%Y-%m-%d'),
                'open': item['o'],
                'high': item['h'],
                'low': item['l'],
                'close': item['c'],
                'volume': item['v']
            }
            result['data'].append(stock_data)
        
        filename_start_date = start_date.replace('-', '')
        filename_end_date = end_date.replace('-', '')
        filename = f"{symbol}_{timeframe}_{filename_start_date}_to_{filename_end_date}.json"
        
        file_path = self.data_folder / filename
        
        with open(file_path, 'w') as f:
            json.dump(result, f, indent=2)
        
        print(f"Saved {len(result['data'])} total records to {filename}")
        return result
    
    def file_exists(self, symbol, start_date, end_date, timeframe="daily"):
        filename_start_date = start_date.replace('-', '')
        filename_end_date = end_date.replace('-', '')
        filename = f"{symbol}_{timeframe}_{filename_start_date}_to_{filename_end_date}.json"
        file_path = self.data_folder / filename
        return file_path.exists()
    
    def download_multiple_tickers(self, ticker_list, start_date, end_date, timeframe="day"):
        """Download data for multiple tickers using the existing get_stock_data method"""
        successful_downloads = []
        skipped_files = []
        failed_downloads = []
        
        print(f"Starting download for {len(ticker_list)} tickers...")
        print(f"Date range: {start_date} to {end_date}")
        print(f"Timeframe: {timeframe}")
        print("-" * 50)
        
        for i, ticker in enumerate(ticker_list, 1):
            print(f"\n[{i}/{len(ticker_list)}] Processing {ticker}...")
            
            try:
                # Check if file already exists
                if self.file_exists(ticker, start_date, end_date, timeframe):
                    print(f"✓ File already exists for {ticker}, skipping...")
                    skipped_files.append(ticker)
                    continue
                
                # Download data using the existing method
                data = self.get_stock_data(ticker, start_date, end_date, timeframe)
                print(f"✓ Successfully downloaded {len(data['data'])} records for {ticker}")
                successful_downloads.append(ticker)
                
            except Exception as e:
                print(f"✗ Failed to download {ticker}: {str(e)}")
                failed_downloads.append(ticker)
                continue
        
        print("\n" + "=" * 50)
        print("DOWNLOAD SUMMARY:")
        print(f"Total tickers: {len(ticker_list)}")
        print(f"Successful downloads: {len(successful_downloads)}")
        print(f"Skipped (already exist): {len(skipped_files)}")
        print(f"Failed downloads: {len(failed_downloads)}")
        print("=" * 50)
        
        return {
            'successful': successful_downloads,
            'skipped': skipped_files,
            'failed': failed_downloads
        }

if __name__ == "__main__":
    polygon = PolygonStockData("aeyRBRmWPfMN1sOOQQMOYuQNWdxu2VbX")
    sp500_tickers = ["MMM", "AOS", "ABT", "ABBV", "ACN", "ADBE", "AMD", "AES", "AFL", "A", "APD", "ABNB", "AKAM", "ALB", "ARE", "ALGN", "ALLE", "LNT", "ALL", "GOOGL", "GOOG", "MO", "AMZN", "AMCR", "AEE", "AEP", "AXP", "AIG", "AMT", "AWK", "AMP", "AME", "AMGN", "APH", "ADI", "AON", "APA", "APO", "AAPL", "AMAT", "APTV", "ACGL", "ADM", "ANET", "AJG", "AIZ", "T", "ATO", "ADSK", "ADP", "AZO", "AVB", "AVY", "AXON", "BKR", "BALL", "BAC", "BAX", "BDX", "BRK.B", "BBY", "TECH", "BIIB", "BLK", "BX", "XYZ", "BK", "BA", "BKNG", "BSX", "BMY", "AVGO", "BR", "BRO", "BF.B", "BLDR", "BG", "BXP", "CHRW", "CDNS", "CZR", "CPT", "CPB", "COF", "CAH", "KMX", "CCL", "CARR", "CAT", "CBOE", "CBRE", "CDW", "COR", "CNC", "CNP", "CF", "CRL", "SCHW", "CHTR", "CVX", "CMG", "CB", "CHD", "CI", "CINF", "CTAS", "CSCO", "C", "CFG", "CLX", "CME", "CMS", "KO", "CTSH", "COIN", "CL", "CMCSA", "CAG", "COP", "ED", "STZ", "CEG", "COO", "CPRT", "GLW", "CPAY", "CTVA", "CSGP", "COST", "CTRA", "CRWD", "CCI", "CSX", "CMI", "CVS", "DHR", "DRI", "DDOG", "DVA", "DAY", "DECK", "DE", "DELL", "DAL", "DVN", "DXCM", "FANG", "DLR", "DG", "DLTR", "D", "DPZ", "DASH", "DOV", "DOW", "DHI", "DTE", "DUK", "DD", "EMN", "ETN", "EBAY", "ECL", "EIX", "EW", "EA", "ELV", "EMR", "ENPH", "ETR", "EOG", "EPAM", "EQT", "EFX", "EQIX", "EQR", "ERIE", "ESS", "EL", "EG", "EVRG", "ES", "EXC", "EXE", "EXPE", "EXPD", "EXR", "XOM", "FFIV", "FDS", "FICO", "FAST", "FRT", "FDX", "FIS", "FITB", "FSLR", "FE", "FI", "F", "FTNT", "FTV", "FOXA", "FOX", "BEN", "FCX", "GRMN", "IT", "GE", "GEHC", "GEV", "GEN", "GNRC", "GD", "GIS", "GM", "GPC", "GILD", "GPN", "GL", "GDDY", "GS", "HAL", "HIG", "HAS", "HCA", "DOC", "HSIC", "HSY", "HPE", "HLT", "HOLX", "HD", "HON", "HRL", "HST", "HWM", "HPQ", "HUBB", "HUM", "HBAN", "HII", "IBM", "IEX", "IDXX", "ITW", "INCY", "IR", "PODD", "INTC", "IBKR", "ICE", "IFF", "IP", "IPG", "INTU", "ISRG", "IVZ", "INVH", "IQV", "IRM", "JBHT", "JBL", "JKHY", "J", "JNJ", "JCI", "JPM", "K", "KVUE", "KDP", "KEY", "KEYS", "KMB", "KIM", "KMI", "KKR", "KLAC", "KHC", "KR", "LHX", "LH", "LRCX", "LW", "LVS", "LDOS", "LEN", "LII", "LLY", "LIN", "LYV", "LKQ", "LMT", "L", "LOW", "LULU", "LYB", "MTB", "MPC", "MKTX", "MAR", "MMC", "MLM", "MAS", "MA", "MTCH", "MKC", "MCD", "MCK", "MDT", "MRK", "META", "MET", "MTD", "MGM", "MCHP", "MU", "MSFT", "MAA", "MRNA", "MHK", "MOH", "TAP", "MDLZ", "MPWR", "MNST", "MCO", "MS", "MOS", "MSI", "MSCI", "NDAQ", "NTAP", "NFLX", "NEM", "NWSA", "NWS", "NEE", "NKE", "NI", "NDSN", "NSC", "NTRS", "NOC", "NCLH", "NRG", "NUE", "NVDA", "NVR", "NXPI", "ORLY", "OXY", "ODFL", "OMC", "ON", "OKE", "ORCL", "OTIS", "PCAR", "PKG", "PLTR", "PANW", "PSKY", "PH", "PAYX", "PAYC", "PYPL", "PNR", "PEP", "PFE", "PCG", "PM", "PSX", "PNW", "PNC", "POOL", "PPG", "PPL", "PFG", "PG", "PGR", "PLD", "PRU", "PEG", "PTC", "PSA", "PHM", "PWR", "QCOM", "DGX", "RL", "RJF", "RTX", "O", "REG", "REGN", "RF", "RSG", "RMD", "RVTY", "ROK", "ROL", "ROP", "ROST", "RCL", "SPGI", "CRM", "SBAC", "SLB", "STX", "SRE", "NOW", "SHW", "SPG", "SWKS", "SJM", "SW", "SNA", "SOLV", "SO", "LUV", "SWK", "SBUX", "STT", "STLD", "STE", "SYK", "SMCI", "SYF", "SNPS", "SYY", "TMUS", "TROW", "TTWO", "TPR", "TRGP", "TGT", "TEL", "TDY", "TER", "TSLA", "TXN", "TPL", "TXT", "TMO", "TJX", "TKO", "TTD", "TSCO", "TT", "TDG", "TRV", "TRMB", "TFC", "TYL", "TSN", "USB", "UBER", "UDR", "ULTA", "UNP", "UAL", "UPS", "URI", "UNH", "UHS", "VLO", "VTR", "VLTO", "VRSN", "VRSK", "VZ", "VRTX", "VTRS", "VICI", "V", "VST", "VMC", "WRB", "GWW", "WAB", "WMT", "DIS", "WBD", "WM", "WAT", "WEC", "WFC", "WELL", "WST", "WDC", "WY", "WSM", "WMB", "WTW", "WDAY", "WYNN", "XEL", "XYL", "YUM", "ZBRA", "ZBH", "ZTS"]
    
    # Use the new method
    start_date = "2021-01-01"
    end_date = "2025-09-15"
    timeframe = "day"
    
    results = polygon.download_multiple_tickers(sp500_tickers, start_date, end_date, timeframe)
    
    # You can access the results
    print(f"\nSuccessful downloads: {results['successful'][:10]}...")  # First 10
    print(f"Failed downloads: {results['failed']}")
