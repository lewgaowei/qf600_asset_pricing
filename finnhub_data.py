import requests
import json
from datetime import datetime, timedelta
from pathlib import Path
import finnhub
from config import FINNHUB_API_KEY
from time import sleep

class FinnhubData:
    
    def __init__(self, api_key=None, data_folder=None):
        # Use API key from config if not provided
        self.api_key = api_key or FINNHUB_API_KEY
        self.base_url = "https://finnhub.io/api/v1"
        
        # Connect to Finnhub:
        self.finnhub_client = finnhub.Client(api_key=self.api_key)
        
        # Create data folder
        if data_folder is None:
            script_dir = Path(__file__).parent
            self.download_folder = script_dir / "finnhub_data"
        else:
            self.download_folder = Path(data_folder)
        
        self.download_folder.mkdir(exist_ok=True)
        print(f"Data will be saved to: {self.download_folder}")

    def get_financials(self, statement, symbol, frequency):
        
        subfolder = self.download_folder / "financials"
        subfolder.mkdir(exist_ok=True)
        
        # statement: "bs" (Balance Sheet), "ic" (Income Statement), "cf" (Cash Flow)
        # frequency: "annual" or "quarterly"
        print(f"Fetching {statement} financials for {symbol} ({frequency})...")
        
        try:
            # Get financial data from Finnhub
            data = self.finnhub_client.financials(symbol, statement, frequency)
            
            # Create result structure
            result = {
                'symbol': symbol,
                'statement': statement,
                'frequency': frequency,
                'timestamp': datetime.now().isoformat(),
                'data': data
            }
            
            # Rename the filename statement accordingly
            statement_map = {
                "ic": "income",
                "bs": "balance",
                "cf": "cashflow"
            }

            filename_statement = statement_map.get(statement)
            filename = f"financials_{filename_statement}_{symbol}_{frequency}.json"
            file_path = subfolder / filename
            
            # Save to file
            with open(file_path, 'w') as f:
                json.dump(result, f, indent=2)
            
            print(f"[SUCCESS] Saved financials data to {filename}")
            print(f"[SUCCESS] File location: {file_path}")
            return result
            
        except Exception as e:
            print(f"[ERROR] Error fetching financials for {symbol}: {e}")
            return None

    def download_multiple_companies_financials(self, ticker_list, statement_types=None, frequencies=None):

        if statement_types is None:
            statement_types = ['ic', 'bs', 'cf']  # income, balance, cashflow
        
        if frequencies is None:
            frequencies = ['annual', 'quarterly']
        
        print(f"\n=== Starting batch download ===")
        print(f"Companies: {ticker_list}")
        print(f"Statement types: {statement_types}")
        print(f"Frequencies: {frequencies}")
        print("-" * 50)
        
        successful_downloads = []
        failed_downloads = []
        
        total_requests = len(ticker_list) * len(statement_types) * len(frequencies)
        current_request = 0
        
        for symbol in ticker_list:
            print(f"\n--- Processing {symbol} ---")
            
            for statement in statement_types:
                for frequency in frequencies:
                    current_request += 1
                    print(f"\n[{current_request}/{total_requests}] {symbol} - {statement} - {frequency}")
                    
                    try:
                        result = self.get_financials(statement, symbol, frequency)
                        if result:
                            successful_downloads.append(f"{symbol}_{statement}_{frequency}")
                        else:
                            failed_downloads.append(f"{symbol}_{statement}_{frequency}")
                    except Exception as e:
                        print(f"[ERROR] Failed to process {symbol}_{statement}_{frequency}: {e}")
                        failed_downloads.append(f"{symbol}_{statement}_{frequency}")
        
        # Print summary
        print("\n" + "=" * 50)
        print("DOWNLOAD SUMMARY:")
        print(f"Total requests: {total_requests}")
        print(f"Successful downloads: {len(successful_downloads)}")
        print(f"Failed downloads: {len(failed_downloads)}")
        print("=" * 50)
        
        if successful_downloads:
            print(f"Successful: {successful_downloads[:10]}{'...' if len(successful_downloads) > 10 else ''}")
        if failed_downloads:
            print(f"Failed: {failed_downloads}")
        
        return {
            'successful': successful_downloads,
            'failed': failed_downloads
        }
    
    
    def get_financials_as_reported(self, symbol, frequency):
        """
        Download financials as reported for a given symbol and frequency (annual or quarterly).
        """
        subfolder = self.download_folder / "financials_as_reported"
        subfolder.mkdir(exist_ok=True)

        # frequency: "annual" or "quarterly"
        print(f"Fetching as-reported financials for {symbol} ({frequency})...")

        try:
            # Get as-reported financial data from Finnhub
            # The method only takes the symbol parameter, not frequency
            data = self.finnhub_client.financials_reported(symbol=symbol, freq=frequency)

            # Create result structure
            result = {
                'symbol': symbol,
                'frequency': frequency,
                'timestamp': datetime.now().isoformat(),
                'data': data
            }

            filename = f"FinancialsAsReported_{symbol}_{frequency}.json"
            file_path = subfolder / filename

            # Save to file
            with open(file_path, 'w') as f:
                json.dump(result, f, indent=2)

            print(f"[SUCCESS] Saved as-reported financials data to {filename}")
            print(f"[SUCCESS] File location: {file_path}")
            return result

        except Exception as e:
            print(f"[ERROR] Error fetching as-reported financials for {symbol}: {e}")
            return None

    def download_multiple_companies_financials_as_reported(self, ticker_list, frequencies=None):

        if frequencies is None:
            frequencies = ['annual', 'quarterly']
        
        print(f"\n=== Starting as-reported financials batch download ===")
        print(f"Companies: {ticker_list}")
        print(f"Frequencies: {frequencies}")
        print("-" * 50)
        
        successful_downloads = []
        failed_downloads = []
        
        total_requests = len(ticker_list) * len(frequencies)
        current_request = 0
        
        for symbol in ticker_list:
            print(f"\n--- Processing {symbol} ---")
            
            for frequency in frequencies:
                current_request += 1
                print(f"\n[{current_request}/{total_requests}] {symbol} - {frequency}")
                
                try:
                    result = self.get_financials_as_reported(symbol, frequency)
                    if result:
                        successful_downloads.append(f"{symbol}_{frequency}")
                    else:
                        failed_downloads.append(f"{symbol}_{frequency}")
                except Exception as e:
                    print(f"[ERROR] Failed to process {symbol}_{frequency}: {e}")
                    failed_downloads.append(f"{symbol}_{frequency}")
        
        # Print summary
        print("\n" + "=" * 50)
        print("DOWNLOAD SUMMARY:")
        print(f"Total requests: {total_requests}")
        print(f"Successful downloads: {len(successful_downloads)}")
        print(f"Failed downloads: {len(failed_downloads)}")
        print("=" * 50)
        
        if successful_downloads:
            print(f"Successful: {successful_downloads}")
        if failed_downloads:
            print(f"Failed: {failed_downloads}")
        
        return {
            'successful': successful_downloads,
            'failed': failed_downloads
        }



    
    def get_company_profile(self, symbol):
        print(f"Fetching company profile for {symbol}...")
        
        subfolder = self.download_folder / "company_profiles"
        subfolder.mkdir(exist_ok=True)
        
        try:
            # Get company profile from Finnhub
            data = self.finnhub_client.company_profile(symbol=symbol)
            
            # Create result structure
            result = {
                'symbol': symbol,
                'data_type': 'company_profile',
                'timestamp': datetime.now().isoformat(),
                'data': data
            }
            
            # Create filename for company profile
            filename = f"company_profile_{symbol}.json"
            file_path = subfolder / filename
            
            # Save to file
            with open(file_path, 'w') as f:
                json.dump(result, f, indent=2)
            
            print(f"[SUCCESS] Saved company profile to {filename}")
            print(f"[SUCCESS] File location: {file_path}")
            return result
            
        except Exception as e:
            print(f"[ERROR] Error fetching company profile for {symbol}: {e}")
            return None
    
    def download_multiple_companies_profiles(self, ticker_list):
        print(f"\n=== Starting company profile batch download ===")
        print(f"Companies: {ticker_list}")
        print("-" * 50)
        
        successful_downloads = []
        failed_downloads = []
        
        total_requests = len(ticker_list)
        current_request = 0
        
        for symbol in ticker_list:
            current_request += 1
            print(f"\n[{current_request}/{total_requests}] Processing {symbol}...")
            
            try:
                result = self.get_company_profile(symbol)
                if result:
                    successful_downloads.append(f"{symbol}_profile")
                else:
                    failed_downloads.append(f"{symbol}_profile")
            except Exception as e:
                print(f"[ERROR] Failed to process {symbol}_profile: {e}")
                failed_downloads.append(f"{symbol}_profile")
        
        # Print summary
        print("\n" + "=" * 50)
        print("DOWNLOAD SUMMARY:")
        print(f"Total requests: {total_requests}")
        print(f"Successful downloads: {len(successful_downloads)}")
        print(f"Failed downloads: {len(failed_downloads)}")
        print("=" * 50)
        
        if successful_downloads:
            print(f"Successful: {successful_downloads}")
        if failed_downloads:
            print(f"Failed: {failed_downloads}")
        
        return {
            'successful': successful_downloads,
            'failed': failed_downloads
        }

    
    def get_dividends(self, symbol, _from=None, to=None):
        print(f"Fetching dividends for {symbol}...")
        subfolder = self.download_folder / "dividends"
        subfolder.mkdir(exist_ok=True)
        
        try:
            # Get dividends from Finnhub
            data = self.finnhub_client.stock_dividends(symbol, _from=_from, to=to)
            
            # Create result structure
            result = {
                'symbol': symbol,
                'data_type': 'dividends',
                'timestamp': datetime.now().isoformat(),
                'data': data
            }
            
            # Create filename for dividends data
            filename = f"dividends_{symbol}.json"
            file_path = subfolder / filename
            
            # Save to file
            with open(file_path, 'w') as f:
                json.dump(result, f, indent=2)
            
            print(f"[SUCCESS] Saved dividends data to {filename}")
            print(f"[SUCCESS] File location: {file_path}")
            return result
            
        except Exception as e:
            print(f"[ERROR] Error fetching dividends data for {symbol}: {e}")
            return None
    
    def download_multiple_companies_dividends(self, ticker_list):
        print(f"\n=== Starting dividends batch download ===")
        print(f"Companies: {ticker_list}")
        print("-" * 50)
        
        successful_downloads = []
        failed_downloads = []
        
        total_requests = len(ticker_list)
        current_request = 0
        
        for symbol in ticker_list:
            current_request += 1
            print(f"\n[{current_request}/{total_requests}] Processing {symbol}...")
            
            try:
                result = self.get_dividends(symbol)
                if result:
                    successful_downloads.append(f"{symbol}_dividends")
                else:
                    failed_downloads.append(f"{symbol}_dividends")
            except Exception as e:
                print(f"[ERROR] Failed to process {symbol}_dividends: {e}")
                failed_downloads.append(f"{symbol}_dividends")
        
        # Print summary
        print("\n" + "=" * 50)
        print("DOWNLOAD SUMMARY:")
        print(f"Total requests: {total_requests}")
        print(f"Successful downloads: {len(successful_downloads)}")
        print(f"Failed downloads: {len(failed_downloads)}")
        print("=" * 50)
        
        if successful_downloads:
            print(f"Successful: {successful_downloads}")
        if failed_downloads:
            print(f"Failed: {failed_downloads}")
        
        return {
            'successful': successful_downloads,
            'failed': failed_downloads
        }
        
        
        
# Example usage
if __name__ == "__main__":
    # Initialize the class - API key will be loaded from config.py
    FD = FinnhubData()
    
    # Or you can still override with a custom API key if needed
    # FD = FinnhubData(api_key="your_custom_key")
    
    sp500_tickers = ["MMM", "AOS", "ABT", "ABBV", "ACN", "ADBE", "AMD", "AES", "AFL", "A", "APD", "ABNB", "AKAM", "ALB", "ARE", "ALGN", "ALLE", "LNT", "ALL", "GOOGL", "GOOG", "MO", "AMZN", "AMCR", "AEE", "AEP", "AXP", "AIG", "AMT", "AWK", "AMP", "AME", "AMGN", "APH", "ADI", "AON", "APA", "APO", "AAPL", "AMAT", "APTV", "ACGL", "ADM", "ANET", "AJG", "AIZ", "T", "ATO", "ADSK", "ADP", "AZO", "AVB", "AVY", "AXON", "BKR", "BALL", "BAC", "BAX", "BDX", "BRK.B", "BBY", "TECH", "BIIB", "BLK", "BX", "XYZ", "BK", "BA", "BKNG", "BSX", "BMY", "AVGO", "BR", "BRO", "BF.B", "BLDR", "BG", "BXP", "CHRW", "CDNS", "CZR", "CPT", "CPB", "COF", "CAH", "KMX", "CCL", "CARR", "CAT", "CBOE", "CBRE", "CDW", "COR", "CNC", "CNP", "CF", "CRL", "SCHW", "CHTR", "CVX", "CMG", "CB", "CHD", "CI", "CINF", "CTAS", "CSCO", "C", "CFG", "CLX", "CME", "CMS", "KO", "CTSH", "COIN", "CL", "CMCSA", "CAG", "COP", "ED", "STZ", "CEG", "COO", "CPRT", "GLW", "CPAY", "CTVA", "CSGP", "COST", "CTRA", "CRWD", "CCI", "CSX", "CMI", "CVS", "DHR", "DRI", "DDOG", "DVA", "DAY", "DECK", "DE", "DELL", "DAL", "DVN", "DXCM", "FANG", "DLR", "DG", "DLTR", "D", "DPZ", "DASH", "DOV", "DOW", "DHI", "DTE", "DUK", "DD", "EMN", "ETN", "EBAY", "ECL", "EIX", "EW", "EA", "ELV", "EMR", "ENPH", "ETR", "EOG", "EPAM", "EQT", "EFX", "EQIX", "EQR", "ERIE", "ESS", "EL", "EG", "EVRG", "ES", "EXC", "EXE", "EXPE", "EXPD", "EXR", "XOM", "FFIV", "FDS", "FICO", "FAST", "FRT", "FDX", "FIS", "FITB", "FSLR", "FE", "FI", "F", "FTNT", "FTV", "FOXA", "FOX", "BEN", "FCX", "GRMN", "IT", "GE", "GEHC", "GEV", "GEN", "GNRC", "GD", "GIS", "GM", "GPC", "GILD", "GPN", "GL", "GDDY", "GS", "HAL", "HIG", "HAS", "HCA", "DOC", "HSIC", "HSY", "HPE", "HLT", "HOLX", "HD", "HON", "HRL", "HST", "HWM", "HPQ", "HUBB", "HUM", "HBAN", "HII", "IBM", "IEX", "IDXX", "ITW", "INCY", "IR", "PODD", "INTC", "IBKR", "ICE", "IFF", "IP", "IPG", "INTU", "ISRG", "IVZ", "INVH", "IQV", "IRM", "JBHT", "JBL", "JKHY", "J", "JNJ", "JCI", "JPM", "K", "KVUE", "KDP", "KEY", "KEYS", "KMB", "KIM", "KMI", "KKR", "KLAC", "KHC", "KR", "LHX", "LH", "LRCX", "LW", "LVS", "LDOS", "LEN", "LII", "LLY", "LIN", "LYV", "LKQ", "LMT", "L", "LOW", "LULU", "LYB", "MTB", "MPC", "MKTX", "MAR", "MMC", "MLM", "MAS", "MA", "MTCH", "MKC", "MCD", "MCK", "MDT", "MRK", "META", "MET", "MTD", "MGM", "MCHP", "MU", "MSFT", "MAA", "MRNA", "MHK", "MOH", "TAP", "MDLZ", "MPWR", "MNST", "MCO", "MS", "MOS", "MSI", "MSCI", "NDAQ", "NTAP", "NFLX", "NEM", "NWSA", "NWS", "NEE", "NKE", "NI", "NDSN", "NSC", "NTRS", "NOC", "NCLH", "NRG", "NUE", "NVDA", "NVR", "NXPI", "ORLY", "OXY", "ODFL", "OMC", "ON", "OKE", "ORCL", "OTIS", "PCAR", "PKG", "PLTR", "PANW", "PSKY", "PH", "PAYX", "PAYC", "PYPL", "PNR", "PEP", "PFE", "PCG", "PM", "PSX", "PNW", "PNC", "POOL", "PPG", "PPL", "PFG", "PG", "PGR", "PLD", "PRU", "PEG", "PTC", "PSA", "PHM", "PWR", "QCOM", "DGX", "RL", "RJF", "RTX", "O", "REG", "REGN", "RF", "RSG", "RMD", "RVTY", "ROK", "ROL", "ROP", "ROST", "RCL", "SPGI", "CRM", "SBAC", "SLB", "STX", "SRE", "NOW", "SHW", "SPG", "SWKS", "SJM", "SW", "SNA", "SOLV", "SO", "LUV", "SWK", "SBUX", "STT", "STLD", "STE", "SYK", "SMCI", "SYF", "SNPS", "SYY", "TMUS", "TROW", "TTWO", "TPR", "TRGP", "TGT", "TEL", "TDY", "TER", "TSLA", "TXN", "TPL", "TXT", "TMO", "TJX", "TKO", "TTD", "TSCO", "TT", "TDG", "TRV", "TRMB", "TFC", "TYL", "TSN", "USB", "UBER", "UDR", "ULTA", "UNP", "UAL", "UPS", "URI", "UNH", "UHS", "VLO", "VTR", "VLTO", "VRSN", "VRSK", "VZ", "VRTX", "VTRS", "VICI", "V", "VST", "VMC", "WRB", "GWW", "WAB", "WMT", "DIS", "WBD", "WM", "WAT", "WEC", "WFC", "WELL", "WST", "WDC", "WY", "WSM", "WMB", "WTW", "WDAY", "WYNN", "XEL", "XYL", "YUM", "ZBRA", "ZBH", "ZTS"]    
    
    results = FD.download_multiple_companies_financials_as_reported(sp500_tickers, ['quarterly'])
    
    print(f"\nFinal Results:")
    print(f"Successfully downloaded: {len(results['successful'])} files")
    print(f"Failed downloads: {len(results['failed'])} files")
    
