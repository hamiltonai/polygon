import csv
import logging
from polygon import RESTClient
from config import POLYGON_API_KEY, S3_BUCKET, SNS_TOPIC_ARN, AWS_S3_ENABLED
from utils import get_date_str, upload_to_s3, send_sns_notification

logger = logging.getLogger(__name__)

def get_search_configurations():
    """
    Define comprehensive search configurations to capture all possible symbols
    
    Returns:
        list: List of search configuration dictionaries
    """
    return [
        {
            'name': 'NASDAQ_Stocks',
            'market': 'stocks',
            'exchange': 'XNAS',
            'type_filter': 'CS',
            'active': True,
            'priority': 1
        },
        {
            'name': 'NYSE_Stocks', 
            'market': 'stocks',
            'exchange': 'XNYS',
            'type_filter': 'CS',
            'active': True,
            'priority': 1
        },
        {
            'name': 'NYSE_American',
            'market': 'stocks',
            'exchange': 'XASE',
            'type_filter': 'CS',
            'active': True,
            'priority': 2
        },
        {
            'name': 'NYSE_Arca',
            'market': 'stocks',
            'exchange': 'ARCX',
            'type_filter': 'CS',
            'active': True,
            'priority': 2
        },
        {
            'name': 'CBOE_BZX',
            'market': 'stocks',
            'exchange': 'BATS',
            'type_filter': 'CS',
            'active': True,
            'priority': 3
        },
        {
            'name': 'All_US_Stocks',
            'market': 'stocks',
            'exchange': None,
            'type_filter': 'CS',
            'active': True,
            'priority': 4
        },
        {
            'name': 'All_Active_Securities',
            'market': None,
            'exchange': None,
            'type_filter': None,
            'active': True,
            'priority': 5
        }
    ]

def validate_symbol(symbol):
    """
    Validate if symbol meets basic criteria for inclusion
    
    Args:
        symbol (str): Stock symbol to validate
        
    Returns:
        bool: True if symbol is valid for inclusion
    """
    if not symbol or len(symbol) > 8:
        return False
    
    # Clean symbol
    clean_symbol = symbol.strip().upper()
    
    # Allow alphanumeric symbols with dots and hyphens
    allowed_chars = clean_symbol.replace('.', '').replace('-', '')
    if not allowed_chars.isalnum():
        return False
    
    # Exclude obvious test symbols
    test_symbols = ['TEST', 'EXAMPLE', 'DEMO']
    if clean_symbol in test_symbols:
        return False
    
    return True

def process_ticker_data(ticker, search_config_name):
    """
    Process individual ticker data from API response
    
    Args:
        ticker: Ticker object from Polygon API
        search_config_name (str): Name of search configuration used
        
    Returns:
        dict or None: Processed ticker data or None if invalid
    """
    try:
        symbol = ticker.ticker.strip().upper()
        
        if not validate_symbol(symbol):
            return None
        
        return {
            'symbol': symbol,
            'name': getattr(ticker, 'name', 'N/A'),
            'type': getattr(ticker, 'type', 'N/A'),
            'market': getattr(ticker, 'market', 'N/A'),
            'exchange': getattr(ticker, 'primary_exchange', 'N/A'),
            'active': getattr(ticker, 'active', True),
            'currency': getattr(ticker, 'currency_name', 'USD'),
            'search_source': search_config_name
        }
    except Exception as e:
        logger.debug(f"Error processing ticker {getattr(ticker, 'ticker', 'UNKNOWN')}: {e}")
        return None

def search_symbols_with_config(client, config):
    """
    Search for symbols using a specific configuration
    
    Args:
        client: Polygon RESTClient instance
        config (dict): Search configuration
        
    Returns:
        tuple: (symbols_found, error_message)
    """
    logger.info(f"Searching with config: {config['name']}")
    
    # Build search parameters
    search_params = {'limit': 1000}
    
    if config['market']:
        search_params['market'] = config['market']
    if config['exchange']:
        search_params['exchange'] = config['exchange']
    if config['active'] is not None:
        search_params['active'] = config['active']
    
    try:
        symbols_found = []
        symbol_set = set()  # Track duplicates
        page_count = 0
        max_items = 100000  # Reasonable limit per config
        
        logger.debug(f"Search parameters: {search_params}")
        
        tickers_iter = client.list_tickers(**search_params)
        
        for ticker in tickers_iter:
            # Apply type filter if specified
            if config['type_filter'] and hasattr(ticker, 'type'):
                if ticker.type != config['type_filter']:
                    continue
            
            # Process ticker data
            ticker_data = process_ticker_data(ticker, config['name'])
            
            if ticker_data and ticker_data['symbol'] not in symbol_set:
                symbols_found.append(ticker_data)
                symbol_set.add(ticker_data['symbol'])
            
            page_count += 1
            if page_count >= max_items:
                logger.warning(f"Reached limit for {config['name']}: {max_items} items")
                break
        
        logger.info(f"Config {config['name']}: Found {len(symbols_found)} unique symbols")
        return symbols_found, None
        
    except Exception as e:
        error_msg = f"Error in config {config['name']}: {str(e)}"
        logger.error(error_msg)
        return [], error_msg

def deduplicate_symbols(all_symbols):
    """
    Remove duplicate symbols, keeping the one from highest priority source
    
    Args:
        all_symbols (list): List of symbol dictionaries
        
    Returns:
        list: Deduplicated list of symbols
    """
    logger.info(f"Deduplicating {len(all_symbols)} symbols...")
    
    # Group by symbol
    symbol_groups = {}
    for symbol_data in all_symbols:
        symbol = symbol_data['symbol']
        if symbol not in symbol_groups:
            symbol_groups[symbol] = []
        symbol_groups[symbol].append(symbol_data)
    
    # Get search config priorities
    configs = get_search_configurations()
    priority_map = {config['name']: config['priority'] for config in configs}
    
    # Keep best version of each symbol
    deduplicated = []
    for symbol, symbol_list in symbol_groups.items():
        # Sort by priority (lower number = higher priority)
        best_symbol = min(symbol_list, 
                         key=lambda x: priority_map.get(x['search_source'], 999))
        deduplicated.append(best_symbol)
    
    logger.info(f"After deduplication: {len(deduplicated)} unique symbols")
    return sorted(deduplicated, key=lambda x: x['symbol'])

def filter_for_trading_suitability(symbols):
    """
    Filter symbols for trading suitability (common stocks, active, USD)
    
    Args:
        symbols (list): List of symbol dictionaries
        
    Returns:
        list: Filtered symbols suitable for trading
    """
    logger.info(f"Filtering {len(symbols)} symbols for trading suitability...")
    
    suitable_symbols = []
    
    for symbol_data in symbols:
        # Check if suitable for trading
        is_suitable = (
            symbol_data.get('type') == 'CS' and  # Common stock
            symbol_data.get('active', True) and  # Active
            symbol_data.get('currency', 'USD').upper() == 'USD' and  # USD currency
            len(symbol_data.get('symbol', '')) <= 6  # Reasonable symbol length
        )
        
        if is_suitable:
            suitable_symbols.append(symbol_data)
    
    logger.info(f"Filtered to {len(suitable_symbols)} trading-suitable symbols")
    return suitable_symbols

def save_symbols_to_csv(symbols, filename):
    """
    Save symbols to CSV file
    
    Args:
        symbols (list): List of symbol dictionaries
        filename (str): Output filename
        
    Returns:
        bool: True if successful
    """
    try:
        with open(filename, "w", newline="") as f:
            # Write simple format for compatibility
            writer = csv.writer(f)
            writer.writerow(["symbol"])
            
            for symbol_data in symbols:
                writer.writerow([symbol_data['symbol']])
        
        logger.info(f"Symbols written to {filename} ({len(symbols)} symbols)")
        return True
        
    except Exception as e:
        logger.error(f"Error writing CSV file {filename}: {e}")
        return False

def save_detailed_symbols_to_csv(symbols, filename):
    """
    Save detailed symbol information to CSV file
    
    Args:
        symbols (list): List of symbol dictionaries
        filename (str): Output filename
        
    Returns:
        bool: True if successful
    """
    try:
        if not symbols:
            logger.warning("No symbols to save")
            return False
        
        fieldnames = symbols[0].keys()
        
        with open(filename, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(symbols)
        
        logger.info(f"Detailed symbols written to {filename}")
        return True
        
    except Exception as e:
        logger.error(f"Error writing detailed CSV file {filename}: {e}")
        return False

def get_comprehensive_stock_symbols(date_str=None):
    """
    Get comprehensive list of stock symbols from all US exchanges using Polygon API
    
    Args:
        date_str (str): Date string for file naming
        
    Returns:
        bool: True if successful, False otherwise
    """
    if not date_str:
        date_str = get_date_str()
    
    if not POLYGON_API_KEY:
        logger.error("POLYGON_API_KEY not found")
        return False
    
    try:
        client = RESTClient(POLYGON_API_KEY)
        
        logger.info("Starting comprehensive symbol collection...")
        logger.info("This will search across multiple exchanges and markets")
        
        # Get search configurations
        configs = get_search_configurations()
        all_symbols = []
        config_stats = {}
        
        # Run searches with different configurations
        for config in configs:
            symbols_found, error = search_symbols_with_config(client, config)
            
            if error:
                logger.warning(f"Config {config['name']} failed: {error}")
                config_stats[config['name']] = {'symbols': 0, 'error': error}
            else:
                all_symbols.extend(symbols_found)
                config_stats[config['name']] = {'symbols': len(symbols_found), 'error': None}
        
        if not all_symbols:
            logger.error("No symbols collected from any configuration")
            return False
        
        logger.info(f"Total symbols collected: {len(all_symbols)}")
        
        # Deduplicate symbols
        unique_symbols = deduplicate_symbols(all_symbols)
        
        # Filter for trading suitability
        trading_symbols = filter_for_trading_suitability(unique_symbols)
        
        # Save main symbols file (simple format for compatibility)
        main_filename = f"nasdaq_symbols_{date_str}.csv"
        if not save_symbols_to_csv(trading_symbols, main_filename):
            return False
        
        # Save detailed symbols file
        detailed_filename = f"comprehensive_symbols_detailed_{date_str}.csv"
        save_detailed_symbols_to_csv(unique_symbols, detailed_filename)
        
        # Upload main file to S3
        if AWS_S3_ENABLED and S3_BUCKET:
            s3_key = f"stock_data/symbols/{main_filename}"
            if not upload_to_s3(S3_BUCKET, s3_key, main_filename):
                logger.error("Failed to upload symbols to S3")
                return False
            logger.info(f"Symbols uploaded to S3: {s3_key}")
        
        # Prepare summary message
        success_configs = [name for name, stats in config_stats.items() if not stats['error']]
        total_unique = len(unique_symbols)
        total_trading = len(trading_symbols)
        
        message = (
            f"📋 COMPREHENSIVE SYMBOL COLLECTION COMPLETE\n\n"
            f"Date: {date_str}\n"
            f"Configurations used: {len(success_configs)}/{len(configs)}\n"
            f"Total unique symbols: {total_unique:,}\n"
            f"Trading-suitable symbols: {total_trading:,}\n"
            f"Main file: {main_filename}\n"
            f"Detailed file: {detailed_filename}\n\n"
            f"Search coverage:\n"
        )
        
        # Add config statistics
        for config_name, stats in config_stats.items():
            if stats['error']:
                message += f"❌ {config_name}: {stats['error']}\n"
            else:
                message += f"✅ {config_name}: {stats['symbols']:,} symbols\n"
        
        # Add sample symbols
        sample_symbols = [s['symbol'] for s in trading_symbols[:15]]
        message += f"\nSample symbols: {', '.join(sample_symbols)}"
        
        if len(trading_symbols) > 15:
            message += f" (+{len(trading_symbols) - 15:,} more)"
        
        logger.info("Symbol collection completed successfully")
        
        # Send notification
        if SNS_TOPIC_ARN:
            subject = f"✅ Comprehensive Symbols Complete - {total_trading:,} symbols"
            send_sns_notification(SNS_TOPIC_ARN, subject, message)
        
        return True
        
    except Exception as e:
        error_msg = f"Error in comprehensive symbol collection: {str(e)}"
        logger.error(error_msg)
        
        # Send error notification
        if SNS_TOPIC_ARN:
            send_sns_notification(
                SNS_TOPIC_ARN,
                "❌ Symbol Collection Failed",
                f"Error: {error_msg}\nDate: {date_str or get_date_str()}"
            )
        
        return False

# Keep original function name for backward compatibility
def get_nasdaq_symbols(date_str=None):
    """
    Backward compatibility wrapper for comprehensive symbol collection
    
    Args:
        date_str (str): Date string for file naming
        
    Returns:
        bool: True if successful, False otherwise
    """
    return get_comprehensive_stock_symbols(date_str)

if __name__ == "__main__":
    from config import setup_logging, validate_config
    
    # Setup
    setup_logging()
    validate_config()
    
    # Test the function
    success = get_comprehensive_stock_symbols()
    if success:
        print("✅ Comprehensive symbol collection completed successfully")
    else:
        print("❌ Symbol collection failed")
        exit(1)