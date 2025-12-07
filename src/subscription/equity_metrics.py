from tastytrade.market_data import get_market_data_by_type
from tastytrade.metrics import get_market_metrics
from src.session.session_manager import validate_session
import time
import math


class EquityMetrics:
    def __init__(self, session, data_store):
        """
        Initialize the MarketDataCombiner with a session.

        Args:
            session: OAuthSession instance for Tastytrade API.
        """
        self.session = session
        self.data_store = data_store

    @staticmethod
    def _combine_data(metrics_data, market_data):
        """
        Combine metrics and market data into a unified result set per symbol.

        Args:
            metrics_data: List of metric objects with symbol attribute
            market_data: List of market data objects with symbol attribute

        Returns:
            dict: Combined data keyed by symbol
        """
        combined_data = {}

        # First, process all metrics data
        for metric in metrics_data:
            symbol = metric.symbol
            combined_data[symbol] = {
                'symbol': symbol,
                'metrics': metric.model_dump() if hasattr(metric, 'model_dump') else metric.dict(),
                'market_data': None
            }

        # Then, add matching market data
        for equity in market_data:
            symbol = equity.symbol
            if symbol in combined_data:
                # Symbol exists in metrics, add market data
                combined_data[symbol]['market_data'] = equity.model_dump() if hasattr(equity,
                                                                                      'model_dump') else equity.dict()
            else:
                # Symbol only exists in market data, create a new entry
                combined_data[symbol] = {
                    'symbol': symbol,
                    'metrics': None,
                    'market_data': equity.model_dump() if hasattr(equity, 'model_dump') else equity.dict()
                }

        return combined_data

    @staticmethod
    def _chunk_symbols(symbols_list, chunk_size=10):
        """
        Split symbols list into chunks of specified size.

        Args:
            symbols_list: List of symbols to chunk
            chunk_size: Maximum size of each chunk (default 10)

        Returns:
            Generator yielding chunks of symbols
        """
        for i in range(0, len(symbols_list), chunk_size):
            yield symbols_list[i:i + chunk_size]

    def _fetch_metrics_in_batches(self, symbols_list, delay_between_calls=0.1):
        """
        Fetch market metrics in batches to handle API limitations.

        Args:
            symbols_list: List of symbols to fetch metrics for
            delay_between_calls: Delay in seconds between API calls to avoid rate limiting

        Returns:
            List of all metrics data
        """
        all_metrics = []
        chunks = list(self._chunk_symbols(symbols_list, 10))

        print(f"  Fetching metrics for {len(symbols_list)} symbols in {len(chunks)} batches...")

        for i, chunk in enumerate(chunks, 1):
            try:
                print(f"    Metrics batch {i}/{len(chunks)}: {len(chunk)} symbols")
                metrics_data = get_market_metrics(self.session, chunk)
                all_metrics.extend(metrics_data)

                # Add delay between calls to avoid rate limiting (except for the last call)
                if i < len(chunks) and delay_between_calls > 0:
                    time.sleep(delay_between_calls)

            except Exception as e:
                print(f"    Error fetching metrics for batch {i}: {e}")
                # Continue with other batches even if one fails
                continue

        return all_metrics

    def _fetch_market_data_in_batches(self, symbols_list, delay_between_calls=0.1):
        """
        Fetch market data in batches to handle API limitations.

        Args:
            symbols_list: List of symbols to fetch market data for
            delay_between_calls: Delay in seconds between API calls to avoid rate limiting

        Returns:
            List of all market data
        """
        all_market_data = []
        chunks = list(self._chunk_symbols(symbols_list, 10))

        print(f"  Fetching market data for {len(symbols_list)} symbols in {len(chunks)} batches...")

        for i, chunk in enumerate(chunks, 1):
            try:
                print(f"    Market data batch {i}/{len(chunks)}: {len(chunk)} symbols")
                market_data = get_market_data_by_type(self.session, equities=chunk)
                all_market_data.extend(market_data)

                # Add delay between calls to avoid rate limiting (except for the last call)
                if i < len(chunks) and delay_between_calls > 0:
                    time.sleep(delay_between_calls)

            except Exception as e:
                print(f"    Error fetching market data for batch {i}: {e}")
                # Continue with other batches even if one fails
                continue

        return all_market_data

    def _process_symbol_batch(self, symbols_batch, batch_number, total_batches, delay_between_calls=0.1):
        """
        Process a single batch of symbols (up to 50).

        Args:
            symbols_batch: List of symbols to process (max 50)
            batch_number: Current batch number for logging
            total_batches: Total number of batches for logging
            delay_between_calls: Delay between individual API calls within this batch

        Returns:
            dict: Combined data for this batch
        """
        print(f"Processing batch {batch_number}/{total_batches}: {len(symbols_batch)} symbols")
        batch_start_time = time.time()

        # Get both market metrics (IV data) and market data (price info) in batches
        metrics_data = self._fetch_metrics_in_batches(symbols_batch, delay_between_calls)
        market_data = self._fetch_market_data_in_batches(symbols_batch, delay_between_calls)

        # Combine the data for this batch
        batch_combined_data = self._combine_data(metrics_data, market_data)

        # Store each symbol's data
        symbols_processed = 0
        for symbol, data in batch_combined_data.items():
            try:
                # Extract earnings data safely
                earnings = data['metrics'].get('earnings') if data['metrics'] else None

                metrics_dict = {
                    'symbol': symbol,
                    # From metrics
                    'iv_rank': data['metrics'].get('implied_volatility_index_rank') if data['metrics'] else None,
                    'iv_index': data['metrics'].get('implied_volatility_index') if data['metrics'] else None,
                    'iv_percentile': data['metrics'].get('implied_volatility_percentile') if data['metrics'] else None,
                    'liquidity_rating': data['metrics'].get('liquidity_rating') if data['metrics'] else None,
                    'liquidity_value': data['metrics'].get('liquidity_value') if data['metrics'] else None,
                    'iv_30_day': data['metrics'].get('implied_volatility_30_day') if data['metrics'] else None,
                    'hv_30_day': data['metrics'].get('historical_volatility_30_day') if data['metrics'] else None,
                    'iv_hv_difference': data['metrics'].get('iv_hv_30_day_difference') if data['metrics'] else None,
                    'beta': data['metrics'].get('beta') if data['metrics'] else None,
                    'hv_60_day': data['metrics'].get('historical_volatility_60_day') if data['metrics'] else None,
                    'hv_90_day': data['metrics'].get('historical_volatility_90_day') if data['metrics'] else None,
                    'earnings_expected_date': earnings.get('expected_report_date') if earnings else None,
                    'earnings_time_of_day': earnings.get('time_of_day') if earnings else None,

                    # From market_data
                    'bid': data['market_data'].get('bid') if data['market_data'] else None,
                    'ask': data['market_data'].get('ask') if data['market_data'] else None,
                    'last_price': data['market_data'].get('last') if data['market_data'] else None,
                    'close_price': data['market_data'].get('prev_close') if data['market_data'] else None,
                    'volume': data['market_data'].get('volume') if data['market_data'] else None,
                }
            except KeyError as e:
                print(f"Error processing symbol '{symbol}': Missing required key {e}")
                # You can either skip this symbol or create a minimal metrics_dict
                metrics_dict = {'symbol': symbol}  # Minimal fallback
                # Or you could re-raise to stop processing: raise
            except TypeError as e:
                print(f"Error processing symbol '{symbol}': Data type error - {e}")
                metrics_dict = {'symbol': symbol}  # Minimal fallback
            except Exception as e:
                print(f"Error processing symbol '{symbol}': Unexpected error - {e}")
                metrics_dict = {'symbol': symbol}  # Minimal fallback

            # Store the data
            self.data_store.store_metric_data(metrics_dict)
            symbols_processed += 1

        batch_end_time = time.time()
        batch_duration = batch_end_time - batch_start_time

        print(f"  Batch {batch_number} completed in {batch_duration:.2f}s")
        print(f"  Processed: {symbols_processed} symbols")
        print(f"  Symbols with metrics: {sum(1 for data in batch_combined_data.values() if data['metrics'])}")
        print(f"  Symbols with market data: {sum(1 for data in batch_combined_data.values() if data['market_data'])}")

        return batch_combined_data

    def gather_metrics(self, symbols_list, symbols_per_batch=50, delay_between_calls=0.1, delay_between_batches=1.0,
                       verbose=True):
        """
        Fetch market metrics and market data in manageable batches, combine them, and store the results.

        Args:
            symbols_list: List of symbols to fetch data for
            symbols_per_batch: Maximum number of symbols to process in each batch (default 50)
            delay_between_calls: Delay in seconds between individual API calls within a batch (default 0.1)
            delay_between_batches: Delay in seconds between processing each batch (default 1.0)
            verbose: Whether to print detailed progress information (default True)

        Returns:
            dict: Combined data for all symbols
        """
        # Validate the session
        validate_session(self.session)

        total_symbols = len(symbols_list)
        total_batches = math.ceil(total_symbols / symbols_per_batch)

        if verbose:
            print(f"Starting data collection for {total_symbols} symbols...")
            print(f"Processing in {total_batches} batches of up to {symbols_per_batch} symbols each")
            print(f"Delay between API calls: {delay_between_calls}s")
            print(f"Delay between batches: {delay_between_batches}s")

        start_time = time.time()
        all_combined_data = {}

        # Process symbols in batches
        for batch_num in range(total_batches):
            batch_start_idx = batch_num * symbols_per_batch
            batch_end_idx = min(batch_start_idx + symbols_per_batch, total_symbols)
            symbols_batch = symbols_list[batch_start_idx:batch_end_idx]

            # Process this batch
            batch_combined_data = self._process_symbol_batch(
                symbols_batch,
                batch_num + 1,
                total_batches,
                delay_between_calls
            )

            # Add batch results to overall results
            all_combined_data.update(batch_combined_data)

            # Add delay between batches (except for the last batch)
            if batch_num < total_batches - 1 and delay_between_batches > 0:
                if verbose:
                    print(f"  Waiting {delay_between_batches}s before next batch...")
                time.sleep(delay_between_batches)

        end_time = time.time()
        total_duration = end_time - start_time

        if verbose:
            print(f"\n=== DATA COLLECTION SUMMARY ===")
            print(f"Total processing time: {total_duration:.2f} seconds")
            print(f"Total symbols processed: {len(all_combined_data)}")
            print(f"Symbols with metrics: {sum(1 for data in all_combined_data.values() if data['metrics'])}")
            print(f"Symbols with market data: {sum(1 for data in all_combined_data.values() if data['market_data'])}")
            print(f"Average time per symbol: {total_duration / len(all_combined_data):.3f}s")
            print(f"Batches processed: {total_batches}")

        return all_combined_data