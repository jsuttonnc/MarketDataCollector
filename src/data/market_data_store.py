from typing import Dict, List, Any
from src.data.db_connector import DatabaseConnector


class MarketDataStore:
    """Class responsible for storing and managing market data"""

    def __init__(self):
        """Initialize the data store"""
        db_connector = DatabaseConnector()
        db_connector.connect()
        self.db = db_connector

        """Initialize the data store"""
        self.market_data = {}


    def store_candle_data(self, candle_data: Dict[str, Any]) -> None:
        """
        Store and process incoming market data.

        Args:
            quote_data: Dictionary containing market data quote information
        """
        insert_sql = """
                     INSERT INTO market_data (event_type, event_symbol, time, open, high, low, close, volume, \
                                              bid_volume, ask_volume, imp_volatility, iv_index, iv_index_5_day_change, \
                                              iv_index_rank, tos_iv_index_rank, tw_iv_index_rank, iv_percentile, \
                                              liquidity_rating, beta, corr_spy_3month, liquidity_value, liquidity_rank) \
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) \
                     """

        # Parameters tuple for the INSERT statement
        params = (
            candle_data.get('event_type'),
            candle_data.get('event_symbol'),
            candle_data.get('time'),
            candle_data.get('open'),
            candle_data.get('high'),
            candle_data.get('low'),
            candle_data.get('close'),
            None if candle_data['volume'] is None else float(candle_data['volume']),  # volume
            None if candle_data['bid_volume'] is None else float(candle_data['bid_volume']),  # bid_volume
            None if candle_data['ask_volume'] is None else float(candle_data['ask_volume']),  # ask_volume
            None if candle_data['imp_volatility'] is None else float(candle_data['imp_volatility']),  # imp_volatility
            candle_data.get('iv_index'),
            candle_data.get('iv_index_5_day_change'),
            candle_data.get('iv_index_rank'),
            candle_data.get('tos_iv_index_rank'),
            candle_data.get('tw_iv_index_rank'),
            candle_data.get('iv_percentile'),
            candle_data.get('liquidity_rating'),
            candle_data.get('beta'),
            candle_data.get('corr_spy_3month'),
            candle_data.get('liquidity_value'),
            candle_data.get('liquidity_rank')
        )

        # Execute the query using `self.db.execute_query`
        try:
            self.db.execute_query(insert_sql, params=params)
        except Exception as e:
            print(f"Error inserting data: {e}")

    def store_metric_data(self, metrics_data: Dict[str, Any]) -> None:
        """
        Store equity metrics and market data.

        Args:
            metrics_data: Dictionary containing combined metrics and market data information
        """
        insert_sql = """
                     INSERT INTO equity_data (symbol, bid, ask, last_price, close_price, volume, \
                                              implied_volatility_index, implied_volatility_index_rank, \
                                              implied_volatility_percentile, liquidity_rating, liquidity_value, \
                                              implied_volatility_30_day, historical_volatility_30_day, \
                                              iv_hv_30_day_difference, historical_volatility_60_day, \
                                              historical_volatility_90_day, beta, earnings_expected_report_date, \
                                              earnings_time_of_day) \
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) \
                     """

        # Parameters tuple for the INSERT statement
        params = (
            metrics_data.get('symbol'),
            None if metrics_data.get('bid') is None else float(metrics_data.get('bid')),
            None if metrics_data.get('ask') is None else float(metrics_data.get('ask')),
            None if metrics_data.get('last_price') is None else float(metrics_data.get('last_price')),
            None if metrics_data.get('close_price') is None else float(metrics_data.get('close_price')),
            None if metrics_data.get('volume') is None else int(metrics_data.get('volume')),
            None if metrics_data.get('iv_index') is None else float(metrics_data.get('iv_index')),
            None if metrics_data.get('iv_rank') is None else float(metrics_data.get('iv_rank')),
            None if metrics_data.get('iv_percentile') is None else float(metrics_data.get('iv_percentile')),
            None if metrics_data.get('liquidity_rating') is None else float(metrics_data.get('liquidity_rating')),
            None if metrics_data.get('liquidity_value') is None else float(metrics_data.get('liquidity_value')),
            None if metrics_data.get('iv_30_day') is None else float(metrics_data.get('iv_30_day')),
            None if metrics_data.get('hv_30_day') is None else float(metrics_data.get('hv_30_day')),
            None if metrics_data.get('iv_hv_difference') is None else float(metrics_data.get('iv_hv_difference')),
            None if metrics_data.get('hv_60_day') is None else float(metrics_data.get('hv_60_day')),
            None if metrics_data.get('hv_90_day') is None else float(metrics_data.get('hv_90_day')),
            None if metrics_data.get('beta') is None else float(metrics_data.get('beta')),
            metrics_data.get('earnings_expected_date'),  # Date field, keep as-is
            metrics_data.get('earnings_time_of_day')  # String field, keep as-is
        )

        # Execute the query using `self.db.execute_query`
        try:
            self.db.execute_query(insert_sql, params=params)
            print(f"Successfully stored metrics data for symbol: {metrics_data.get('symbol')}")
        except Exception as e:
            print(f"Error inserting metrics data for {metrics_data.get('symbol')}: {e}")


    def get_stored_data(self, symbol: str = None) -> Dict[str, List[Dict[str, Any]]]:
        """
        Retrieve stored market data for a specific symbol or all symbols.

        Args:
            symbol: Optional symbol to retrieve data for. If None, returns all data.

        Returns:
            Dictionary containing stored market data
        """
        if symbol:
            return {symbol: self.market_data.get(symbol, [])}
        return self.market_data


    def clear_data(self, symbol: str = None) -> None:
        """
        Clear the stored data for a specific symbol or all symbols.

        Args:
            symbol: Optional symbol to clear data for. If None, clears all data.
        """
        if symbol:
            self.market_data.pop(symbol, None)
        else:
            self.market_data.clear()
