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
