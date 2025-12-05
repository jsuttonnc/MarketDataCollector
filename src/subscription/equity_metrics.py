from tastytrade.market_data import get_market_data_by_type
from tastytrade.metrics import get_market_metrics
from src.session.session_manager import validate_session


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

    def gather_metrics(self, symbols_list):
        """
        Fetch market metrics and market data, combine them, and display the results.

        Args:
            symbols_list: List of symbols to fetch data for (e.g., ["AMD", "MU"])
        """
        # validate the session
        validate_session(self.session)

        # Get both market metrics (IV data) and market data (price info)
        metrics_data = get_market_metrics(self.session, symbols_list)
        market_data = get_market_data_by_type(self.session, equities=symbols_list)

        # Combine the data
        combined_data = self._combine_data(metrics_data, market_data)

        # Print the combined data for each symbol
        for symbol, data in combined_data.items():
            # print(f"=== {symbol} ===")
            # Inside the loop where you're processing each symbol
            metrics_dict = {
                'symbol': symbol,
                'iv_rank': data['metrics'].get('implied_volatility_index_rank') if data['metrics'] else None,
                'iv_index': data['metrics'].get('implied_volatility_index') if data['metrics'] else None,
                'iv_percentile': data['metrics'].get('implied_volatility_percentile') if data['metrics'] else None,
                'liquidity_rating': data['metrics'].get('liquidity_rating') if data['metrics'] else None,
                'liquidity_value': data['metrics'].get('liquidity_value') if data['metrics'] else None,
                'iv_30_day': data['metrics'].get('implied_volatility_30_day') if data['metrics'] else None,
                'hv_30_day': data['metrics'].get('historical_volatility_30_day') if data['metrics'] else None,
                'iv_hv_difference': data['metrics'].get('iv_hv_30_day_difference') if data['metrics'] else None,
                'bid': data['market_data'].get('bid') if data['market_data'] else None,
                'ask': data['market_data'].get('ask') if data['market_data'] else None,
                'last_price': data['market_data'].get('last') if data['market_data'] else None,
                'close_price': data['market_data'].get('close') if data['market_data'] else None,
                'beta': data['metrics'].get('beta') if data['metrics'] else None,
                'hv_60_day': data['metrics'].get('historical_volatility_60_day') if data['metrics'] else None,
                'hv_90_day': data['metrics'].get('historical_volatility_90_day') if data['metrics'] else None,
                'volume': data['market_data'].get('volume') if data['market_data'] else None,
                'earnings_expected_date': data['metrics'].get('earnings', {}).get('expected_report_date') if data['metrics'] else None,
                'earnings_time_of_day': data['metrics'].get('earnings', {}).get('time_of_day') if data['metrics'] else None
            }

            # store the data
            self.data_store.store_metric_data(metrics_dict)

        return combined_data