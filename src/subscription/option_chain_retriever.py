from tastytrade.instruments import get_option_chain
from tastytrade.instruments import NestedOptionChain
from tastytrade.metrics import get_market_metrics
from src.session.session_manager import validate_session
from datetime import datetime, timedelta


class OptionChainRetriever:
    def __init__(self, session):
        self.session = session

    def get_index_call_options_above_price(self, symbol: str, price_offset: float = 100.0, days_to_expiry: int = 30):
        """
        Get call option chain for an index that is $100 above current price

        Args:
            symbol: Index symbol (e.g., 'SPX', 'VIX')
            price_offset: Dollar amount above current price (default: $100)
            days_to_expiry: Maximum days to expiration for options

        Returns:
            List of call options with strikes around the target price
        """
        try:
            validate_session(self.session)

            # Get current market data for the index
            market_metrics = get_market_metrics(self.session, [symbol])

            # Check if we got data and if our symbol is in the results
            if not market_metrics:
                raise ValueError(f"No market data returned for {symbol}")

            # Find the metric for our symbol
            metric_info = None
            for metric in market_metrics:
                if metric.symbol == symbol:
                    metric_info = metric
                    break

            if not metric_info:
                available_symbols = [m.symbol for m in market_metrics]
                raise ValueError(f"Symbol {symbol} not found in market data. Available symbols: {available_symbols}")

            current_price = 6700
            target_strike = current_price + price_offset

            print(f"Current {symbol} price: ${current_price:.2f}")
            print(f"Target strike around: ${target_strike:.2f}")

            # Get option chain
            #option_chain = get_option_chain(self.session, symbol)
            option_chain = NestedOptionChain.get(self.session, symbol)
            print(option_chain[0].expirations[0].strikes)

            # Filter for call options near target strike
            target_calls = []
            expiry_cutoff = datetime.now() + timedelta(days=days_to_expiry)

            for chain_data in option_chain.items():
                exp_date =  chain_data[0]
                strike_data = chain_data[1]

                # Filter strikes within a reasonable range of target ($10 above and below)
                if abs(strike_price - target_strike) <= 10:

                    for expiration in strike_data.expirations:
                        exp_date = datetime.strptime(expiration.expiration_date, '%Y-%m-%d')

                        # Only include options that expire within our timeframe
                        if exp_date <= expiry_cutoff:

                            # Look for call options
                            for option in expiration.strikes:
                                if option.option_type == 'C':  # Call option
                                    target_calls.append({
                                        'symbol': option.symbol,
                                        'strike': strike_price,
                                        'expiration': expiration.expiration_date,
                                        'option_type': 'Call',
                                        'bid': option.bid,
                                        'ask': option.ask,
                                        'last': option.last,
                                        'volume': option.volume,
                                        'open_interest': option.open_interest,
                                        'delta': option.greeks.delta if option.greeks else None,
                                        'gamma': option.greeks.gamma if option.greeks else None,
                                        'theta': option.greeks.theta if option.greeks else None,
                                        'vega': option.greeks.vega if option.greeks else None,
                                        'implied_volatility': option.implied_volatility
                                    })

            # Sort by expiration date and strike
            target_calls.sort(key=lambda x: (x['expiration'], x['strike']))

            return target_calls

        except Exception as e:
            print(f"Error retrieving option chain: {e}")
            raise

    def get_closest_call_strike_above_price(self, symbol: str, price_offset: float = 100.0):
        """
        Get the single closest call option strike above the current price + offset

        Args:
            symbol: Index symbol (e.g., 'SPX', 'VIX')
            price_offset: Dollar amount above current price

        Returns:
            Single option contract closest to target price
        """
        options = self.get_index_call_options_above_price(symbol, price_offset)

        if not options:
            return None

        # Get current price again for calculation
        market_metrics = get_market_metrics(self.session, [symbol])
        current_price = market_metrics[symbol].close
        target_strike = current_price + price_offset

        # Find closest strike to target
        closest_option = min(options, key=lambda x: abs(x['strike'] - target_strike))

        return closest_option
