from typing import Any, Coroutine

from dotenv import load_dotenv
from os import getenv
import asyncio
from tastytrade import OAuthSession
from tastytrade import DXLinkStreamer
from tastytrade.dxfeed import Candle
from tastytrade.metrics import get_market_metrics
from datetime import datetime


class MarketDataSubscription:
    def __init__(self, session, symbols, data_store):
        """
        Initialize the market data subscription.

        Args:
            symbols: List of trading symbols to subscribe to.
            data_store: MarketDataStore instance for storing data.
            update_interval: Update interval in seconds.
        """
        load_dotenv(override=True)
        self.symbols = symbols
        self.data_store = data_store
        self.update_interval = int(getenv("UPDATEINTERVAL", 60))
        self.session = None
        self.streamer = None
        self.is_running = False
        self.listen_task = None


    async def connect(self):
        """Establish connection to Tastytrade and create a subscription."""
        try:
            # Ensure stale connections/subscriptions are cleared
            if self.streamer:
                await self.stop()

            print("Connecting to Tastytrade...")
            clientSecret = getenv("TT_OAUTH_CLIENT_SECRET", "")
            refreshToken = getenv("TT_OAUTH_REFRESH_TOKEN", "")

            if not clientSecret or not refreshToken:
                raise ValueError("Missing TT_OAUTH_CLIENT_SECRET or TT_OAUTH_REFRESH_TOKEN environment variables")

            self.session = OAuthSession(clientSecret, refreshToken)

            if not self.symbols:
                raise ValueError("Symbol list is empty. Unable to establish subscription.")

            # Create and store the streamer
            self.streamer = DXLinkStreamer(self.session)
            await self.streamer.__aenter__()

            # Subscribe to candles
            await self.streamer.subscribe(Candle, self.symbols, self.update_interval)

            # Set the running flag
            self.is_running = True


            # Start listening in the background
            self.listen_task = asyncio.create_task(self._listen_for_data())

            print(f"Successfully subscribed to symbols: {self.symbols}")

        except Exception as e:
            print(f"Error during connection: {e}")
            await self.cleanup()
            raise


    async def _listen_for_data(self):
        """Internal method to listen for streaming data"""
        try:
            async for candle in self.streamer.listen(Candle):
                if not self.is_running:
                    break

                try:
                    self.on_candle(candle)
                except Exception as e:
                    print(f"Error processing candle data: {e}")

        except asyncio.CancelledError:
            print("Data listening task was cancelled")
        except Exception as e:
            print(f"Error in data listening loop: {e}")


    async def stop(self):
        """Stop the subscription and clean up resources gracefully."""
        print("Stopping market data subscription...")

        try:
            # Set flag to stop listening
            self.is_running = False

            # Cancel the listening task
            if self.listen_task and not self.listen_task.done():
                self.listen_task.cancel()
                try:
                    await self.listen_task
                except asyncio.CancelledError:
                    print("Listen task cancelled successfully")

            # Unsubscribe from symbols
            if self.streamer and self.symbols:
                try:
                    await self.streamer.unsubscribe(Candle, self.symbols)
                    print(f"Unsubscribed from symbols: {self.symbols}")
                except Exception as e:
                    print(f"Error unsubscribing from symbols: {e}")

            # Close the streamer
            await self.cleanup()

            print("Market data subscription stopped successfully")

        except Exception as e:
            print(f"Error during stop: {e}")


    async def cleanup(self):
        """Clean up streamer and session resources"""
        try:
            if self.streamer:
                try:
                    await self.streamer.__aexit__(None, None, None)
                    print("Streamer closed successfully")
                except Exception as e:
                    print(f"Error closing streamer: {e}")
                finally:
                    self.streamer = None

            # Clear session reference
            self.session = None

        except Exception as e:
            print(f"Error during cleanup: {e}")

    def on_quote(self, quote_data):
        """Callback function to handle incoming quote data."""
        try:
            self.data_store.store_data(quote_data)
        except Exception as e:
            print(f"Error processing quote data: {e}")

    def on_candle(self, candle_data):
        """Callback function to handle incoming candle data."""
        try:
            # get metric data
            m_data = get_market_metrics(self.session, [candle_data.event_symbol])

            # Convert a candle object to the dictionary format expected by store
            candle_dict = {
                'event_type': 'Candle',
                'event_symbol': candle_data.event_symbol,
                'time': candle_data.time,
                'open': candle_data.open,
                'high': candle_data.high,
                'low': candle_data.low,
                'close': candle_data.close,
                'volume': candle_data.volume,
                'bid_volume': candle_data.bid_volume,
                'ask_volume': candle_data.ask_volume,
                'imp_volatility': candle_data.imp_volatility,
            }

            # add the market metric data
            if m_data:
                candle_dict.update({
                    'iv_index': m_data[0].implied_volatility_index,
                    'iv_index_5_day_change': m_data[0].implied_volatility_index_5_day_change,
                    'iv_index_rank': m_data[0].implied_volatility_index_rank,
                    'tos_iv_index_rank': m_data[0].tos_implied_volatility_index_rank,
                    'tw_iv_index_rank': m_data[0].tw_implied_volatility_index_rank,
                    'iv_percentile': m_data[0].implied_volatility_percentile,
                    'liquidity_rating': m_data[0].liquidity_rating,
                    'beta': m_data[0].beta,
                    'corr_spy_3month': m_data[0].corr_spy_3month,
                    'liquidity_value': m_data[0].liquidity_value,
                    'liquidity_rank': m_data[0].liquidity_rank,
                })

            print(candle_dict)
            self.data_store.store_candle_data(candle_dict)
        except Exception as e:
            print(f"Error processing candle data: {e}")

    async def __aenter__(self):
        """Async context manager entry"""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.stop()


    async def download_historical_data(
        self,
        session,
        symbols: list[str],
        interval: str,
        start: datetime,
        end: datetime = datetime.now(),
    ) -> None:
        data = {symbol: [] for symbol in symbols}
        end_ms = int(end.timestamp() * 1000)
        start_ms = int(start.timestamp() * 1000)
        completed_symbols = set()

        try:
            async with DXLinkStreamer(session) as streamer:
                self.streamer = streamer
                self.session = session

                await streamer.subscribe_candle(
                    symbols,
                    interval,
                    start_time=start,
                    extended_trading_hours=False
                )

                candle_iter = streamer.listen(Candle).__aiter__()

                while True:
                    try:
                        candle = await asyncio.wait_for(anext(candle_iter), timeout=3.0)
                    except asyncio.TimeoutError:
                        print("No candles received in 3 seconds; breaking out.")
                        break

                    symbol = candle.event_symbol.split('{')[0]

                    if candle.time < start_ms:
                        completed_symbols.add(symbol)
                        if len(completed_symbols) == len(symbols):
                            break
                        continue

                    if candle.close == 0:
                        completed_symbols.add(symbol)
                        if len(completed_symbols) == len(symbols):
                            break
                    else:
                        data[symbol].append(candle.model_dump())

        finally:
            await self.cleanup()

        for symbol in symbols:
            self.data_store.store_metric_data_history(symbol, data[symbol])
            print(f"{symbol}: {len(data[symbol])} candles")