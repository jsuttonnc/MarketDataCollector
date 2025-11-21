# import json
import os
import yaml
import asyncio
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from src.data.market_data_store import MarketDataStore
from src.subscription.market_data_subscription import MarketDataSubscription
# from src.messages.push_notifications import send_pushover_notification
from src.session.session_manager import create_session


def load_symbols():
    """Load symbols from the symbols.yaml file."""
    symbols_file = os.path.join(os.path.dirname(__file__), 'symbols.yaml')
    try:
        with open('symbols.yaml', 'r') as f:
            symbols_data = yaml.safe_load(f)
            return symbols_data
    except FileNotFoundError:
        raise FileNotFoundError(f"Symbols file not found: {symbols_file}")
    except yaml.YAMLError as e:
        raise ValueError(f"Invalid YAML in symbols file: {symbols_file}. Error: {e}")


@asynccontextmanager
async def market_subscription_manager(session, symbols):
    """Context manager for market subscription lifecycle"""
    data_store = None
    market_sub = None

    try:
        # Setup
        data_store = MarketDataStore()
        market_sub = MarketDataSubscription(session, symbols, data_store)
        await market_sub.connect()

        yield market_sub

    finally:
        # Cleanup
        print("Cleaning up resources...")
        if market_sub:
            try:
                if asyncio.iscoroutinefunction(market_sub.stop):
                    await market_sub.stop()
                else:
                    market_sub.stop()
            except Exception as e:
                print(f"Error stopping streamer: {e}")

        if data_store and hasattr(data_store, 'disconnect'):
            try:
                data_store.disconnect()
            except Exception as e:
                print(f"Error disconnecting database: {e}")


async def main():
    try:
        # load symbols
        symbols = load_symbols()
        print(f"Loaded symbols: {symbols}")
    except Exception as e:
        print(f"Error loading symbols: {e}")
        return

    try:
        # create session
        session = create_session()
    except Exception as e:
        print(f"Error creating session: {e}")
        return

    try:
        # create a subscription
        indices_list = symbols['indices']
        async with market_subscription_manager(session, indices_list) as market_sub:
            print("Streamer is running. Press Ctrl+C to stop.")
            while True:
                await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("\nShutdown requested. Exiting Script...")


if __name__ == "__main__":
    load_dotenv()
    # send_pushover_notification("The TastyData container has started successfully!")

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Application interrupted")
