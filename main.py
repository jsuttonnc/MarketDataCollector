import os
import yaml
import asyncio
from datetime import datetime
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv
from src.data.market_data_store import MarketDataStore
from src.subscription.market_data_subscription import MarketDataSubscription
# from src.messages.push_notifications import send_pushover_notification
from src.session.session_manager import create_session
from src.subscription.equity_metrics import EquityMetrics
from tastytrade.market_data import get_market_data_by_type
from tastytrade.order import InstrumentType
from tastytrade.metrics import get_market_metrics

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
async def market_subscription_manager(session, symbols, data_store):
    """Context manager for market subscription lifecycle"""
    market_sub = None

    try:
        # Setup
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
                    await market_sub.stop()
            except Exception as e:
                print(f"Error stopping streamer: {e}")

        if data_store and hasattr(data_store, 'disconnect'):
            try:
                data_store.disconnect()
            except Exception as e:
                print(f"Error disconnecting database: {e}")


async def daily_task():
    """Task that runs once daily"""
    print(f"Running daily task at {datetime.now()}")
    try:
        # Add your specific daily task logic here
        # For example:
        # - Database cleanup
        # - Generate daily reports
        # - Send notifications
        # - Process accumulated data

        # Example task
        data_store = MarketDataStore()
        # await data_store.cleanup_old_data()
        print("Daily maintenance task completed successfully")

    except Exception as e:
        print(f"Error in daily task: {e}")


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

        # create the data store
        data_store = MarketDataStore()

    except Exception as e:
        print(f"Error creating session: {e}")
        return

    # Get both market metrics (IV data) and market data (price info)
    eqMetrics = EquityMetrics(session, data_store)
    combined_data = eqMetrics.gather_metrics(["AMD"])
    return

    try:
        # Initialize scheduler
        scheduler = AsyncIOScheduler()

        # Schedule the task using cron syntax
        scheduler.add_job(
            daily_task,
            CronTrigger(hour=3, minute=0, second=0),  # Run at 3:00:00 AM daily
            id='daily_task',
            name='Daily Maintenance Task',
            max_instances=1  # Prevent overlapping executions
        )
    except Exception as e:
        print(f"Error initializing or configuring scheduler: {e}")
        return

    try:
        # create a subscription
        indices_list = symbols['indices']
        async with market_subscription_manager(session, indices_list, data_store) as market_sub:
            print("Streamer is running. Press Ctrl+C to stop.")

            # Start the scheduler
            scheduler.start()
            print("Daily task scheduled successfully")

            try:
                # Keep the main process running
                while True:
                    await asyncio.sleep(60)  # Check every minute instead of every second
            except KeyboardInterrupt:
                print("\nShutdown requested. Stopping scheduler...")
                scheduler.shutdown(wait=True)
                raise

    except KeyboardInterrupt:
        print("\nShutdown requested. Exiting Script...")


if __name__ == "__main__":
    load_dotenv()
    # send_pushover_notification("The TastyData container has started successfully!")

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Application interrupted")
