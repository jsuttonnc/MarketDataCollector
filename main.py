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

def load_symbols(source_file):
    """Load symbols from the source_file file."""
    symbols_file = os.path.join(os.path.dirname(__file__), source_file)
    try:
        with open(source_file, 'r') as f:
            symbols_data = yaml.safe_load(f)
            return symbols_data
    except FileNotFoundError:
        raise FileNotFoundError(f"Symbols file not found: {source_file}")
    except yaml.YAMLError as e:
        raise ValueError(f"Invalid YAML in symbols file: {source_file}. Error: {e}")


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


async def main():
    try:
        # load streaming symbols
        streaming_symbols = load_symbols('streaming-symbols.yaml')

        # load nightly symbols
        nightly_symbols = load_symbols('nightly-symbols.yaml')

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

    # Create a closure that captures the variables you need
    async def daily_task():
        """Task that runs once daily with access to session, data_store, and symbols"""
        print(f"Running daily task at {datetime.now()}")
        try:
            # Custom throttling (25 symbols per batch, 0.5s between calls, 2s between batches)
            eq_metrics = EquityMetrics(session, data_store)
            eq_metrics.gather_metrics(
                nightly_symbols['equities'],
                symbols_per_batch=25,
                delay_between_calls=0.5,
                delay_between_batches=2.0
            )

            # Add your other daily task logic here
            print("Daily maintenance task completed successfully")

        except Exception as ex:
            print(f"Error in daily task: {ex}")


    try:
        # Initialize scheduler
        scheduler = AsyncIOScheduler()

        # Schedule the task using cron syntax
        scheduler.add_job(
            daily_task,
            CronTrigger(hour=11, minute=42, second=0),  # Run at 3:00:00 AM daily
            id='daily_task',
            name='Daily Maintenance Task',
            max_instances=1  # Prevent overlapping executions
        )
    except Exception as e:
        print(f"Error initializing or configuring scheduler: {e}")
        return

    try:
        # create a subscription
        indices_list = streaming_symbols['indices']
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
