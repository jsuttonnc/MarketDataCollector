
import asyncio
import os
from contextlib import asynccontextmanager
from datetime import datetime

import yaml
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv

from src.data.market_data_store import MarketDataStore
# from src.messages.push_notifications import send_pushover_notification
from src.session.session_manager import create_session
from src.subscription.equity_metrics import EquityMetrics
from src.subscription.market_data_subscription import MarketDataSubscription


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
        # Cleanup - Fixed to prevent recursive cancellation
        print("Cleaning up resources...")
        if market_sub:
            try:
                # Ensure we don't create recursive task cancellation
                if hasattr(market_sub, 'stop'):
                    if asyncio.iscoroutinefunction(market_sub.stop):
                        await asyncio.wait_for(market_sub.stop(), timeout=5.0)
                    else:
                        market_sub.stop()
            except asyncio.TimeoutError:
                print("Warning: Market subscription stop timed out")
            except Exception as e:
                print(f"Error stopping streamer: {e}")

        if data_store and hasattr(data_store, 'disconnect'):
            try:
                data_store.disconnect()
            except Exception as e:
                print(f"Error disconnecting database: {e}")


async def main():
    # Create a shutdown event
    shutdown_event = asyncio.Event()
    scheduler = None

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
            CronTrigger(hour=17, minute=24, second=0),  # Run at 3 AM daily
            id='daily_task',
            name='Daily Maintenance Task',
            max_instances=1  # Prevent overlapping executions
        )

        # create a subscription
        indices_list = streaming_symbols['indices']
        async with market_subscription_manager(session, indices_list, data_store) as market_sub:
            print("Streamer is running. Press Ctrl+C to stop.")

            # Start the scheduler
            scheduler.start()
            print("Daily task scheduled successfully")

            # Wait for the shutdown signal (Ctrl+C will naturally raise KeyboardInterrupt)
            try:
                await shutdown_event.wait()
            except KeyboardInterrupt:
                print("\nKeyboard interrupt received in wait loop")
                raise

    except KeyboardInterrupt:
        print("\nShutdown requested. Exiting Script...")
    except Exception as e:
        print(f"Error in main execution: {e}")
    finally:
        # Proper cleanup of scheduler
        if scheduler and scheduler.running:
            print("Shutting down scheduler...")
            try:
                scheduler.shutdown(wait=False)  # Don't wait to prevent deadlock
            except Exception as e:
                print(f"Error shutting down scheduler: {e}")


if __name__ == "__main__":
    load_dotenv()
    # send_pushover_notification("The TastyData container has started successfully!")

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nApplication interrupted gracefully")
    except Exception as e:
        print(f"Application error: {e}")
    finally:
        print("Application shutdown complete")