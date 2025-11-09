import threading
import time
from dotenv import load_dotenv
from tastytrade.market_data import get_market_data_by_type
from tastytrade.metrics import get_market_metrics
from src.session.session_manager import validate_session

class MarketDataManager:
    def __init__(self, session, update_interval=60):
        """
        Initialize the MarketDataManager.

        Args:
            session: OAuthSession instance for Tastytrade API.
            update_interval: Update interval in seconds (default: 60).
        """
        load_dotenv(override=True)
        self.session = session
        self.update_interval = update_interval
        self.running = False
        self.thread = None
        self._stop_event = threading.Event()

    def gather_market_data(self, symbols):
        """Fetch market data from Tastytrade API."""
        try:
            validate_session(self.session)
            data = get_market_data_by_type(
                self.session,
                indices=symbols.get('indices'),
                cryptocurrencies=symbols.get('cryptocurrencies'),
                equities=symbols.get('equities'),
                futures=symbols.get('futures'),
                future_options=symbols.get('future_options'),
                options=symbols.get('options'),
            )
            print(f"Market data updated at {time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(data)

            # get metric data
            m_data = get_market_metrics(self.session, symbols.get('indices'))
            print(m_data)
            return data

        except Exception as e:
            print(f"Error fetching market data: {e}")
            return None

    def _run_loop(self, symbols):
        """Internal method to run the market data fetching loop."""
        while not self._stop_event.is_set():
            try:
                self.gather_market_data(symbols)
            except Exception as e:
                print(f"Error in market data loop: {e}")

            # Wait for the specified interval or until stop is requested
            self._stop_event.wait(self.update_interval)

    def start(self, symbols):
        """
        Start the market data manager in a separate thread.

        Args:
            symbols: Dictionary containing symbol lists by category.
        """
        if self.running:
            print("MarketDataManager is already running")
            return

        self.running = True
        self._stop_event.clear()
        self.thread = threading.Thread(target=self._run_loop, args=(symbols,))
        self.thread.daemon = True  # Dies when main thread dies
        self.thread.start()
        print(f"MarketDataManager started with {self.update_interval}s update interval")

    def stop(self):
        """Stop the market data manager."""
        if not self.running:
            print("MarketDataManager is not running")
            return

        self.running = False
        self._stop_event.set()

        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5)  # Wait up to 5 seconds for thread to finish

        print("MarketDataManager stopped")

    def is_running(self):
        """Check if the market data manager is currently running."""
        return self.running and self.thread and self.thread.is_alive()

    def set_update_interval(self, interval):
        """Update the update interval (takes effect on the next restart)."""
        self.update_interval = interval
        print(f"Update interval set to {interval} seconds")