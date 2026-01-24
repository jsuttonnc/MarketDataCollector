from typing import List, Set
from tastytrade.watchlists import PublicWatchlist


class WatchListManager:
    def __init__(self, session, data_store):
        """
        Handle watch lists

        Args:
        """
        self.data_store = data_store
        self.session = session

    async def load_watch_list_data(self):
        try:
            public_watchlist = PublicWatchlist.get(self.session)

            val = self.extract_equity_symbols_from_watchlists(public_watchlist)
            # val = self.extract_equity_symbols_detailed(public_watchlist)
            print(sorted(val))
        except Exception as e:
            print(f"Error loading watch lists: {e}")
            return


    def extract_equity_symbols_from_watchlists(self, watchlists: List[PublicWatchlist]) -> Set[str]:
        """
        Recursively extract equity symbols from tastytrade PublicWatchlist objects.

        Args:
            watchlists: List of PublicWatchlist objects

        Returns:
            Set of unique equity symbols
        """
        equity_symbols = set()

        for watchlist in watchlists:
            # Skip excluded group names
            # group_name = watchlist.group_name if hasattr(watchlist, 'group_name') else None
            # if group_name and group_name.lower() in ['indicators', 'market indices', 'crypto','futures']:
            #     continue

            # Access watchlist_entries attribute
            for watchlist in watchlists:
                # Access watchlist_entries attribute
                if hasattr(watchlist, 'watchlist_entries') and watchlist.watchlist_entries:
                    for entry in watchlist.watchlist_entries:
                        # Check if entry is equity type
                        if entry.get('instrument-type') == 'Equity':
                            if (symbol := entry.get('symbol')) and not symbol.startswith("$"):
                                equity_symbols.add(symbol)

        return equity_symbols


    # Alternative version with more details
    def extract_equity_symbols_detailed(self, watchlists: List[PublicWatchlist]) -> dict:
        """
        Extract equity symbols with additional metadata.

        Args:
            watchlists: List of PublicWatchlist objects

        Returns:
            Dictionary with symbols and source watchlist names
        """
        equity_data = {}

        for watchlist in watchlists:
            # Skip excluded group names
            group_name = watchlist.group_name if hasattr(watchlist, 'group_name') else None
            if group_name and group_name.lower() in ['indicators', 'test']:
                continue

            watchlist_name = watchlist.name if hasattr(watchlist, 'name') else 'Unknown'

            if hasattr(watchlist, 'watchlist_entries') and watchlist.watchlist_entries:
                for entry in watchlist.watchlist_entries:
                    if entry.get('instrument-type') == 'Equity':
                        symbol = entry.get('symbol')
                        if symbol:
                            if symbol not in equity_data:
                                equity_data[symbol] = {
                                    'symbol': symbol,
                                    'watchlists': []
                                }
                            equity_data[symbol]['watchlists'].append(watchlist_name)

        return equity_data

