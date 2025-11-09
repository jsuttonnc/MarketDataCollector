from dotenv import load_dotenv
import os
from tastytrade import OAuthSession
from tastytrade.utils import now_in_new_york


def validate_session(session):
    """Establish connection to Tastytrade and create a subscription."""
    try:
        if now_in_new_york() > session.session_expiration:
            session.refresh()
    except Exception as e:
        print(f"Error refreshing session: {e}")
        raise


def create_session() -> OAuthSession:
    """Establish connection to Tastytrade and create a subscription."""
    try:
        print("Connecting to Tastytrade...")
        load_dotenv(override=True)
        clientSecret = os.getenv("TT_OAUTH_CLIENT_SECRET", "")
        refreshToken = os.getenv("TT_OAUTH_REFRESH_TOKEN", "")

        if not clientSecret or not refreshToken:
            raise ValueError("Missing TT_OAUTH_CLIENT_SECRET or TT_OAUTH_REFRESH_TOKEN environment variables")

        session = OAuthSession(clientSecret, refreshToken)
        return session

    except Exception as e:
        print(f"Error creating session: {e}")
        raise