"""
config.example.py — horse_bets_v3

Copy this file to config.py and fill in your credentials.
Never commit config.py.
"""

# ── Racing API ────────────────────────────────────────────────────────────────
# Sign up at https://www.theracingapi.com
RACING_API_USERNAME = "your_racing_api_username"
RACING_API_PASSWORD = "your_racing_api_password"
RACING_API_BASE_URL = "https://api.theracingapi.com/v1"

# ── Telegram ──────────────────────────────────────────────────────────────────
# Create bots via @BotFather and get your chat ID via @userinfobot
TELEGRAM_CHAT_ID = "your_telegram_chat_id"

# Main alert bot (race alerts, morning briefing)
TELEGRAM_BOT_TOKEN = "your_main_bot_token"

# Betfair bot (bet placement, settlement, daily summary)
BETFAIR_TELEGRAM_BOT_TOKEN = "your_betfair_bot_token"

# Bet365 / manual daily analysis bot
BET365_TELEGRAM_BOT_TOKEN = "your_bet365_bot_token"

# Results bot (race result notifications)
RESULTS_TELEGRAM_BOT_TOKEN = "your_results_bot_token"

# ── Betfair API ───────────────────────────────────────────────────────────────
# Requires a Betfair Exchange account with API access enabled
# Generate an API key at https://developer.betfair.com
# SSL certificates must be generated and placed in the certs/ directory
# See: https://docs.developer.betfair.com/display/1smk3cen4v3lu3yomq5qye0ni/Non-Interactive+%28bot%29+login
BETFAIR_USERNAME = "your_betfair_username"
BETFAIR_PASSWORD = "your_betfair_password"
BETFAIR_APP_KEY  = "your_betfair_app_key"

# Absolute paths to your Betfair SSL certificate directory
# The directory should contain client-2048.crt and client-2048.key
BETFAIR_CERTS_DIR_SERVER = "/path/to/certs/on/server"
BETFAIR_CERTS_DIR_MAC    = "/path/to/certs/on/mac"

# ── Bot timing ────────────────────────────────────────────────────────────────
MORNING_BRIEFING_TIME  = "12:00"   # when to send the daily summary (24h HH:MM)
RESULT_POLL_INTERVAL_S = 60        # seconds between result polls

# ── Regions ───────────────────────────────────────────────────────────────────
TARGET_REGIONS = ["gb", "ire"]

# ── Data directories ──────────────────────────────────────────────────────────
import os as _os

DIR_RAW        = "data/raw"
DIR_SIMULATION = "data/simulation"
DIR_LOGS       = "data/logs"
DIR_CARDS      = _os.path.join(_os.path.dirname(__file__), "data", "cards")
DIR_HISTORY    = _os.path.join(_os.path.dirname(__file__), "data", "history")
DIR_RESULTS    = _os.path.join(_os.path.dirname(__file__), "data", "results")
