"""
main.py

Entry point for the horse racing alert bot.

Startup sequence:
  1. Configure logging
  2. Validate config (API credentials, tokens)
  3. Build the APScheduler with fixed daily jobs
  4. Run startup_catchup to register race jobs for today's card
  5. Start scheduler
  6. Send startup notification
  7. Start command listener (handles Telegram bot commands)
  8. Keep process alive

Usage:
    python main.py

Background:
    nohup python main.py >> logs/bot.log 2>&1 &

Stop:
    kill $(pgrep -f "python main.py")
    or send /quit via Telegram (not yet implemented — use kill)
"""

import os
import sys
import time
import logging
from datetime import datetime

# ── Logging setup ─────────────────────────────────────────────────────────────

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            f"logs/bot_{datetime.now().strftime('%Y%m%d')}.log"
        ),
    ],
)

# Suppress APScheduler routine noise
logging.getLogger("apscheduler.scheduler").setLevel(logging.WARNING)
logging.getLogger("apscheduler.executors.default").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

# ── Imports ───────────────────────────────────────────────────────────────────

import config
from scheduler.main_scheduler   import build_scheduler, startup_catchup
from scheduler.command_listener import CommandListener
from notifications.telegram     import send_main
from scheduler.daily_jobs       import get_today_analysed


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 50)
    logger.info("Racing bot v3 starting up")
    logger.info("=" * 50)

    # Validate config
    if not config.TELEGRAM_BOT_TOKEN or not config.TELEGRAM_CHAT_ID:
        logger.error("TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set in config.py")
        sys.exit(1)

    if not config.RACING_API_USERNAME or not config.RACING_API_PASSWORD:
        logger.error("Racing API credentials not set in config.py")
        sys.exit(1)

    if not config.RESULTS_TELEGRAM_BOT_TOKEN:
        logger.warning("RESULTS_TELEGRAM_BOT_TOKEN not set — results will use main bot")

    # Build scheduler
    scheduler = build_scheduler()

    # Catchup — register race jobs for today
    startup_catchup(scheduler)

    # Start scheduler
    scheduler.start()
    logger.info("Scheduler started")

    # Send startup notification (startup_catchup handles data notifications)
    send_main(
        f"🤖 <b>Racing bot v3 online</b>\n"
        f"Send /help for available commands"
    )

    # Start command listener
    listener = CommandListener()
    listener.start(
        get_analysed_fn=get_today_analysed,
    )
    logger.info("Command listener started")

    # Keep alive
    logger.info("Bot running — press Ctrl+C to stop")
    try:
        while True:
            time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down")
        listener.stop()
        scheduler.shutdown()
        send_main("🤖 Racing bot v3 offline")
        logger.info("Bot stopped")


if __name__ == "__main__":
    main()
