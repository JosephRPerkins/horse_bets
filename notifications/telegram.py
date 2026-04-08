"""
notifications/telegram.py

Single responsibility: send messages to Telegram.
Supports multiple bots (main alerts vs results) with per-bot mute state.
Nothing else in the system calls the Telegram API directly.

Mute state:
  - "main"    → TELEGRAM_BOT_TOKEN (pre-race alerts, morning briefing)
  - "results" → RESULTS_TELEGRAM_BOT_TOKEN (race results)

Commands /mute, /unmute, /muteres, /unmuteres control mute state.
"""

import logging
import requests

import config

logger = logging.getLogger(__name__)

TELEGRAM_BASE = "https://api.telegram.org"

# ── Mute state ────────────────────────────────────────────────────────────────

_mute: dict[str, bool] = {
    "main":    False,
    "results": False,
}


def set_mute(bot: str, muted: bool) -> None:
    """Set mute state for a bot. bot is 'main' or 'results'."""
    if bot in _mute:
        _mute[bot] = muted
        logger.info(f"Telegram: {bot} bot {'muted' if muted else 'unmuted'}")


def is_muted(bot: str) -> bool:
    return _mute.get(bot, False)


# ── Core send ─────────────────────────────────────────────────────────────────

def send(message: str,
         token: str = None,
         chat_id: str = None,
         parse_mode: str = "HTML") -> bool:
    """
    Send a message using any bot token.

    Args:
        message:    The text to send
        token:      Bot token. Defaults to TELEGRAM_BOT_TOKEN.
        chat_id:    Override the default chat ID from config
        parse_mode: 'HTML' (default) or 'Markdown'
    """
    tok = token or config.TELEGRAM_BOT_TOKEN
    cid = chat_id or config.TELEGRAM_CHAT_ID
    url = f"{TELEGRAM_BASE}/bot{tok}/sendMessage"

    payload = {
        "chat_id":    cid,
        "text":       message,
        "parse_mode": parse_mode,
    }

    try:
        r = requests.post(url, json=payload, timeout=10)
        if not r.ok:
            logger.error(f"Telegram send failed: {r.status_code} — {r.text[:200]}")
            return False
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"Telegram request error: {e}")
        return False


def send_chunks(message: str,
                token: str = None,
                chat_id: str = None,
                parse_mode: str = "HTML") -> bool:
    """
    Send a long message in chunks if it exceeds Telegram's 4096 char limit.
    Splits cleanly on newlines to avoid breaking mid-line.
    """
    MAX = 4000
    if len(message) <= MAX:
        return send(message, token, chat_id, parse_mode)

    lines = message.split("\n")
    chunk = ""
    ok    = True

    for line in lines:
        if len(chunk) + len(line) + 1 > MAX:
            if chunk:
                ok = send(chunk.strip(), token, chat_id, parse_mode) and ok
            chunk = line + "\n"
        else:
            chunk += line + "\n"

    if chunk.strip():
        ok = send(chunk.strip(), token, chat_id, parse_mode) and ok

    return ok


# ── Bot-specific senders ──────────────────────────────────────────────────────

def send_main(message: str, parse_mode: str = "HTML") -> bool:
    """Send via the main alert bot. Respects mute state."""
    if _mute.get("main"):
        logger.debug("Main bot muted — message suppressed")
        return True
    return send_chunks(message, token=config.TELEGRAM_BOT_TOKEN, parse_mode=parse_mode)


def send_results(message: str, parse_mode: str = "HTML") -> bool:
    """Send via the results bot. Respects mute state."""
    if _mute.get("results"):
        logger.debug("Results bot muted — message suppressed")
        return True
    return send_chunks(message, token=config.RESULTS_TELEGRAM_BOT_TOKEN, parse_mode=parse_mode)


def send_bet365(message: str, parse_mode: str = "HTML") -> bool:
    """Send via the Bet365 daily analysis bot."""
    return send_chunks(message, token=config.BET365_TELEGRAM_BOT_TOKEN, parse_mode=parse_mode)


def send_betfair(message: str, parse_mode: str = "HTML") -> bool:
    """Send via the Betfair bot."""
    return send_chunks(message, token=config.BETFAIR_TELEGRAM_BOT_TOKEN, parse_mode=parse_mode)


def get_updates(token: str, offset: int, timeout: int = 3) -> list:
    """
    Poll getUpdates for a bot. Used by CommandListener.
    Returns list of update dicts.
    """
    url = f"{TELEGRAM_BASE}/bot{token}/getUpdates"
    try:
        r = requests.get(
            url,
            params={"offset": offset + 1, "timeout": timeout},
            timeout=timeout + 5,
        )
        if not r.ok:
            return []
        return r.json().get("result", [])
    except requests.exceptions.RequestException:
        return []
