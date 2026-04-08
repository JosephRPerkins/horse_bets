"""
betfair/notify.py
Send messages via the Betfair Telegram bot (BETFAIR_TELEGRAM_BOT_TOKEN).
Supports muting — when muted, all sends are silently dropped.
"""

import logging
import requests
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import config

logger = logging.getLogger("betfair.notify")

CHUNK_SIZE = 4000
_muted     = False   # set via set_muted() on startup and /mute /unmute


def set_muted(muted: bool):
    global _muted
    _muted = muted


def is_muted() -> bool:
    return _muted


def send(text: str) -> bool:
    if _muted:
        logger.debug(f"[MUTED] {text[:80]}")
        return True

    token   = config.BETFAIR_TELEGRAM_BOT_TOKEN
    chat_id = config.TELEGRAM_CHAT_ID
    if not token or not chat_id:
        logger.warning("Betfair bot token or chat_id not set")
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
        if not r.ok:
            logger.warning(f"Telegram send failed: {r.status_code} {r.text[:200]}")
        return r.ok
    except Exception as e:
        logger.error(f"Telegram send error: {e}")
        return False


def send_chunks(text: str):
    if len(text) <= CHUNK_SIZE:
        send(text)
        return
    lines  = text.split("\n")
    chunk  = []
    length = 0
    for line in lines:
        if length + len(line) + 1 > CHUNK_SIZE:
            send("\n".join(chunk))
            chunk  = [line]
            length = len(line)
        else:
            chunk.append(line)
            length += len(line) + 1
    if chunk:
        send("\n".join(chunk))
