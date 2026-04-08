"""
scheduler/command_listener.py

Polls both the main bot and results bot for Telegram commands.
Runs as a background thread alongside the main scheduler.

Main bot commands (/help for full list):
  /briefing   — send today's morning briefing now
  /races      — list all today's races with tier badges in time order
  /next       — show the next upcoming race
  /status     — show race count and tier breakdown
  /results    — show confirmed results today
  /refresh    — re-fetch today's card (updates verdicts and odds)
  /bet365     — trigger Bet365 daily analysis now
  /mute       — silence main bot pre-race alerts and briefings
  /unmute     — re-enable main bot notifications
  /help       — list available commands

Results bot commands:
  /muteres    — silence results notifications
  /unmuteres  — re-enable results notifications

Both bots share the same chat ID. The listener polls both tokens.
"""

import logging
import time
import threading
from datetime import datetime, date

import config
from notifications.telegram  import (
    send, send_main, send_results, get_updates,
    set_mute, is_muted,
)
from utils.helpers import parse_off_time

logger = logging.getLogger(__name__)

POLL_INTERVAL = 5  # seconds between getUpdates calls

DIVIDER = "─" * 30
INFO    = "ℹ️"
RACE    = "🏇"
CLOCK   = "⏰"
WIN     = "✅"
LOSS    = "❌"
FIRE    = "🔥"


class CommandListener:

    def __init__(self):
        self._last_main    = 0
        self._last_results = 0
        self._stop_event   = threading.Event()
        self._thread       = None
        self._get_analysed = None
        self._get_results  = None

    def start(self, get_analysed_fn=None, get_results_fn=None):
        """Start the listener in a background thread."""
        self._get_analysed = get_analysed_fn
        self._get_results  = get_results_fn
        self._thread       = threading.Thread(
            target=self._run, daemon=True, name="CommandListener"
        )
        self._thread.start()
        logger.info("CommandListener started")

    def stop(self):
        self._stop_event.set()

    def _run(self):
        logger.info("CommandListener: polling for commands")
        while not self._stop_event.is_set():
            try:
                self._poll_bot(config.TELEGRAM_BOT_TOKEN,
                               "_last_main", "main")
                self._poll_bot(config.RESULTS_TELEGRAM_BOT_TOKEN,
                               "_last_results", "results")
            except Exception as e:
                logger.error(f"CommandListener error: {e}")
            time.sleep(POLL_INTERVAL)

    def _poll_bot(self, token: str, last_attr: str, bot_name: str):
        offset = getattr(self, last_attr, 0)
        updates = get_updates(token, offset)
        for update in updates:
            setattr(self, last_attr, update["update_id"])
            msg = update.get("message") or update.get("edited_message")
            if not msg:
                continue
            text    = (msg.get("text") or "").strip().lower()
            chat_id = str(msg["chat"]["id"])
            if chat_id != str(config.TELEGRAM_CHAT_ID):
                continue
            self._handle(text, chat_id, bot_name, token)

    def _handle(self, text: str, chat_id: str, bot_name: str, token: str):
        logger.info(f"CommandListener [{bot_name}]: received '{text}'")

        # Commands handled by whichever bot received them
        if text.startswith("/briefing"):
            self._cmd_briefing(chat_id, token)
        elif text.startswith("/races"):
            self._cmd_races(chat_id, token)
        elif text.startswith("/next"):
            self._cmd_next(chat_id, token)
        elif text.startswith("/status"):
            self._cmd_status(chat_id, token)
        elif text.startswith("/results"):
            self._cmd_results(chat_id, token)
        elif text.startswith("/refresh"):
            self._cmd_refresh(chat_id, token)
        elif text.startswith("/bet365"):
            self._cmd_bet365(chat_id, token)
        elif text.startswith("/mute") and not text.startswith("/muteres"):
            self._cmd_mute(chat_id, token, True)
        elif text.startswith("/unmute") and not text.startswith("/unmuteres"):
            self._cmd_mute(chat_id, token, False)
        elif text.startswith("/muteres"):
            self._cmd_mute_results(chat_id, token, True)
        elif text.startswith("/unmuteres"):
            self._cmd_mute_results(chat_id, token, False)
        elif text.startswith("/help"):
            self._cmd_help(chat_id, token)
        elif text.startswith("/"):
            send(f"{INFO} Unknown command. Send /help for the list.",
                 token=token, chat_id=chat_id)

    # ── Commands ──────────────────────────────────────────────────────────────

    def _cmd_briefing(self, chat_id: str, token: str):
        """Send today's full morning briefing right now."""
        races = self._today_races()
        if not races:
            send(f"{INFO} No races loaded yet.", token=token, chat_id=chat_id)
            return
        from notifications.formatter import format_morning_briefing
        from notifications.telegram  import send_chunks
        msg = format_morning_briefing(analysed_races=races)
        send_chunks(msg, token=token, chat_id=chat_id)

    def _cmd_races(self, chat_id: str, token: str):
        """List all today's races with tier badges in time order."""
        races = self._today_races()
        if not races:
            send(f"{INFO} No races loaded yet.", token=token, chat_id=chat_id)
            return

        from predict_v2 import (
            TIER_SUPREME, TIER_STRONG, TIER_GOOD, TIER_STD, TIER_SKIP,
            TIER_LABELS,
        )
        tier_badges = {
            TIER_SUPREME: "🔥🔥🔥",
            TIER_STRONG:  "🔥🔥",
            TIER_GOOD:    "🔥",
            TIER_STD:     "·",
            TIER_SKIP:    "✗",
        }

        today = date.today().strftime("%Y-%m-%d")
        now   = datetime.now()
        lines = [f"{RACE} <b>Today's races</b>\n{DIVIDER}"]

        for r in sorted(races, key=lambda x: x.get("off", "99:99")):
            off_dt = parse_off_time(r.get("off", ""), today)
            gone   = off_dt and off_dt < now
            badge  = tier_badges.get(r.get("tier", TIER_STD), "·")
            a      = r.get("top1") or {}
            b      = r.get("top2") or {}
            status = "✓" if gone else CLOCK

            line = (
                f"\n{status} {badge} "
                f"<b>{r.get('off','?')} {r.get('course','?')}</b>\n"
                f"  {r.get('type','?')} {r.get('dist','?')} | "
                f"{r.get('going','?')} | {r.get('field_size',0)} runners"
            )
            if a:
                line += f"\n  ⭐ {a.get('horse','?')} ({a.get('sp','?')})"
            if b:
                line += f" + 🔵 {b.get('horse','?')} ({b.get('sp','?')})"
            lines.append(line)

        from notifications.telegram import send_chunks
        send_chunks("\n".join(lines), token=token, chat_id=chat_id)

    def _cmd_next(self, chat_id: str, token: str):
        """Show the next upcoming race."""
        races = self._today_races()
        if not races:
            send(f"{INFO} No races loaded yet.", token=token, chat_id=chat_id)
            return

        today    = date.today().strftime("%Y-%m-%d")
        now      = datetime.now()
        upcoming = [
            r for r in races
            if parse_off_time(r.get("off", ""), today) and
               parse_off_time(r.get("off", ""), today) > now
        ]
        upcoming.sort(key=lambda r: r.get("off", "99:99"))

        if not upcoming:
            send(f"{INFO} No more races today.", token=token, chat_id=chat_id)
            return

        r      = upcoming[0]
        a      = r.get("top1") or {}
        b      = r.get("top2") or {}
        off_dt = parse_off_time(r.get("off", ""), today)
        mins   = int((off_dt - now).total_seconds() / 60) if off_dt else "?"

        from predict_v2 import TIER_LABELS
        tier_label = TIER_LABELS.get(r.get("tier"), "· STANDARD")

        lines = [
            f"{CLOCK} <b>Next race — {r.get('off','?')} {r.get('course','?')}</b>",
            f"In {mins} minutes",
            f"{tier_label}",
            f"{r.get('type','?')} | {r.get('dist','?')} | "
            f"{r.get('going','?')} | {r.get('field_size',0)} runners",
        ]
        if a:
            lines.append(f"\n⭐ WIN: {a.get('horse','?')} ({a.get('sp','?')})")
        if b:
            lines.append(f"🔵 PLACE: {b.get('horse','?')} ({b.get('sp','?')})")
        std  = r.get("places", "?")
        cons = r.get("cons_places", "?")
        lines.append(f"\n📌 Top-{std} each | cons: Top-{cons}")

        send("\n".join(lines), token=token, chat_id=chat_id)

    def _cmd_status(self, chat_id: str, token: str):
        """Show today's race count and tier breakdown."""
        races = self._today_races()
        today = date.today().strftime("%Y-%m-%d")
        now   = datetime.now()

        from predict_v2 import TIER_SUPREME, TIER_STRONG, TIER_GOOD

        done   = sum(
            1 for r in races
            if parse_off_time(r.get("off", ""), today) and
               parse_off_time(r.get("off", ""), today) < now
        ) if races else 0
        total   = len(races)
        supreme = sum(1 for r in races if r.get("tier") == TIER_SUPREME)
        strong  = sum(1 for r in races if r.get("tier") == TIER_STRONG)
        good    = sum(1 for r in races if r.get("tier") == TIER_GOOD)
        muted   = "🔕 Main muted" if is_muted("main") else ""
        muted_r = "🔕 Results muted" if is_muted("results") else ""

        lines = [
            f"📊 <b>Today's status</b>",
            f"Races: {done}/{total} run",
            f"🔥🔥🔥 Supreme: {supreme}",
            f"🔥🔥 Strong:   {strong}",
            f"🔥 Good:      {good}",
        ]
        if muted:
            lines.append(muted)
        if muted_r:
            lines.append(muted_r)
        send("\n".join(lines), token=token, chat_id=chat_id)

    def _cmd_results(self, chat_id: str, token: str):
        """Show confirmed results so far today."""
        results = self._today_results()
        races   = self._today_races()

        if not results:
            send(f"{INFO} No results confirmed yet today.", token=token, chat_id=chat_id)
            return

        result_map = {r.get("race_id"): r for r in results} if isinstance(results, list) else results
        lines      = [f"{RACE} <b>Results so far today</b>\n{DIVIDER}"]
        wins = losses = 0

        for race in sorted(races, key=lambda x: x.get("off", "99:99")):
            rid = race.get("race_id", "")
            res = result_map.get(rid)
            if not res:
                continue
            cons_won = res.get("cons_win")
            icon     = WIN if cons_won else (LOSS if cons_won is False else "⏳")
            if cons_won:   wins += 1
            elif cons_won is False: losses += 1

            a     = race.get("top1") or {}
            b     = race.get("top2") or {}
            a_pos = res.get("a_pos", "?")
            b_pos = res.get("b_pos", "?")

            def pos_str(p):
                try: return f"{int(p)}"
                except: return str(p) if p else "?"

            lines.append(
                f"\n{icon} {race.get('off','?')} {race.get('course','?')}\n"
                f"  {a.get('horse','?')} → {pos_str(a_pos)} | "
                f"{b.get('horse','?')} → {pos_str(b_pos)}"
            )

        lines.append(f"\n{DIVIDER}\nRecord: {wins}W {losses}L")
        from notifications.telegram import send_chunks
        send_chunks("\n".join(lines), token=token, chat_id=chat_id)

    def _cmd_refresh(self, chat_id: str, token: str):
        """Re-fetch today's card and rebuild analysis."""
        send(f"{INFO} Refreshing today's card...", token=token, chat_id=chat_id)
        try:
            from scheduler.daily_jobs   import morning_briefing_job
            from scheduler.main_scheduler import _scheduler
            morning_briefing_job(scheduler=_scheduler)
            send("✅ Card refreshed — verdicts, odds and race jobs updated.",
                 token=token, chat_id=chat_id)
        except Exception as e:
            send(f"❌ Refresh failed: {e}", token=token, chat_id=chat_id)

    def _cmd_bet365(self, chat_id: str, token: str):
        """Trigger Bet365 daily analysis now."""
        send(f"{INFO} Generating Bet365 plan...", token=token, chat_id=chat_id)
        try:
            from scheduler.daily_jobs import run_bet365_daily
            run_bet365_daily()
            send("✅ Bet365 plan sent.", token=token, chat_id=chat_id)
        except Exception as e:
            send(f"❌ Bet365 plan failed: {e}", token=token, chat_id=chat_id)

    def _cmd_mute(self, chat_id: str, token: str, mute: bool):
        """Mute or unmute the main alert bot."""
        set_mute("main", mute)
        label = "🔕 Muted" if mute else "🔔 Unmuted"
        send(f"{label} — main bot alerts {'silenced' if mute else 're-enabled'}.\n"
             f"Use /unmute to re-enable, /muteres for results bot.",
             token=token, chat_id=chat_id)

    def _cmd_mute_results(self, chat_id: str, token: str, mute: bool):
        """Mute or unmute the results bot."""
        set_mute("results", mute)
        label = "🔕 Muted" if mute else "🔔 Unmuted"
        send(f"{label} — results bot {'silenced' if mute else 're-enabled'}.\n"
             f"Use /unmuteres to re-enable.",
             token=token, chat_id=chat_id)

    def _cmd_help(self, chat_id: str, token: str):
        mute_status  = "🔕 muted" if is_muted("main") else "🔔 active"
        res_status   = "🔕 muted" if is_muted("results") else "🔔 active"
        send(
            f"<b>Available commands</b>\n"
            f"{DIVIDER}\n"
            f"/briefing  — full morning briefing now\n"
            f"/races     — all today's races with tier badges\n"
            f"/next      — next upcoming race\n"
            f"/status    — race count and tier breakdown\n"
            f"/results   — confirmed results today\n"
            f"/refresh   — re-fetch today's card\n"
            f"/bet365    — trigger Bet365 daily plan\n"
            f"\n<b>Notifications</b>\n"
            f"/mute      — silence alerts ({mute_status})\n"
            f"/unmute    — re-enable alerts\n"
            f"/muteres   — silence results ({res_status})\n"
            f"/unmuteres — re-enable results\n"
            f"{DIVIDER}\n"
            f"/help      — this message",
            token=token, chat_id=chat_id,
        )

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _today_races(self) -> list[dict]:
        if self._get_analysed:
            return self._get_analysed() or []
        from scheduler.daily_jobs import get_today_analysed, _load_today_analysed
        return get_today_analysed() or _load_today_analysed() or []

    def _today_results(self):
        if self._get_results:
            return self._get_results()
        # Pull from race_jobs settled state
        try:
            from scheduler.race_jobs import _settled
            return list(_settled)
        except Exception:
            return []
