"""
scheduler/main_scheduler.py

Sets up APScheduler and registers all jobs.

Daily fixed jobs:
  - 00:01  midnight_job          — clear stale card, pre-fetch tomorrow
  - 11:00  morning_briefing_job  — fetch card, analyse, send 12pm summary
  ## no longer used - 09:30  bet365_job            — send Bet365 daily action plan
  - dynamic end_of_day_job       — 90 mins after last race

Per-race dynamic jobs (registered after morning briefing):
  - T-5min  pre_race_job(race_id)
  - shared   result_poller — every 60s, checks all unsettled races

Startup logic:
  - If today's card already exists on disk (bot restarted mid-day):
    load it and register jobs for remaining races
  - If no card: run morning_briefing_job immediately
"""

import os
import json
import logging
from datetime import datetime, timedelta, date

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron         import CronTrigger
from apscheduler.triggers.date         import DateTrigger
from apscheduler.triggers.interval     import IntervalTrigger

import config
from scheduler.daily_jobs import (
    midnight_job,
    morning_briefing_job,
    fetch_today_card,
    run_bet365_daily,
    end_of_day_job,
    get_today_analysed,
    _load_today_analysed,
    _hydrate_today_analysed,
)
from scheduler.race_jobs import pre_race_job, poll_all_results
from utils.helpers       import parse_off_time

logger = logging.getLogger(__name__)

_scheduler: BackgroundScheduler | None = None


def build_scheduler() -> BackgroundScheduler:
    """
    Create and configure the scheduler.
    Call scheduler.start() in main.py after this returns.
    """
    global _scheduler
    scheduler  = BackgroundScheduler(timezone="Europe/London")
    _scheduler = scheduler

    # ── Fixed daily jobs ──────────────────────────────────────────────────────

    scheduler.add_job(
        midnight_job,
        CronTrigger(hour=4, minute=45),
        id="midnight",
        replace_existing=True,
    )

    # Fetch card + send morning briefing at MORNING_BRIEFING_TIME
    h, m = _parse_time(config.MORNING_BRIEFING_TIME)
    scheduler.add_job(
        lambda: morning_briefing_job(scheduler),
        CronTrigger(hour=h, minute=m),
        id="morning_briefing",
        replace_existing=True,
    )

    ## Bet365 daily plan at 09:30
    #scheduler.add_job(
    #    run_bet365_daily,
    #    CronTrigger(hour=9, minute=30),
    #    id="bet365_daily",
    #    replace_existing=True,
    #)

    #logger.info(
    #    f"Scheduler: fixed jobs registered "
    #    f"(briefing at {config.MORNING_BRIEFING_TIME})"
    #)

    return scheduler


def register_race_jobs(scheduler: BackgroundScheduler,
                       analysed_races: list[dict]):
    """
    Register pre-race alert jobs for every race in today's card.
    A single shared result poller handles ALL result checking.
    """
    now      = datetime.now()
    today    = date.today().strftime("%Y-%m-%d")
    last_off = None

    for race in analysed_races:
        race_id = race.get("race_id", "")

        off_dt_str = race.get("off_dt", "")
        if off_dt_str and "T" in off_dt_str:
            try:
                from datetime import timezone
                off_dt = datetime.fromisoformat(off_dt_str)
                if off_dt.tzinfo is None:
                    off_dt = off_dt.replace(tzinfo=timezone.utc)
                off_dt = off_dt.astimezone().replace(tzinfo=None)
            except (ValueError, AttributeError):
                off_dt = parse_off_time(race.get("off", ""), today)
        else:
            off_dt = parse_off_time(race.get("off", ""), today)

        if not off_dt or not race_id:
            continue

        if last_off is None or off_dt > last_off:
            last_off = off_dt

        pre_race_time = off_dt - timedelta(minutes=10)

        if pre_race_time > now:
            scheduler.add_job(
                lambda rid=race_id: pre_race_job(rid),
                DateTrigger(run_date=pre_race_time),
                id=f"pre_{race_id}",
                replace_existing=True,
            )
        elif off_dt > now:
            # Pre-race window passed but race hasn't started — send now
            scheduler.add_job(
                lambda rid=race_id: pre_race_job(rid),
                DateTrigger(run_date=now + timedelta(seconds=5)),
                id=f"pre_{race_id}",
                replace_existing=True,
            )

    # ── Auto-refresh 20 mins before first race ─────────────────────────────────
    first_off = min(
        (off_dt for race in analysed_races
         for off_dt in [_parse_race_off(race, today)]
         if off_dt),
        default=None
    )
    if first_off:
        refresh_time = first_off - timedelta(minutes=20)
        if refresh_time > now:
            scheduler.add_job(
                lambda: morning_briefing_job(scheduler),
                DateTrigger(run_date=refresh_time),
                id="auto_refresh",
                replace_existing=True,
            )
            logger.info(
                f"Scheduler: auto-refresh scheduled for "
                f"{refresh_time.strftime('%H:%M')} (20 mins before first race)"
            )

    # ── Single shared result poller ────────────────────────────────────────────
    scheduler.add_job(
        poll_all_results,
        IntervalTrigger(seconds=config.RESULT_POLL_INTERVAL_S),
        id="result_poller",
        replace_existing=True,
    )
    logger.info("Scheduler: shared result poller registered (every 60s)")

    # ── End of day job ────────────────────────────────────────────────────────
    if last_off:
        eod_time = last_off + timedelta(minutes=90)
        if eod_time > now:
            scheduler.add_job(
                end_of_day_job,
                DateTrigger(run_date=eod_time),
                id="end_of_day",
                replace_existing=True,
            )
            logger.info(
                f"Scheduler: end_of_day scheduled for "
                f"{eod_time.strftime('%H:%M')}"
            )

    logger.info(
        f"Scheduler: registered jobs for {len(analysed_races)} races"
    )


def startup_catchup(scheduler: BackgroundScheduler):
    """
    Called on bot startup (or restart) at any time of day.

    1. Check if today's card already exists on disk (briefing ran earlier)
    2. If yes — load into memory, register remaining race jobs immediately
    3. If no  — kick off morning_briefing_job in a background thread so the
                command listener starts immediately instead of waiting 20+ min
                for the API enrichment to complete
    """
    logger.info("Scheduler: startup catchup")

    races = get_today_analysed()
    if not races:
        races = _load_today_analysed()

    if races:
        # Card found on disk — hydrate in-memory state and register jobs immediately
        _hydrate_today_analysed(races)
        logger.info(
            f"Scheduler: today's card found — "
            f"{len(races)} races, registering jobs for remaining"
        )

        # Reset streak tracker so the result replay rebuilds it from scratch.
        # The result poller will re-process all past races in order, which means
        # streak_update() will be called for each and the state will be correct
        # by the time live races start.
        try:
            from notifications.streak_tracker import reset_streaks
            reset_streaks()
            logger.info("Scheduler: streak tracker reset for mid-day replay")
        except Exception as e:
            logger.warning(f"Scheduler: streak reset failed: {e}")

        register_race_jobs(scheduler, races)
        # Send briefing now (bot restarted mid-day)
        from notifications.telegram import send_main
        from notifications.formatter import format_morning_briefing
        from scheduler.daily_jobs   import _load_previous_summary
        prev  = _load_previous_summary()
        msg   = format_morning_briefing(analysed_races=races, previous_summary=prev)
        send_main(msg)
    else:
        logger.info(
            "Scheduler: no card found — fetching in background "
            "(bot is live now, briefing will arrive when fetch completes)"
        )
        from notifications.telegram import send_main as _send
        _send(
            f"⏳ <b>Collecting today's race data...</b>\n"
            f"Fetching card for {', '.join(config.TARGET_REGIONS).upper()} — "
            f"briefing will follow shortly."
        )

        import threading
        def _fetch_and_brief():
            try:
                from notifications.telegram import send_main
                n = fetch_today_card(scheduler=scheduler, notify=True)
                if n > 0:
                    from scheduler.daily_jobs import (
                        get_today_analysed, _load_previous_summary
                    )
                    from notifications.formatter import format_morning_briefing
                    races = get_today_analysed()
                    send_main(f"✅ <b>{n} races loaded</b> — sending briefing now...")
                    prev  = _load_previous_summary()
                    msg   = format_morning_briefing(
                        analysed_races   = races,
                        previous_summary = prev,
                    )
                    send_main(msg)
                else:
                    logger.warning("startup_catchup: 0 races after fetch")
            except Exception as e:
                logger.error(f"startup_catchup background fetch failed: {e}", exc_info=True)
                from notifications.telegram import send_main
                send_main(f"❌ Startup fetch failed: {e}")

        t = threading.Thread(target=_fetch_and_brief, daemon=True, name="StartupFetch")
        t.start()
        logger.info("Scheduler: background fetch started")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_time(time_str: str) -> tuple[int, int]:
    """Parse 'HH:MM' string into (hour, minute) ints."""
    try:
        parts = time_str.strip().split(":")
        return int(parts[0]), int(parts[1])
    except (ValueError, IndexError):
        logger.warning(f"Could not parse time '{time_str}' — defaulting to 12:00")
        return 12, 0


def _parse_race_off(race: dict, today: str) -> datetime | None:
    """Parse a race's off_dt or off string into a naive local datetime."""
    off_dt_str = race.get("off_dt", "")
    if off_dt_str and "T" in off_dt_str:
        try:
            from datetime import timezone
            dt = datetime.fromisoformat(off_dt_str)
            if dt.tzinfo:
                dt = dt.astimezone()
            return dt.replace(tzinfo=None)
        except (ValueError, AttributeError):
            pass
    return parse_off_time(race.get("off", ""), today)
