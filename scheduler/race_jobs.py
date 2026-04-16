"""
scheduler/race_jobs.py

Per-race jobs:

  pre_race_job(race_id)
      Fires 10 minutes before off.
      Re-fetches live card, filters non-runners, re-scores active runners,
      sends pre-race alert via main bot.
      Calls Betfair hook (stub until betfair bot implemented).

  poll_all_results (single shared job)
      Runs every 60 seconds from first race off + 10 mins.
      Checks ALL unsettled races in one loop per tick.
      Sends result notification via results bot when confirmed.
"""

import logging
from datetime import datetime, timedelta, date

import config
from core.api_client         import RacingAPIClient
from predict                 import score_runner, place_terms
from predict_v2              import race_confidence, conservative_place_terms, TIER_SKIP
from notifications.formatter import format_pre_race_alert, format_result
from notifications.telegram  import send_main, send_results
from scheduler.daily_jobs    import get_today_analysed, _derive_outcome
from utils.helpers           import is_non_runner, derive_surface, normalise_going
from betfair                 import on_pre_race, get_exchange_odds

logger = logging.getLogger(__name__)

# ── State ─────────────────────────────────────────────────────────────────────

_settled:    set[str]           = set()   # race_ids fully processed
_poll_start: dict[str, datetime] = {}    # when polling began per race
MAX_POLL_MINUTES = 90


# ── Pre-race job ───────────────────────────────────────────────────────────────

def pre_race_job(race_id: str):
    """
    Send the pre-race alert 10 minutes before off.
    Re-fetches the live card to get updated odds and non-runner flags.
    """
    logger.info(f"pre_race_job: {race_id}")

    # Find race in cached card
    race_data = _find_race(race_id)
    if not race_data:
        logger.warning(f"pre_race_job: {race_id} not in today's card")
        return

    # Re-fetch live card for this race to get updated odds and NR flags
    client = RacingAPIClient()
    live_card = None
    try:
        live_card = client.get_race_racecard(race_id)
    except Exception as e:
        logger.warning(f"pre_race_job: live re-fetch failed for {race_id}: {e}")

    if live_card:
        # Merge live runner data (odds, status) over cached race data
        live_runners = live_card.get("runners", [])
        if live_runners:
            # Build status map from live card
            live_map = {r.get("horse_id"): r for r in live_runners}
            updated_runners = []
            for r in race_data.get("all_runners", []):
                hid  = r.get("horse_id", "")
                live = live_map.get(hid, {})
                # Update odds and status from live card
                merged = {**r}
                if live.get("status"):
                    merged["status"] = live["status"]
                # Re-normalise odds from live card
                live_norm = RacingAPIClient.normalise_runner(live)
                if live_norm.get("sp_dec"):
                    merged["sp_dec"] = live_norm["sp_dec"]
                    merged["sp"]     = live_norm["sp"]
                if live_norm.get("bf_sp_dec"):
                    merged["bf_sp_dec"] = live_norm["bf_sp_dec"]
                    merged["bf_sp"]     = live_norm["bf_sp"]
                updated_runners.append(merged)
            race_data = {**race_data, "all_runners": updated_runners}
            # Update verdict if now available
            if live_card.get("verdict"):
                race_data = {**race_data, "verdict": live_card["verdict"]}
            if live_card.get("tip"):
                race_data = {**race_data, "tip": live_card["tip"]}

    # Filter out non-runners from active pool, re-score
    all_runners  = race_data.get("all_runners", [])
    active       = [r for r in all_runners if not is_non_runner(r)]

    if not active:
        logger.warning(f"pre_race_job: {race_id} — no active runners after NR filter")
        return

    # Re-score active runners
    rescored = []
    for r in active:
        sc, signals = score_runner(r)
        rescored.append({
            **r,
            "score":      sc,
            "signals":    signals,
            "flags_good": [s for s in signals if not s.startswith("⚠")],
            "flags_bad":  [s for s in signals if s.startswith("⚠")],
        })
    rescored.sort(key=lambda r: (-r.get("score", 0), r.get("sp_dec") or 999))

    horse_a = rescored[0] if len(rescored) >= 1 else None
    horse_b = rescored[1] if len(rescored) >= 2 else None

    if not horse_a:
        return

    field_size  = len(active)
    std_places  = place_terms(field_size)
    cons_places = conservative_place_terms(field_size)

    # Re-run tier with active runners
    going_raw = race_data.get("going", "")
    surface   = derive_surface({**race_data, "going": going_raw})
    race_for_tier = {
        "type":    race_data.get("type", "Unknown"),
        "surface": surface,
        "class":   race_data.get("race_class", "Unknown"),
        "runners": active,
        "dist_f":  race_data.get("dist", ""),
    }
    win_score = horse_a.get("score", 0)
    tier, tier_reasons = race_confidence(race_for_tier, win_score)

    # Get exchange odds from Betfair stub (returns {} until implemented)
    horse_ids     = [r.get("horse_id", "") for r in active]
    exchange_odds = get_exchange_odds(race_id, horse_ids)

    msg = format_pre_race_alert(
        race          = {**race_data, "all_runners": all_runners},
        horse_a       = horse_a,
        horse_b       = horse_b,
        tier          = tier,
        tier_reasons  = tier_reasons,
        places        = std_places,
        cons_places   = cons_places,
        exchange_odds = exchange_odds,
    )
    send_main(msg)
    logger.info(
        f"pre_race_job: sent for {race_data.get('course')} {race_data.get('off')}"
    )

    # Fire Betfair hook (no-op until betfair bot implemented)
    try:
        on_pre_race(
            race_id    = race_id,
            win_pick   = horse_a,
            place_picks = [horse_b] if horse_b else [],
            tier       = tier,
            tier_label = race_data.get("tier_label", ""),
        )
    except Exception as e:
        logger.debug(f"pre_race_job: betfair hook error: {e}")


# ── Single shared result poller ────────────────────────────────────────────────

def poll_all_results():
    """
    Called every 60 seconds by a single shared scheduler job.
    Loops through every race that has passed its off time and
    hasn't been settled yet. Sends result via results bot.
    """
    now       = datetime.now()
    today_str = date.today().strftime("%Y-%m-%d")
    client    = RacingAPIClient()
    races     = get_today_analysed()

    if not races:
        return

    for race in races:
        race_id = race.get("race_id", "")
        off_str = race.get("off_dt") or race.get("off", "")

        if not race_id or race_id in _settled:
            continue

        off_dt = _parse_off(off_str, today_str)
        if not off_dt:
            continue

        # Only poll races that started at least 10 minutes ago
        if now < off_dt + timedelta(minutes=10):
            continue

        # Enforce 90-minute timeout per race
        if race_id not in _poll_start:
            _poll_start[race_id] = now

        elapsed = (now - _poll_start[race_id]).total_seconds() / 60
        if elapsed > MAX_POLL_MINUTES:
            logger.warning(
                f"poll_all_results: {race_id} timed out after {MAX_POLL_MINUTES}m"
            )
            _settled.add(race_id)
            continue

        # Fetch result
        try:
            result = client.get_result_by_race_id(race_id)
        except Exception as e:
            logger.debug(f"poll_all_results: error fetching {race_id}: {e}")
            continue

        if not result or not _result_is_complete(result):
            continue

        logger.info(f"poll_all_results: result confirmed for {race_id}")
        _settled.add(race_id)

        try:
            _send_result(race, result)
        except Exception as e:
            logger.error(
                f"poll_all_results: failed to send result for {race_id}: {e}",
                exc_info=True,
            )


# ── Result processing ─────────────────────────────────────────────────────────

def _send_result(race: dict, result: dict):
    """Build and send the result notification for one race."""
    top1        = race.get("top1")
    top2        = race.get("top2")
    std_places  = race.get("places", 3)
    cons_places = race.get("cons_places", 4)
    tier        = race.get("tier", 0)

    if not top1:
        return

    # Merge finishing positions into all_runners for the full result table
    result_runners = result.get("runners", [])
    pos_lookup     = {}
    sp_lookup      = {}
    pos_str_lookup = {}
    sp_dec_lookup: dict[str, float] = {}
    for r in result_runners:
        hid = r.get("horse_id", "")
        if hid:
            pos_raw = r.get("position", "")
            try:
                pos_lookup[hid] = int(pos_raw) or None
            except (TypeError, ValueError):
                pos_lookup[hid] = None
            sp_lookup[hid]      = r.get("sp", "")
            pos_str_lookup[hid] = pos_raw
            if r.get("sp_dec"):
                sp_dec_lookup[hid] = float(r["sp_dec"])

    pre_runners = race.get("all_runners", [])
    pre_ids     = {r.get("horse_id") for r in pre_runners}
    merged      = []
    for r in pre_runners:
        hid = r.get("horse_id", "")
        merged.append({
            **r,
            "actual_pos": pos_lookup.get(hid),
            "position":   pos_str_lookup.get(hid) or r.get("position"),
            "sp":         sp_lookup.get(hid) or r.get("sp", ""),
        })
    # Add runners in result not on pre-race card (late entries)
    for r in result_runners:
        hid = r.get("horse_id", "")
        if hid not in pre_ids:
            merged.append({
                "horse_id":   hid,
                "horse":      r.get("horse", ""),
                "sp":         r.get("sp", ""),
                "sp_dec":     r.get("sp_dec"),
                "actual_pos": pos_lookup.get(hid),
                "position":   pos_str_lookup.get(hid, ""),
            })

    # Derive outcome
    outcome = _derive_outcome(race, result)

    race_with_result = {**race, "all_runners": merged}

    # Enrich horse_a/b with actual positions and result SP for display + streak tracker
    a_hid   = (top1 or {}).get("horse_id", "")
    b_hid   = (top2 or {}).get("horse_id", "") if top2 else ""
    a_pos   = pos_lookup.get(a_hid)
    b_pos   = pos_lookup.get(b_hid) if top2 else None
    horse_a = {
        **(top1 or {}),
        "actual_pos": a_pos,
        "position":   pos_str_lookup.get(a_hid, ""),
        "sp":         sp_lookup.get(a_hid) or (top1 or {}).get("sp", ""),
        "sp_dec":     sp_dec_lookup.get(a_hid) or (top1 or {}).get("sp_dec"),
    }
    horse_b = (
        {
            **(top2 or {}),
            "actual_pos": b_pos,
            "position":   pos_str_lookup.get(b_hid, ""),
            "sp":         sp_lookup.get(b_hid) or (top2 or {}).get("sp", ""),
            "sp_dec":     sp_dec_lookup.get(b_hid) or (top2 or {}).get("sp_dec"),
        }
        if top2 else None
    )

    # Log outcome
    if outcome.get("cons_win"):
        logger.info(f"result: CONS WIN — {race.get('course')} {race.get('off')}")
    elif outcome.get("std_win"):
        logger.info(f"result: STD WIN — {race.get('course')} {race.get('off')}")
    else:
        logger.info(f"result: LOSS — {race.get('course')} {race.get('off')}")

    msg = format_result(
        race        = race_with_result,
        horse_a     = horse_a,
        horse_b     = horse_b,
        places      = std_places,
        cons_places = cons_places,
        outcome     = outcome,
        tier        = tier,
    )
    send_results(msg)

    # Streak tracker — post a follow-up showing compounding bank state.
    # Pass horse_a/horse_b so result SP (not morning card estimate) is used.
    # Streak tracker moved to betfair_main._paper_settle()
    # Uses real Betfair place prices instead of SP estimation.
    # Do not call here.
    pass


# ── Helpers ───────────────────────────────────────────────────────────────────

def _find_race(race_id: str) -> dict | None:
    for race in get_today_analysed():
        if race.get("race_id") == race_id:
            return race
    return None


def _result_is_complete(result: dict) -> bool:
    """At least one runner has a confirmed numeric finishing position."""
    return any(
        r.get("position") and str(r["position"]).strip().isdigit()
        for r in result.get("runners", [])
    )


def _parse_off(off_str: str, today_str: str) -> datetime | None:
    """Parse off_dt ISO string or HH:MM string into a naive local datetime."""
    if not off_str:
        return None
    try:
        if "T" in off_str:
            from datetime import timezone
            dt = datetime.fromisoformat(off_str)
            if dt.tzinfo:
                dt = dt.astimezone()
            return dt.replace(tzinfo=None)
        parts = off_str.strip().split(":")
        base  = datetime.strptime(today_str, "%Y-%m-%d")
        return base.replace(hour=int(parts[0]), minute=int(parts[1]))
    except (ValueError, IndexError):
        return None
