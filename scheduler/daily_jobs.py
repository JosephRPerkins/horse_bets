"""
scheduler/daily_jobs.py

Jobs that run once per day on a fixed schedule:
  - midnight_job        : clear stale card, pre-fetch tomorrow
  - morning_briefing_job: fetch today's card, analyse, send 12pm summary
  - bet365_job          : send Bet365 daily action plan
  - end_of_day_job      : collate results, send summary, save history

Uses predict.py (score_runner) and predict_v2.py (race_confidence, tiers)
instead of the sacred core/ files from v1.
"""

import os
import json
import logging
from datetime import datetime, date
import time

import config
from core.api_client import RacingAPIClient
from predict          import score_runner, place_terms
from predict_v2       import race_confidence, conservative_place_terms, TIER_SKIP
from notifications.formatter import (
    format_morning_briefing,
    format_end_of_day,
)
from notifications.telegram  import send_main, send_results, send_bet365
from utils.helpers import (
    normalise_going, dist_furlongs, dist_label,
    derive_surface, form_confidence,
)

logger = logging.getLogger(__name__)

os.makedirs(config.DIR_CARDS,   exist_ok=True)
os.makedirs(config.DIR_HISTORY, exist_ok=True)

# ── State (shared with race_jobs via module-level list) ───────────────────────

_today_analysed: list[dict] = []


def get_today_analysed() -> list[dict]:
    return _today_analysed


def _hydrate_today_analysed(races: list[dict]) -> None:
    """Set the in-memory state from an externally loaded list (e.g. from disk)."""
    global _today_analysed
    if races and not _today_analysed:
        _today_analysed = races


# ── Midnight job ──────────────────────────────────────────────────────────────

def midnight_job():
    """
    Runs at 04:45. Archives today's card, enriches with results, resets streaks.
    No card fetch — that happens at T-30 before first race via morning_briefing_job.
    """
    logger.info("midnight_job: archiving and resetting")

    today_card = os.path.join(config.DIR_CARDS, "today.json")
    if os.path.exists(today_card):
        # ── Archive today.json as dated file ─────────────────────────────
        try:
            import shutil, json as _json
            with open(today_card) as _f:
                _card = _json.load(_f)
            card_date = _card.get("date")
            if card_date:
                archive_path = os.path.join(config.DIR_CARDS, f"{card_date}.json")
                if not os.path.exists(archive_path):
                    shutil.copy2(today_card, archive_path)
                    logger.info(f"midnight_job: archived today.json as {card_date}.json")
                # ── Enrich archived card with results ─────────────────────
                try:
                    from betfair.api import _norm_horse as _norm
                    import requests as _requests
                    auth = (config.RACING_API_USERNAME, config.RACING_API_PASSWORD)
                    res = _requests.get(
                        f"{config.RACING_API_BASE_URL}/results",
                        params={"date": card_date, "region": "gb"},
                        auth=auth, timeout=30
                    )
                    if res.status_code == 200:
                        results_by_id = {
                            (r.get("race_id") or r.get("id")): r
                            for r in res.json().get("results", [])
                            if r.get("race_id") or r.get("id")
                        }
                        with open(archive_path) as f:
                            card = json.load(f)
                        enriched = 0
                        for race in card.get("races", []):
                            result = results_by_id.get(race.get("race_id"))
                            if not result:
                                continue
                            result_runners = {
                                _norm(r.get("horse","")): r
                                for r in result.get("runners", [])
                            }
                            for runner in (race.get("all_runners") or []):
                                rr = result_runners.get(_norm(runner.get("horse","")))
                                if rr:
                                    runner["finish_pos"]    = rr.get("position")
                                    runner["sp_dec_final"]  = rr.get("sp_dec")
                                    runner["bsp_final"]     = rr.get("bsp") or ""
                            race["result_enriched"] = True
                            enriched += 1
                        with open(archive_path, "w") as f:
                            json.dump(card, f, indent=2, default=str)
                        logger.info(f"midnight_job: enriched {enriched} races in {card_date}.json")
                except Exception as e:
                    logger.error(f"midnight_job: result enrichment failed: {e}")
        except Exception as e:
            logger.error(f"midnight_job: archive failed: {e}")

        os.remove(today_card)
        logger.info("midnight_job: cleared stale today.json")

    # ── Reset streaks ─────────────────────────────────────────────────────
    try:
        from notifications.streak_tracker import reset_streaks
        reset_streaks()
    except Exception as e:
        logger.warning(f"midnight_job: streak reset failed: {e}")

    logger.info("midnight_job: done")

# ── Morning briefing job ───────────────────────────────────────────────────────

def fetch_today_card(scheduler=None, notify: bool = False) -> int:
    """
    Fetch today's pro racecard, score every race, save to disk.
    Returns the number of races loaded (0 = nothing found).

    The pro racecard already contains trainer_14_days, form string, OR/TSR/RPR
    and odds — no separate enrichment calls needed.

    If notify=True, sends a Telegram message while fetching so the user knows
    the bot is working.  Called from startup_catchup and morning_briefing_job.
    """
    global _today_analysed

    logger.info("fetch_today_card: starting")
    client    = RacingAPIClient()
    today_str = date.today().strftime("%Y-%m-%d")

    try:
        racecards = client.get_todays_racecards(
            region_codes=config.TARGET_REGIONS
        )
        logger.info(f"fetch_today_card: {len(racecards)} races from API")

        if not racecards:
            if notify:
                send_main(
                    f"⚠️ No races found for today ({today_str}) in "
                    f"{', '.join(config.TARGET_REGIONS).upper()}.\n"
                    f"Will retry at next scheduled briefing time."
                )
            return 0

        # Normalise runners — pro racecard has trainer/form/odds embedded
        normalised_racecards = []
        for race in racecards:
            norm_runners = [RacingAPIClient.normalise_runner(r)
                            for r in race.get("runners", [])]
            normalised_racecards.append({**race, "runners": norm_runners})

        analysed        = [_analyse_race(r) for r in normalised_racecards]
        _today_analysed = analysed

        card_path = os.path.join(config.DIR_CARDS, "today.json")
        with open(card_path, "w") as f:
            json.dump({"date": today_str, "races": analysed}, f,
                      indent=2, default=str)
        logger.info(f"fetch_today_card: saved {len(analysed)} races to disk")

        if scheduler is not None:
            from scheduler.main_scheduler import register_race_jobs
            register_race_jobs(scheduler, analysed)
            logger.info("fetch_today_card: race jobs registered")

        return len(analysed)

    except Exception as e:
        logger.error(f"fetch_today_card failed: {e}", exc_info=True)
        if notify:
            send_main(f"❌ Error fetching today's card: {e}")
        return 0


def morning_briefing_job(scheduler=None):
    """
    Runs at MORNING_BRIEFING_TIME (default 12:00) and on /refresh.
    Fetches the card (if not already loaded), then sends the daily briefing.
    Does NOT send the briefing if no races were found.
    """
    logger.info("morning_briefing_job: starting")

    n = fetch_today_card(scheduler=scheduler, notify=False)
    if n == 0:
        logger.warning("morning_briefing_job: 0 races — skipping briefing")
        send_main(
            f"⚠️ No races found for today in "
            f"{', '.join(config.TARGET_REGIONS).upper()} — briefing skipped."
        )
        return

    prev_summary = _load_previous_summary()
    msg = format_morning_briefing(
        analysed_races   = _today_analysed,
        previous_summary = prev_summary,
    )
    send_main(msg)
    logger.info(f"morning_briefing_job: sent ({n} races)")


# ── Bet365 daily analysis ─────────────────────────────────────────────────────

def run_bet365_daily():
    """
    Fetch today's card, build Bet365 chain plan, send to BET365 bot.
    Callable from /bet365 command or scheduled at 09:30.
    """
    logger.info("run_bet365_daily: starting")
    today_str = date.today().strftime("%A %-d %B %Y")

    try:
        # Use cached card if available, otherwise fetch
        races = _today_analysed or _load_today_analysed()
        if not races:
            client   = RacingAPIClient()
            raw      = client.get_todays_racecards(region_codes=config.TARGET_REGIONS)
            enriched = client.enrich_runners(raw)
            races    = [_analyse_race(r) for r in enriched]

        # Filter to quality races only
        from predict_v2 import TIER_GOOD
        quality = [r for r in races if r.get("tier", -1) >= TIER_GOOD]

        lines = [
            f"📋 <b>BET365 ACTION PLAN — {today_str}</b>",
            f"{'═' * 30}",
        ]

        if not quality:
            lines.append("No qualifying races today (GOOD tier or above)")
        else:
            lines.append(f"{len(quality)} quality races:\n")
            for r in sorted(quality, key=lambda x: x.get("off", "99:99")):
                a     = r.get("top1") or {}
                b     = r.get("top2") or {}
                badge = _tier_badge(r.get("tier", 0))
                std   = r.get("places", "?")
                cons  = r.get("cons_places", "?")
                lines.append(
                    f"{badge} <b>{r.get('off','?')} {r.get('course','?')}</b>\n"
                    f"  ⭐ {a.get('horse','?')} ({a.get('sp','?')}) + "
                    f"🔵 {b.get('horse','?')} ({b.get('sp','?')})\n"
                    f"  Top-{std} each | cons: Top-{cons}\n"
                )

        send_bet365("\n".join(lines))
        logger.info("run_bet365_daily: sent")

    except Exception as e:
        logger.error(f"run_bet365_daily failed: {e}", exc_info=True)


def _tier_badge(tier: int) -> str:
    from predict_v2 import TIER_SUPREME, TIER_STRONG, TIER_GOOD, TIER_STD, TIER_SKIP
    badges = {
        TIER_SUPREME: "🔥🔥🔥",
        TIER_STRONG:  "🔥🔥",
        TIER_GOOD:    "🔥",
        TIER_STD:     "·",
        TIER_SKIP:    "✗",
    }
    return badges.get(tier, "·")


# ── End of day job ─────────────────────────────────────────────────────────────

def end_of_day_job():
    """
    Runs 90 minutes after the last race.
    Collates results, sends end-of-day summary, saves history.
    """
    logger.info("end_of_day_job: starting")
    client    = RacingAPIClient()
    today_str = date.today().strftime("%Y-%m-%d")

    try:
        all_results = client.get_results_today(
            region_codes=config.TARGET_REGIONS
        )
        result_map  = {r.get("race_id"): r for r in all_results}

        analysed = _today_analysed or _load_today_analysed()

        paired_results = []
        for race in analysed:
            rid = race.get("race_id", "")
            raw = result_map.get(rid)
            if raw and race.get("top1") and race.get("top2"):
                outcome = _derive_outcome(race, raw)
                paired_results.append({**outcome, "race_id": rid})

        cons_wins = sum(1 for r in paired_results if r.get("cons_win"))
        losses    = sum(1 for r in paired_results if not r.get("cons_win"))

        msg = format_end_of_day(
            date_str       = datetime.now().strftime("%A %-d %B %Y"),
            analysed_races = analysed,
            results        = paired_results,
        )
        send_main(msg)
        logger.info(f"end_of_day_job: sent — {cons_wins}W {losses}L")

        history = {
            "date":       today_str,
            "total_bets": cons_wins + losses,
            "cons_wins":  cons_wins,
            "races":      paired_results,
            "best_call":  _best_call_label(analysed, paired_results),
        }
        hist_path = os.path.join(config.DIR_HISTORY, f"{today_str}.json")
        with open(hist_path, "w") as f:
            json.dump(history, f, indent=2, default=str)
        logger.info(f"end_of_day_job: history saved → {hist_path}")

    except Exception as e:
        logger.error(f"end_of_day_job failed: {e}", exc_info=True)


# ── Race analysis ─────────────────────────────────────────────────────────────

def _analyse_race(race: dict) -> dict:
    """
    Score runners with predict.py, assign confidence tier from predict_v2.py.
    Returns a structured race dict for the bot to use.
    """
    runners = race.get("runners", [])

    # ── Time ──────────────────────────────────────────────────────────────────
    off_dt_raw = race.get("off_dt") or ""
    if "T" in off_dt_raw:
        off = off_dt_raw.split("T")[1][:5]
    else:
        off_raw = race.get("off_time") or race.get("off") or ""
        if off_raw and ":" in off_raw:
            parts = off_raw.split(":")
            off   = f"{int(parts[0]):02d}:{parts[1]}"
        else:
            off = off_raw

    # ── Distance ──────────────────────────────────────────────────────────────
    dist = (race.get("distance_round") or
            race.get("distance") or
            race.get("dist") or "")

    dist_f_val = None
    raw_df = race.get("distance_f") or race.get("dist_f")
    if raw_df:
        try:
            dist_f_val = float(raw_df)
        except (ValueError, TypeError):
            pass
    if dist_f_val is None:
        dist_f_val = dist_furlongs(dist)

    # ── Going + surface ───────────────────────────────────────────────────────
    going_raw  = race.get("going_detailed") or race.get("going") or ""
    going_norm = normalise_going(going_raw)
    surface    = derive_surface({**race, "going": going_raw})

    # ── Field size ────────────────────────────────────────────────────────────
    try:
        field_size = int(race.get("field_size") or len(runners))
    except (ValueError, TypeError):
        field_size = len(runners)

    # ── Score runners (predict.py) ────────────────────────────────────────────
    scored = []
    for r in runners:
        sc, signals = score_runner(r)
        scored.append({
            **r,
            "score":       sc,
            "signals":     signals,
            "flags_good":  [s for s in signals if not s.startswith("⚠")],
            "flags_bad":   [s for s in signals if s.startswith("⚠")],
        })
    scored.sort(key=lambda r: (-r.get("score", 0),
                               r.get("sp_dec") or 999))

    top1 = scored[0] if len(scored) >= 1 else None
    top2 = scored[1] if len(scored) >= 2 else None

    win_score = top1.get("score", 0) if top1 else 0

    # ── Place terms ───────────────────────────────────────────────────────────
    std_places  = place_terms(field_size)
    cons_places = conservative_place_terms(field_size)

    # ── Confidence tier ───────────────────────────────────────────────────────
    race_for_tier = {
        "type":    race.get("type", "Unknown"),
        "surface": surface,
        "class":   race.get("race_class") or race.get("class", "Unknown"),
        "runners": runners,          # raw runners — has tsr/or fields
        "dist_f":  dist,             # string e.g. "2m4f" for dist_furlongs()
    }
    tier, reasons = race_confidence(race_for_tier, win_score)

    from predict_v2 import TIER_LABELS
    tier_label = TIER_LABELS.get(tier, "· STANDARD")

    return {
        "race_id":      race.get("race_id", ""),
        "race_name":    race.get("race_name", ""),
        "course":       race.get("course", ""),
        "off":          off,
        "off_dt":       race.get("off_dt", ""),
        "type":         race.get("type", ""),
        "going":        going_norm,
        "surface":      surface,
        "dist":         dist,
        "dist_f":       dist_f_val,
        "dist_label":   dist_label(dist_f_val),
        "field_size":   field_size,
        "race_class":   race.get("race_class") or race.get("class", ""),
        "verdict":      race.get("verdict", ""),
        "tip":          race.get("tip", ""),
        "tier":         tier,
        "tier_label":   tier_label,
        "tier_reasons": reasons,
        "win_score":    win_score,
        "places":       std_places,
        "cons_places":  cons_places,
        "top1":         top1,
        "top2":         top2,
        "all_runners":  scored,
    }


def _derive_outcome(race: dict, result_race: dict) -> dict:
    """
    Build outcome dict from analysed race + raw result.
    Checks both standard and conservative place terms.
    """
    top1        = race.get("top1") or {}
    top2        = race.get("top2") or {}
    std_places  = race.get("places", 3)
    cons_places = race.get("cons_places", 4)

    # Build position lookup from result runners
    result_runners = result_race.get("runners", [])
    pos_lookup     = {}
    for r in result_runners:
        hid = r.get("horse_id", "")
        if hid:
            try:
                pos_lookup[hid] = int(r.get("position", 0)) or None
            except (TypeError, ValueError):
                pos_lookup[hid] = None

    a_pos = pos_lookup.get(top1.get("horse_id", ""))
    b_pos = pos_lookup.get(top2.get("horse_id", ""))

    def placed(pos, n):
        if pos is None:
            return False
        try:
            return int(pos) <= n
        except (TypeError, ValueError):
            return False

    std_a  = placed(a_pos, std_places)
    std_b  = placed(b_pos, std_places)
    cons_a = placed(a_pos, cons_places)
    cons_b = placed(b_pos, cons_places)

    return {
        "a_pos":   a_pos,
        "b_pos":   b_pos,
        "std_a":   std_a,
        "std_b":   std_b,
        "cons_a":  cons_a,
        "cons_b":  cons_b,
        "std_win": std_a and std_b,
        "cons_win": cons_a and cons_b,
    }


# ── File helpers ──────────────────────────────────────────────────────────────

def _load_previous_summary() -> dict | None:
    """Load yesterday's history file if it exists."""
    from datetime import timedelta
    yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    path      = os.path.join(config.DIR_HISTORY, f"{yesterday}.json")
    if os.path.exists(path):
        try:
            with open(path) as f:
                return json.load(f)
        except Exception:
            pass
    return None


def _load_today_analysed() -> list[dict]:
    """
    Load today's analysed races from disk if not in memory.
    Also hydrates _today_analysed so subsequent calls to get_today_analysed()
    return the loaded data without another disk read.
    """
    global _today_analysed

    path      = os.path.join(config.DIR_CARDS, "today.json")
    today_str = date.today().strftime("%Y-%m-%d")

    if not os.path.exists(path):
        return []

    try:
        with open(path) as f:
            data = json.load(f)
    except Exception:
        return []

    card_date = data.get("date", "")
    if card_date != today_str:
        logger.info(
            f"_load_today_analysed: card is from {card_date}, "
            f"not today ({today_str}) — will fetch fresh"
        )
        return []

    races = data.get("races", [])
    logger.info(f"_load_today_analysed: loaded {len(races)} races from disk")

    # Hydrate in-memory state so get_today_analysed() works without re-reading
    if races and not _today_analysed:
        _today_analysed = races

    return races


def _best_call_label(analysed: list[dict], results: list[dict]) -> str | None:
    result_map = {r.get("race_id"): r for r in results}
    wins = [
        r for r in analysed
        if result_map.get(r.get("race_id"), {}).get("cons_win")
    ]
    if not wins:
        return None
    best = max(wins, key=lambda r: r.get("win_score", 0))
    a    = best.get("top1") or {}
    b    = best.get("top2") or {}
    return (
        f"{best.get('off','?')} {best.get('course','?')} — "
        f"{a.get('horse','?')} + {b.get('horse','?')}"
    )
