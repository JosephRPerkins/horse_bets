"""
notifications/formatter.py

Builds every message type as a string.
Never sends anything — call telegram.send_main() or send_results() separately.

Message types:
  1. morning_briefing   — 12pm full day overview with tier indicators
  2. pre_race_alert     — T-10min, live odds, non-runner status, RP verdict match
  3. result_notification — after race settles, full result with incident codes
  4. end_of_day_summary  — full day record, tier breakdown

Confidence tiers (from predict_v2.py):
  🔥🔥🔥 SUPREME   — TSR solo trigger (93% win)
  🔥🔥  STRONG    — Score ≥7 + right conditions (65%)
  🔥    GOOD      — Score ≥6 / Class 3/4 / small field (56%)
  ·     STANDARD  — everything else (43%)
  ✗     SKIP      — Flat/AW low score, 12+ field non-jump, Class 1/2
"""

import re
from datetime import datetime
from difflib import SequenceMatcher

from predict_v2 import TIER_SUPREME, TIER_STRONG, TIER_GOOD, TIER_STD, TIER_SKIP
from utils.helpers import ordinal, dec_to_fractional, runner_status_str, is_non_runner

# ── Constants ─────────────────────────────────────────────────────────────────

WIN     = "✅"
LOSS    = "❌"
WARNING = "⚠️"
INFO    = "ℹ️"
RACE    = "🏇"
CLOCK   = "⏰"
PIN     = "📌"
FIRE    = "🔥"
STAR    = "⭐"
TROPHY  = "🏆"
CHART   = "📊"
SCROLL  = "📋"
BELL    = "🔔"
DIVIDER   = "─" * 30
THICK_DIV = "═" * 30

TIER_BADGE = {
    TIER_SUPREME: "🔥🔥🔥 SUPREME",
    TIER_STRONG:  "🔥🔥 STRONG",
    TIER_GOOD:    "🔥 GOOD",
    TIER_STD:     "· STANDARD",
    TIER_SKIP:    "✗ SKIP",
}

TIER_BET_ADVICE = {
    TIER_SUPREME: f"{WIN} WIN selection — 93% historical rate",
    TIER_STRONG:  f"{WIN} WIN / each-way — 65% historical rate",
    TIER_GOOD:    f"{INFO} Each-way — 56% historical rate",
    TIER_STD:     f"{INFO} Info only — 43% avg",
    TIER_SKIP:    f"✗ Avoid — historically weak",
}


# ── 1. Morning Briefing ───────────────────────────────────────────────────────

def format_morning_briefing(
    analysed_races: list[dict],
    previous_summary: dict | None = None,
) -> str:
    """
    Full day overview sent at 12pm (noon job).
    Shows tier indicators so quality races stand out immediately.
    """
    date_str = datetime.now().strftime("%A %-d %B %Y")

    sorted_races = sorted(analysed_races, key=lambda r: r.get("off", "99:99"))

    supreme = [r for r in sorted_races if r.get("tier") == TIER_SUPREME]
    strong  = [r for r in sorted_races if r.get("tier") == TIER_STRONG]
    good    = [r for r in sorted_races if r.get("tier") == TIER_GOOD]

    fire_count = len(supreme) + len(strong) + len(good)

    lines = [
        f"📅 <b>RACE GUIDE — {date_str}</b>",
        f"{CHART} {len(analysed_races)} races today",
    ]
    if fire_count:
        lines.append(f"{FIRE} {fire_count} quality races flagged")
    lines.append(THICK_DIV)

    # ── Yesterday recap ───────────────────────────────────────────────────────
    if previous_summary:
        lines.append(f"\n{SCROLL} <b>Yesterday</b>")
        wins  = previous_summary.get("cons_wins", 0)
        total = previous_summary.get("total_bets", 0)
        lines.append(f"Conservative: {wins}/{total} wins")
        best  = previous_summary.get("best_call")
        if best:
            lines.append(f"{TROPHY} Best: {best}")
        lines.append(DIVIDER)

    # ── Top picks callout ─────────────────────────────────────────────────────
    if supreme or strong:
        lines.append(f"\n{FIRE} <b>Best bets today:</b>")
        for r in supreme + strong:
            a   = r.get("top1") or {}
            b   = r.get("top2") or {}
            badge = TIER_BADGE.get(r.get("tier"), "·")
            std  = r.get("places", "?")
            cons = r.get("cons_places", "?")
            line = (
                f"{badge} — {r.get('off','?')} {r.get('course','?')}\n"
                f"  {STAR} {a.get('horse','?')} ({a.get('sp','?')}) + "
                f"🔵 {b.get('horse','?')} ({b.get('sp','?')})\n"
                f"  Top-{std} each (cons: Top-{cons})"
            )
            lines.append(line)
        lines.append(DIVIDER)

    # ── Full card in time order ───────────────────────────────────────────────
    lines.append(f"\n{RACE} <b>Full card ({len(sorted_races)} races):</b>")
    for r in sorted_races:
        lines.append(_race_one_liner(r))

    lines.append(f"\n{THICK_DIV}")
    lines.append(
        f"{INFO} Alerts fire 10 mins before each race.\n"
        f"🔥🔥🔥/🔥🔥 = strong bets | 🔥 = each-way | · = info"
    )

    return "\n".join(lines)


def _race_one_liner(r: dict) -> str:
    """Single line summary for morning briefing race list."""
    tier   = r.get("tier", TIER_STD)
    badge  = TIER_BADGE.get(tier, "·")
    off    = r.get("off", "?")
    course = r.get("course", "?")
    rtype  = r.get("type", "?")
    dist   = r.get("dist", "?")
    going  = r.get("going", "?")
    field  = r.get("field_size", 0)
    a      = r.get("top1") or {}
    b      = r.get("top2") or {}
    std    = r.get("places", "?")
    cons   = r.get("cons_places", "?")

    line  = f"\n{badge} <b>{off} {course}</b>\n"
    line += f"  {rtype} | {dist} | {going} | {field} runners\n"

    if a:
        line += f"  {TROPHY} {a.get('horse','?')} ({a.get('sp','?')})\n"
    if a and b:
        line += f"  {PIN} {a.get('horse','?')} + {b.get('horse','?')} — Top-{std} / cons Top-{cons}"
    elif a:
        line += f"  {PIN} Top-{std} pick"

    # Show tier reasons only for quality races
    reasons = r.get("tier_reasons", [])
    if tier >= TIER_GOOD and reasons:
        line += f"\n  {' · '.join(reasons[:2])}"

    return line


# ── 2. Pre-Race Alert ─────────────────────────────────────────────────────────

def format_pre_race_alert(
    race: dict,
    horse_a: dict,
    horse_b: dict | None,
    tier: int,
    tier_reasons: list[str],
    places: int,
    cons_places: int,
    exchange_odds: dict | None = None,
) -> str:
    """
    Sent 10 minutes before each race.
    Live odds, non-runner flags, form, place terms per pick, RP verdict matching.
    """
    course     = race.get("course", "?")
    off        = race.get("off", "?")
    rtype      = race.get("type", "?")
    dist       = race.get("dist", "?")
    going      = race.get("going", "?")
    field_size = race.get("field_size") or len(race.get("all_runners", []))
    badge      = TIER_BADGE.get(tier, "·")
    bet_advice = TIER_BET_ADVICE.get(tier, "")

    lines = [
        f"📍 <b>{off} {course.upper()}</b>",
        f"{rtype} | {dist} | {going} | {field_size} runners",
        f"{badge}",
    ]
    if tier_reasons:
        lines.append(f"  {' · '.join(tier_reasons[:2])}")
    lines.append(bet_advice)
    if tier == TIER_SKIP:
        lines.append(f"{WARNING} <b>SKIP RACE</b> — historically weak category")
    lines.append(DIVIDER)

    # ── Pick 1 ────────────────────────────────────────────────────────────────
    if horse_a:
        lines.append(_pick_block(horse_a, "⭐ Pick 1", places, cons_places, exchange_odds))
        lines.append("")

    # ── Pick 2 ────────────────────────────────────────────────────────────────
    if horse_b and horse_b.get("horse_id") != (horse_a or {}).get("horse_id"):
        lines.append(_pick_block(horse_b, "🔵 Pick 2", places, cons_places, exchange_odds))
        lines.append("")

    lines.append(DIVIDER)

    # ── RP Verdict + fuzzy matching ───────────────────────────────────────────
    verdict = race.get("verdict", "") or race.get("tip", "")
    if verdict:
        lines.append(f"\n📰 <b>RP Verdict</b>")
        lines.append(f"  {verdict[:220]}{'...' if len(verdict) > 220 else ''}")

        all_runners = race.get("all_runners", [])
        a_name = (horse_a or {}).get("horse", "")
        b_name = (horse_b or {}).get("horse", "") if horse_b else ""
        pick_ids = {
            (horse_a or {}).get("horse_id", ""): ("Pick 1", a_name),
            (horse_b or {}).get("horse_id", ""): ("Pick 2", b_name),
        }

        # Check our picks
        for hid, (label, name) in pick_ids.items():
            if not name:
                continue
            in_v = _is_in_verdict(name, verdict)
            icon = WIN if in_v else "·"
            lines.append(f"  {icon} {name} — {'✓ mentioned' if in_v else 'not mentioned'} ({label})")

        # Flag any other runner mentioned in the verdict — show what finish would count
        from predict import place_terms as _place_terms
        std_for_field  = places
        cons_for_field = cons_places
        for r in all_runners:
            hid  = r.get("horse_id", "")
            name = r.get("horse", "")
            if not name or hid in pick_ids:
                continue
            if _is_in_verdict(name, verdict):
                sp_str_ = r.get("sp", "")
                note    = f"({sp_str_})" if sp_str_ else ""
                lines.append(
                    f"  💬 {name} {note} — RP mention "
                    f"(needs Top-{std_for_field}, cons Top-{cons_for_field})"
                )

    # ── All runners table ─────────────────────────────────────────────────────
    all_runners = race.get("all_runners", [])
    if all_runners:
        a_id = (horse_a or {}).get("horse_id", "")
        b_id = (horse_b or {}).get("horse_id", "")
        lines.append(_full_field_with_status(all_runners, a_id, b_id, exchange_odds))

    return "\n".join(lines)


def _pick_block(horse: dict, label: str, places: int, cons_places: int,
                exchange_odds: dict | None) -> str:
    """Formatted block for one pick — odds, ratings, form, place terms."""
    name   = horse.get("horse", "?")
    sp     = horse.get("sp", "?")
    bfsp   = horse.get("bf_sp", "")
    exch   = _exch_str(exchange_odds, horse.get("horse_id", ""))
    or_v   = horse.get("or", "—")
    rpr    = horse.get("rpr", "—")
    tsr    = horse.get("tsr", "—")
    form   = (horse.get("form") or
              (horse.get("form_detail") or {}).get("form_string") or "—")

    odds_parts = [f"Bet365: {sp}"]
    if bfsp:
        odds_parts.append(f"Betfair: {bfsp}")
    if exch:
        odds_parts.append(f"Exchange: {exch}")

    lines = [
        f"\n<b>{label}: {name}</b>",
        f"  {' | '.join(odds_parts)}",
        f"  OR:{or_v}  RPR:{rpr}  TSR:{tsr}  Form: {str(form)[:14] or '—'}",
        f"  {PIN} Top-{places} / cons Top-{cons_places}",
    ]
    good = horse.get("flags_good", [])
    bad  = horse.get("flags_bad", [])
    if good:
        lines.append(f"  {' · '.join(good[:3])}")
    if bad:
        lines.append(f"  {WARNING} {' · '.join(bad[:2])}")
    return "\n".join(lines)


def _full_field_with_status(runners: list, a_id: str, b_id: str,
                             exchange_odds: dict | None) -> str:
    """
    Full field table sorted by price.
    Non-runners are identified by the official status flag only —
    a blank SP does not mean NR (horse may simply have no bookmaker price yet).
    """
    if not runners:
        return ""

    def sort_key(r):
        if is_non_runner(r):
            return 9999
        sp = r.get("sp_dec")
        try:
            return float(sp) if sp else 998
        except (TypeError, ValueError):
            return 998

    sorted_runners = sorted(runners, key=sort_key)
    lines = [f"\n{DIVIDER}\n📋 <b>All runners</b>"]

    for r in sorted_runners:
        name = (r.get("horse") or "?")[:22]
        hid  = r.get("horse_id", "")
        nr   = is_non_runner(r)

        if hid == a_id:
            pick_marker = "  ⭐ Pick 1"
        elif hid == b_id:
            pick_marker = "  🔵 Pick 2"
        else:
            pick_marker = ""

        if nr:
            lines.append(f"  ❌ {'':6}  {name:22}  [NR]{pick_marker}")
            continue

        sp     = r.get("sp", "—")
        exch   = _exch_str(exchange_odds, hid)
        sp_col = sp + (f" ({exch})" if exch else "")
        lines.append(f"  ✅ {sp_col:8}  {name:22}{pick_marker}")

    return "\n".join(lines)


def _exch_str(exchange_odds: dict | None, horse_id: str) -> str:
    """Format exchange odds for display, or empty string."""
    if not exchange_odds or not horse_id:
        return ""
    val = exchange_odds.get(horse_id)
    if val is None:
        return ""
    try:
        return f"{float(val):.2f}"
    except (TypeError, ValueError):
        return ""


def _is_in_verdict(horse_name: str, verdict_text: str) -> bool:
    """
    Fuzzy-check if a horse name appears in the RP verdict text.
    Uses SequenceMatcher on sliding windows of 1–4 words.
    """
    if not verdict_text or not horse_name:
        return False
    name_lower    = horse_name.lower().strip()
    verdict_lower = verdict_text.lower()

    # Direct substring match first (case-insensitive)
    if name_lower in verdict_lower:
        return True

    # Sliding window fuzzy match
    words = verdict_lower.split()
    for length in range(1, min(5, len(horse_name.split()) + 2)):
        for i in range(len(words) - length + 1):
            phrase = " ".join(words[i:i + length])
            ratio  = SequenceMatcher(None, name_lower, phrase).ratio()
            if ratio >= 0.80:
                return True

    return False


# ── 3. Result Notification ────────────────────────────────────────────────────

def format_result(
    race: dict,
    horse_a: dict,
    horse_b: dict | None,
    places: int,
    cons_places: int,
    outcome: dict,
    tier: int,
) -> str:
    """
    Sent via results bot once the race settles.
    Shows actual finishing positions with incident codes.
    Clearly marks WIN/PLACE outcome vs non-runner/incident.
    """
    course = race.get("course", "?")
    off    = race.get("off", "?")
    badge  = TIER_BADGE.get(tier, "·")

    std_win  = outcome.get("std_win", False)
    cons_win = outcome.get("cons_win", False)

    # Header
    if std_win:
        icon = WIN
    elif cons_win:
        icon = "🟡"   # cons win only
    else:
        icon = LOSS

    lines = [
        f"{icon} <b>RESULT — {off} {course.upper()}</b>",
        badge,
        DIVIDER,
    ]

    # ── Horse A ───────────────────────────────────────────────────────────────
    a_name        = (horse_a or {}).get("horse", "?")
    a_sp          = (horse_a or {}).get("sp", "?")
    a_placed_std  = outcome.get("std_a", False)
    a_placed_cons = outcome.get("cons_a", False)
    # Use the raw position string (e.g. "UR", "PU", "1") stored on horse_a,
    # not the numeric outcome field which loses incident codes.
    a_finish = runner_status_str(horse_a or {})
    a_icon   = WIN if a_placed_std else ("🟡" if a_placed_cons else LOSS)
    lines.append(
        f"\n{STAR} <b>{a_name}</b> ({a_sp})\n"
        f"  Needed Top-{places} → {a_finish} {a_icon}"
    )
    if a_placed_cons and not a_placed_std:
        lines.append(f"  (✓ conservative Top-{cons_places})")

    # ── Horse B ───────────────────────────────────────────────────────────────
    if horse_b:
        b_name        = horse_b.get("horse", "?")
        b_sp          = horse_b.get("sp", "?")
        b_placed_std  = outcome.get("std_b", False)
        b_placed_cons = outcome.get("cons_b", False)
        b_finish      = runner_status_str(horse_b)
        b_icon        = WIN if b_placed_std else ("🟡" if b_placed_cons else LOSS)
        lines.append(
            f"\n🔵 <b>{b_name}</b> ({b_sp})\n"
            f"  Needed Top-{places} → {b_finish} {b_icon}"
        )
        if b_placed_cons and not b_placed_std:
            lines.append(f"  (✓ conservative Top-{cons_places})")

    lines.append(f"\n{DIVIDER}")

    # ── Overall outcome ───────────────────────────────────────────────────────
    if tier >= TIER_GOOD:
        if std_win:
            lines.append(f"{WIN} <b>STD WIN</b> + {WIN} <b>CONS WIN</b>")
        elif cons_win:
            lines.append(f"· STD LOSS | {WIN} <b>CONS WIN</b>")
        else:
            lines.append(f"{LOSS} <b>BOTH LOST</b>")
    else:
        if std_win:
            lines.append(f"{INFO} Info only — pair would have WON (std)")
        elif cons_win:
            lines.append(f"{INFO} Info only — pair would have WON (cons)")
        else:
            lines.append(f"{INFO} Info only — pair would have LOST")

    # ── Full result table ─────────────────────────────────────────────────────
    all_runners = race.get("all_runners", [])
    if all_runners:
        a_id = (horse_a or {}).get("horse_id", "")
        b_id = (horse_b or {}).get("horse_id", "")
        lines.append(_full_result_table(all_runners, a_id, b_id))

    return "\n".join(lines)


def _full_result_table(runners: list, a_id: str, b_id: str) -> str:
    """Full result table sorted by finishing position."""
    if not runners:
        return ""

    def pos_key(r):
        pos = r.get("actual_pos") or r.get("position")
        try:
            return int(pos)
        except (TypeError, ValueError):
            return 999

    sorted_runners = sorted(runners, key=pos_key)
    lines = [f"\n{DIVIDER}\n📋 <b>Full result</b>"]

    for r in sorted_runners:
        name   = (r.get("horse") or "?")[:22]
        sp     = r.get("sp", "?")
        hid    = r.get("horse_id", "")
        status = runner_status_str(r)

        if hid and hid == a_id:
            marker = "  ⭐"
        elif hid and hid == b_id:
            marker = "  🔵"
        else:
            marker = ""

        lines.append(f"  {status:12} {name:22} {sp:6}{marker}")

    return "\n".join(lines)


# ── 4. End of Day Summary ─────────────────────────────────────────────────────

def format_end_of_day(
    date_str: str,
    analysed_races: list[dict],
    results: list[dict],
) -> str:
    """
    Full day summary sent ~90 minutes after last race.
    Covers tier breakdown, overall record, best/worst call.
    """
    lines = [
        f"{SCROLL} <b>END OF DAY — {date_str}</b>",
        THICK_DIV,
    ]

    result_map = {r.get("race_id"): r for r in results}
    paired     = [r for r in analysed_races if r.get("top1") and r.get("top2")]

    std_wins   = 0
    cons_wins  = 0
    total      = 0
    best_call  = None
    worst_miss = None
    best_score = -999
    worst_score = 999

    for race in paired:
        rid = race.get("race_id", "")
        res = result_map.get(rid, {})
        if not res:
            continue
        total    += 1
        std_won   = res.get("std_win", False)
        cons_won  = res.get("cons_win", False)
        score     = race.get("win_score", 0)

        if std_won:
            std_wins += 1
        if cons_won:
            cons_wins += 1

        if cons_won and score > best_score:
            best_score = score
            best_call  = race
        if not cons_won and score < worst_score:
            worst_score = score
            worst_miss  = race

    lines.append(f"\n{CHART} <b>Overall</b>")
    lines.append(
        f"Standard:     {std_wins}/{total} wins "
        f"({'%.0f' % (std_wins/total*100) if total else 0}%)"
    )
    lines.append(
        f"Conservative: {cons_wins}/{total} wins "
        f"({'%.0f' % (cons_wins/total*100) if total else 0}%)"
    )

    # Tier breakdown
    from predict_v2 import TIER_SUPREME, TIER_STRONG, TIER_GOOD
    tier_rows = [
        (TIER_SUPREME, "🔥🔥🔥 Supreme"),
        (TIER_STRONG,  "🔥🔥 Strong"),
        (TIER_GOOD,    "🔥 Good"),
    ]
    for t, label in tier_rows:
        t_races = [r for r in paired if r.get("tier") == t]
        t_wins  = sum(
            1 for r in t_races
            if result_map.get(r.get("race_id"), {}).get("cons_win")
        )
        if t_races:
            lines.append(f"  {label}: {t_wins}/{len(t_races)}")

    lines.append(DIVIDER)

    # ── Race by race ──────────────────────────────────────────────────────────
    lines.append(f"\n{RACE} <b>Race by race</b>")

    for race in sorted(paired, key=lambda r: r.get("off", "99:99")):
        rid  = race.get("race_id", "")
        res  = result_map.get(rid, {})
        std  = res.get("std_win")
        cons = res.get("cons_win")
        a    = race.get("top1") or {}
        b    = race.get("top2") or {}
        badge = TIER_BADGE.get(race.get("tier", TIER_STD), "·")
        a_pos = res.get("a_pos", "?")
        b_pos = res.get("b_pos", "?")

        if cons:
            icon = WIN
        elif cons is False:
            icon = LOSS
        else:
            icon = "⏳"

        def pos_str(p):
            try: return ordinal(int(p))
            except: return str(p) if p else "?"

        lines.append(
            f"\n{icon} {badge} {race.get('off','?')} {race.get('course','?')}\n"
            f"  {a.get('horse','?')} → {pos_str(a_pos)} | "
            f"{b.get('horse','?')} → {pos_str(b_pos)}"
        )

    lines.append(f"\n{DIVIDER}")

    if best_call:
        a = best_call.get("top1") or {}
        b = best_call.get("top2") or {}
        lines.append(
            f"\n{TROPHY} <b>Best call</b> — "
            f"{best_call.get('off','?')} {best_call.get('course','?')}\n"
            f"  {a.get('horse','?')} + {b.get('horse','?')}"
        )

    if worst_miss:
        lines.append(
            f"\n{LOSS} <b>Worst miss</b> — "
            f"{worst_miss.get('off','?')} {worst_miss.get('course','?')}"
        )

    lines.append(f"\n{THICK_DIV}")

    try:
        from notifications.streak_tracker import get_eod_summary
        lines.append(f"\n{get_eod_summary()}")
    except Exception:
        pass

    lines.append(f"\n{THICK_DIV}")
    lines.append("Good night. Tomorrow's card loads at midnight.")

    return "\n".join(lines)
