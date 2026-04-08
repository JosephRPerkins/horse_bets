"""
utils/helpers.py

Small reusable utility functions used across the entire system.
No dependencies on other project modules.
"""

import re
from datetime import datetime


# ── Type-safe converters ──────────────────────────────────────────────────────

def safe_float(val, default=None):
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def safe_int(val, default=None):
    try:
        return int(val)
    except (TypeError, ValueError):
        return default


# ── SP / odds helpers ─────────────────────────────────────────────────────────

def resolve_sp(runner: dict) -> float | None:
    """
    Return decimal SP for a runner.
    Parses sp string first, falls back to sp_dec field, then BSP.
    Handles: '9/4', '9/4F', '9/4J', 'Evens', 'EVS', '100/30'
    """
    sp_str = (runner.get("sp") or "").strip().rstrip("FfJj").strip()
    if sp_str.lower() in ("evs", "evens"):
        return 2.0
    if "/" in sp_str:
        parts = sp_str.split("/")
        try:
            num, den = float(parts[0]), float(parts[1])
            if den > 0:
                return round(1 + num / den, 3)
        except (ValueError, ZeroDivisionError):
            pass
    v = safe_float(runner.get("sp_dec"))
    if v:
        return v
    v = safe_float(runner.get("bsp"))
    return round(v, 3) if v else None


def dec_to_fractional(dec: float) -> str:
    """Convert decimal odds to a readable fractional string."""
    if dec is None:
        return "N/A"
    frac = dec - 1
    targets = [
        (0.5, "1/2"), (0.67, "2/3"), (1.0, "Evens"),
        (1.25, "5/4"), (1.5, "6/4"), (1.67, "5/3"),
        (2.0, "2/1"), (2.5, "5/2"), (3.0, "3/1"),
        (4.0, "4/1"), (5.0, "5/1"), (6.0, "6/1"),
        (7.0, "7/1"), (8.0, "8/1"), (10.0, "10/1"),
    ]
    closest = min(targets, key=lambda x: abs(x[0] - frac))
    if abs(closest[0] - frac) < 0.12:
        return closest[1]
    return f"{frac:.1f}/1"


# ── Race classification helpers ───────────────────────────────────────────────

def race_type_key(race_type: str) -> str:
    """Normalise race type to one of: flat, hurdle, chase, nh flat"""
    rt = (race_type or "").lower()
    if "nh flat" in rt or "bumper" in rt:
        return "nh flat"
    if "chase" in rt:
        return "chase"
    if "hurdle" in rt:
        return "hurdle"
    return "flat"


GOING_MAP = {
    "heavy":            "Heavy",
    "soft to heavy":    "Heavy",
    "soft":             "Soft",
    "good to soft":     "Good to Soft",
    "good to yielding": "Good to Soft",
    "yielding to soft": "Soft",
    "yielding":         "Good to Soft",
    "good to firm":     "Good to Firm",
    "firm":             "Firm",
    "standard to slow": "All Weather",
    "standard to fast": "All Weather",
    "standard":         "All Weather",
    "good":             "Good",
    "fast":             "Firm",
}

def normalise_going(going_str: str) -> str:
    """Match longest key first so 'good to soft' beats 'soft'."""
    g = (going_str or "").lower().strip()
    for k in sorted(GOING_MAP, key=len, reverse=True):
        if k in g:
            return GOING_MAP[k]
    return going_str or "Unknown"


def dist_furlongs(dist_str: str) -> float | None:
    """Extract distance in furlongs from strings like '2m4f', '7f', '1m½f', '2m4½f'"""
    if not dist_str:
        return None
    d = str(dist_str).lower()
    # Miles + furlongs combined e.g. '2m4f', '2m½f', '2m4½f'
    m = re.search(r"(\d+)m(\d+\.?\d*)?([½¼]?)f?", d)
    if m:
        miles    = float(m.group(1))
        furlongs = float(m.group(2)) if m.group(2) else 0.0
        fraction = 0.5 if "½" in d else (0.25 if "¼" in d else 0.0)
        return miles * 8 + furlongs + fraction
    # Pure furlongs e.g. '7f'
    m2 = re.search(r"(\d+\.?\d*)f", d)
    if m2:
        return float(m2.group(1))
    return None


def dist_label(f: float | None) -> str:
    if f is None:   return "Unknown"
    if f < 7:       return "Sprint (<7f)"
    if f < 10:      return "Mile (7f–9f)"
    if f < 14:      return "10f–13f"
    if f < 20:      return "14f–2m4f"
    return                  "2m5f+"


def field_label(n: int) -> str:
    if n <= 5:  return "Tiny (≤5)"
    if n <= 7:  return "Small (6–7)"
    if n <= 9:  return "8–9"
    if n <= 11: return "10–11"
    if n <= 14: return "12–14"
    return              "15+"


def sp_band_label(sp: float | None) -> str:
    if sp is None:   return "Unknown"
    if sp < 2.0:     return "Odds-on"
    if sp < 4.0:     return "2/1–3/1"
    if sp < 6.0:     return "4/1–5/1"
    if sp < 10.0:    return "6/1–9/1"
    if sp < 16.0:    return "10/1–15/1"
    return                   "16/1+"


# ── Derive surface ─────────────────────────────────────────────────────────────

AW_TRACKS = {
    "wolverhampton", "kempton", "lingfield", "chelmsford",
    "southwell", "newcastle", "dundalk",
}

def derive_surface(race: dict) -> str:
    """
    Return 'AW' or 'Turf' based on going, surface field, or known track names.
    """
    surface = (race.get("surface") or "").strip()
    if surface:
        return surface if surface in ("AW", "Turf") else (
            "AW" if surface.lower() in ("artificial", "all weather", "polytrack",
                                        "tapeta", "fibresand") else "Turf"
        )
    going = (race.get("going") or "").lower()
    if any(kw in going for kw in ("standard", "all weather", "polytrack", "tapeta")):
        return "AW"
    course = (race.get("course") or "").lower()
    if any(track in course for track in AW_TRACKS):
        return "AW"
    return "Turf"


# ── Time helpers ──────────────────────────────────────────────────────────────

def parse_off_time(off_str: str, race_date: str) -> datetime | None:
    """
    Parse an off time string like '14:30' and date 'YYYY-MM-DD'
    into a datetime object.
    """
    if not off_str or not race_date:
        return None
    try:
        clean = off_str.strip().replace(".", ":")
        parts = clean.split(":")
        hour  = int(parts[0])
        mins  = int(parts[1]) if len(parts) > 1 else 0
        return datetime.strptime(race_date, "%Y-%m-%d").replace(
            hour=hour, minute=mins, second=0, microsecond=0
        )
    except (ValueError, IndexError):
        return None


def format_time_until(dt: datetime) -> str:
    """Human readable time until a datetime, e.g. '1h 23m' or '8m'"""
    now   = datetime.now()
    delta = dt - now
    secs  = int(delta.total_seconds())
    if secs <= 0:
        return "now"
    if secs < 3600:
        return f"{secs // 60}m"
    hours = secs // 3600
    mins  = (secs % 3600) // 60
    return f"{hours}h {mins}m" if mins else f"{hours}h"


# ── Form helpers ──────────────────────────────────────────────────────────────

BAD_FORM_CODES = {"P", "F", "U", "R", "PU", "BD", "RO", "UR"}

def form_confidence(runners: list) -> tuple[str, float]:
    """
    Returns (label, pct) for how much form data exists in a race.
    label: 'high' | 'medium' | 'low' | 'none'
    """
    if not runners:
        return "none", 0.0
    with_form = sum(
        1 for r in runners
        if (r.get("form_detail") or {}).get("recent_positions")
    )
    pct = with_form / len(runners)
    if pct >= 0.6:   return "high",   round(pct, 2)
    if pct >= 0.3:   return "medium", round(pct, 2)
    if pct > 0:      return "low",    round(pct, 2)
    return "none", 0.0


# ── Incident code detection ───────────────────────────────────────────────────

NON_RUNNER_CODES = {"NR"}
INCIDENT_CODES   = {"PU", "F", "U", "UR", "R", "BD", "RO", "REF", "SU", "CO", "WO"}

INCIDENT_LABELS = {
    "PU": "Pulled up",
    "F":  "Fell",
    "U":  "Unseated",
    "UR": "Unseated",
    "R":  "Refused",
    "BD": "Brought down",
    "RO": "Ran out",
    "REF": "Refused",
    "SU": "Slipped up",
    "CO": "Carried out",
    "WO": "Walked over",
    "NR": "Non-runner",
}

def runner_status_str(runner: dict) -> str:
    """
    Return a human-readable finishing status:
    '1st', '2nd', 'Fell', 'Pulled up', 'Non-runner', etc.

    Checks both 'position' and 'actual_pos', preferring whichever
    contains an incident code (non-numeric) over a numeric value,
    so Unseated/PU/Fell aren't misread as Non-runner.
    """
    if runner.get("status", "").lower() in ("non-runner", "nonrunner", "nr"):
        return "Non-runner"

    raw_pos  = str(runner.get("position") or "").strip().upper()
    raw_act  = str(runner.get("actual_pos") or "").strip().upper()

    # Prefer whichever field contains a known incident code
    pos = raw_pos
    if raw_pos in ("", "0", "NONE") and raw_act:
        pos = raw_act
    if pos not in INCIDENT_CODES and raw_pos in INCIDENT_CODES:
        pos = raw_pos
    if pos not in INCIDENT_CODES and raw_act in INCIDENT_CODES:
        pos = raw_act

    if pos in INCIDENT_CODES:
        return INCIDENT_LABELS.get(pos, pos)
    if pos in NON_RUNNER_CODES or pos in ("0", "", "NONE"):
        return "Non-runner"
    try:
        return ordinal(int(pos))
    except (TypeError, ValueError):
        return pos or "?"


def is_non_runner(runner: dict) -> bool:
    status = (runner.get("status") or "").lower()
    if any(s in status for s in ("non-runner", "nonrunner", "nr", "non runner")):
        return True
    pos = str(runner.get("position") or "").strip().upper()
    return pos in NON_RUNNER_CODES


# ── Misc ──────────────────────────────────────────────────────────────────────

def ordinal(n: int) -> str:
    """Return '1st', '2nd', '3rd', '4th' etc."""
    if 11 <= (n % 100) <= 13:
        return f"{n}th"
    return f"{n}{['th','st','nd','rd','th'][min(n % 10, 4)]}"


def stars(n: int) -> str:
    """Return star emoji string for a rating 1–5."""
    return "⭐" * max(1, min(5, n))
