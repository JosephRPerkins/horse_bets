"""
fetch_data.py

Pulls racing data from The Racing API:
  1. Last N days of UK/Irish results (default 30), enriched with form/trainer/jockey
  2. Today's racecards (pro), enriched with the same pipeline

Enrichment per runner:
  - Last 10 races form history
  - Trainer 14-day win%, P/L, a/e (by distance analysis)
  - Jockey 14-day win%, P/L, a/e (by distance analysis)

Strategy to minimise API calls:
  - Fetch all results/cards for the date range first
  - Collect all unique horse/trainer/jockey IDs across ALL days + today's card
  - Fetch enrichment data in one upfront pass with caching
  - Apply cached data to all runners — no repeated calls

Usage:
    python fetch_data.py                   # 30 days + today's card
    python fetch_data.py --days 7          # 7 days + today's card
    python fetch_data.py --no-cards        # skip today's card
    python fetch_data.py --refetch-recent  # re-pull last 5 days (ratings fill in over time)
    python fetch_data.py --refetch-recent --refetch-days 7  # re-pull last 7 days
"""

import os
import json
import time
import argparse
from datetime import date, timedelta

import requests
from requests.auth import HTTPBasicAuth

import config

# ── Setup ─────────────────────────────────────────────────────────────────────
os.makedirs(config.DIR_RAW, exist_ok=True)
os.makedirs(config.DIR_CARDS, exist_ok=True)

auth    = HTTPBasicAuth(config.RACING_API_USERNAME, config.RACING_API_PASSWORD)
session = requests.Session()
session.auth = auth

SLEEP          = 0.6   # seconds between API calls — stays under 2 req/sec
FORM_LIMIT     = 10    # last N races to pull per horse

# ── Caches ────────────────────────────────────────────────────────────────────
horse_cache   = {}   # horse_id   → list of past result races
trainer_cache = {}   # trainer_id → stats dict
jockey_cache  = {}   # jockey_id  → stats dict


# ── Core API call ─────────────────────────────────────────────────────────────

def api_get(endpoint: str, params: dict = None) -> dict | None:
    url = f"{config.RACING_API_BASE_URL}/{endpoint.lstrip('/')}"
    try:
        r = session.get(url, params=params, timeout=15)
        if r.status_code == 422:
            return None
        r.raise_for_status()
        return r.json()
    except requests.exceptions.HTTPError:
        print(f"    ⚠ HTTP {r.status_code} — {endpoint}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"    ⚠ Request failed — {endpoint}: {e}")
        return None


# ── Step 1a: Fetch raw results for a date ─────────────────────────────────────

def fetch_results_for_date(race_date: str) -> list:
    all_results = []
    limit = 50
    skip  = 0

    while True:
        data = api_get("results", {
            "start_date": race_date,
            "end_date":   race_date,
            "region":     config.TARGET_REGIONS,
            "limit":      limit,
            "skip":       skip,
        })
        time.sleep(SLEEP)

        if not data:
            break

        batch = data.get("results") or []
        total = data.get("total", 0)
        all_results.extend(batch)
        print(f"    {len(all_results)}/{total} races...")

        if len(all_results) >= total or not batch:
            break

        skip += limit

    return all_results


# ── Step 1b: Fetch today's racecards (pro) ────────────────────────────────────

def fetch_cards_for_date(card_date: str) -> list:
    """
    Fetch pro racecards for a given date using /v1/racecards/pro.
    Returns a list of race dicts with runners already normalised to match
    the results runner shape (or/tsr/etc.).
    """
    all_cards = []
    limit = 50
    skip  = 0

    while True:
        data = api_get("racecards/pro", {
            "date":         card_date,
            "region_codes": config.TARGET_REGIONS,
            "limit":        limit,
            "skip":         skip,
        })
        time.sleep(SLEEP)

        if not data:
            break

        batch = data.get("racecards") or []
        total = data.get("total", 0)
        all_cards.extend(batch)
        print(f"    {len(all_cards)}/{total} races...")

        if len(all_cards) >= total or not batch:
            break

        skip += limit

    # Normalise runner fields to match results shape
    for race in all_cards:
        for runner in race.get("runners") or []:
            # Map racecard field names → results field names
            runner.setdefault("or",  runner.get("ofr", ""))
            runner.setdefault("tsr", runner.get("ts",  ""))
            # trainer_14_days from the card (runs/wins/percent) — keep as-is
            # our enrichment will add pl/ae on top

    return all_cards


# ── Step 2: Collect all unique IDs across days + cards ───────────────────────

def collect_ids(all_days: dict) -> tuple[set, set, set]:
    horse_ids   = set()
    trainer_ids = set()
    jockey_ids  = set()

    for races in all_days.values():
        for race in races:
            for runner in race.get("runners") or []:
                if runner.get("horse_id"):
                    horse_ids.add(runner["horse_id"])
                if runner.get("trainer_id"):
                    trainer_ids.add(runner["trainer_id"])
                if runner.get("jockey_id"):
                    jockey_ids.add(runner["jockey_id"])

    return horse_ids, trainer_ids, jockey_ids


# ── Step 3: Prefetch all enrichment data ─────────────────────────────────────

def prefetch_horse_form(horse_ids: set):
    """Fetch last FORM_LIMIT results for every unique horse. Skips cached."""
    needed = [h for h in horse_ids if h not in horse_cache]
    print(f"  Fetching form for {len(needed)} horses "
          f"({len(horse_ids) - len(needed)} cached)...")

    for i, horse_id in enumerate(needed, 1):
        data = api_get(f"racecards/{horse_id}/results", {"limit": FORM_LIMIT})
        time.sleep(SLEEP)

        horse_cache[horse_id] = data.get("results", []) if data else []

        if i % 50 == 0:
            print(f"    {i}/{len(needed)} horses done...")


def prefetch_trainer_stats(trainer_ids: set, end_date: str):
    """Fetch 14-day distance-analysis stats for every unique trainer."""
    needed = [t for t in trainer_ids if t not in trainer_cache]
    print(f"  Fetching stats for {len(needed)} trainers "
          f"({len(trainer_ids) - len(needed)} cached)...")

    start_date = (
        date.fromisoformat(end_date) - timedelta(days=14)
    ).strftime("%Y-%m-%d")

    for i, trainer_id in enumerate(needed, 1):
        data = api_get(f"trainers/{trainer_id}/analysis/distances", {
            "start_date": start_date,
            "end_date":   end_date,
        })
        time.sleep(SLEEP)

        if data:
            distances  = data.get("distances") or []
            total_runs = sum(d.get("runners", 0) for d in distances)
            total_wins = sum(d.get("1st", 0)     for d in distances)
            total_pl   = sum(d.get("1_pl", 0.0)  for d in distances)
            avg_ae     = (
                sum(d.get("a/e", 0.0) for d in distances) / len(distances)
                if distances else 0.0
            )
            trainer_cache[trainer_id] = {
                "runs":    total_runs,
                "wins":    total_wins,
                "win_pct": round(total_wins / total_runs, 3) if total_runs else 0.0,
                "pl":      round(total_pl, 2),
                "ae":      round(avg_ae, 2),
            }
        else:
            trainer_cache[trainer_id] = {}

        if i % 50 == 0:
            print(f"    {i}/{len(needed)} trainers done...")


def prefetch_jockey_stats(jockey_ids: set, end_date: str):
    """Fetch 14-day distance-analysis stats for every unique jockey."""
    needed = [j for j in jockey_ids if j not in jockey_cache]
    print(f"  Fetching stats for {len(needed)} jockeys "
          f"({len(jockey_ids) - len(needed)} cached)...")

    start_date = (
        date.fromisoformat(end_date) - timedelta(days=14)
    ).strftime("%Y-%m-%d")

    for i, jockey_id in enumerate(needed, 1):
        data = api_get(f"jockeys/{jockey_id}/analysis/distances", {
            "start_date": start_date,
            "end_date":   end_date,
        })
        time.sleep(SLEEP)

        if data:
            distances  = data.get("distances") or []
            total_runs = sum(d.get("rides", 0)  for d in distances)
            total_wins = sum(d.get("1st", 0)    for d in distances)
            total_pl   = sum(d.get("1_pl", 0.0) for d in distances)
            avg_ae     = (
                sum(d.get("a/e", 0.0) for d in distances) / len(distances)
                if distances else 0.0
            )
            jockey_cache[jockey_id] = {
                "runs":    total_runs,
                "wins":    total_wins,
                "win_pct": round(total_wins / total_runs, 3) if total_runs else 0.0,
                "pl":      round(total_pl, 2),
                "ae":      round(avg_ae, 2),
            }
        else:
            jockey_cache[jockey_id] = {}

        if i % 50 == 0:
            print(f"    {i}/{len(needed)} jockeys done...")


# ── Step 4: Derive form summary from cached horse history ─────────────────────

def derive_form(horse_id: str) -> dict:
    races     = horse_cache.get(horse_id, [])
    positions = []
    going_record = {}

    for race in races:
        runners = race.get("runners") or []
        runner  = next((r for r in runners if r.get("horse_id") == horse_id), None)
        if not runner:
            continue

        pos   = runner.get("position", "")
        going = race.get("going", "Unknown")
        positions.append(pos)

        going_record.setdefault(going, {"runs": 0, "wins": 0})
        going_record[going]["runs"] += 1
        if pos == "1":
            going_record[going]["wins"] += 1

    def is_placed(pos_str):
        try:
            return int(pos_str) <= 4
        except (ValueError, TypeError):
            return False

    bad_codes = {"P", "F", "U", "R", "PU", "BD", "RO", "UR"}
    last_4    = positions[-4:] if len(positions) >= 4 else positions
    last_3    = positions[-3:] if len(positions) >= 3 else positions

    return {
        "form_string":      "-".join(positions) if positions else "",
        "recent_positions": positions,
        "placed_last_4":    sum(1 for p in last_4 if is_placed(p)),
        "bad_recent":       sum(1 for p in last_3 if str(p).upper() in bad_codes),
        "going_record":     going_record,
    }


# ── Step 5: Apply cached enrichment to all runners ───────────────────────────

def apply_enrichment(races: list) -> list:
    enriched_races = []
    for race in races:
        enriched_runners = []
        for runner in race.get("runners") or []:
            horse_id   = runner.get("horse_id", "")
            trainer_id = runner.get("trainer_id", "")
            jockey_id  = runner.get("jockey_id", "")

            enriched_runners.append({
                **runner,
                "form_detail": derive_form(horse_id),
                "trainer_14d": trainer_cache.get(trainer_id, {}),
                "jockey_14d":  jockey_cache.get(jockey_id, {}),
            })

        enriched_races.append({**race, "runners": enriched_runners})

    return enriched_races


# ── Save helpers ──────────────────────────────────────────────────────────────

def save_results_day(race_date: str, races: list) -> str:
    filepath = os.path.join(config.DIR_RAW, f"{race_date}.json")
    with open(filepath, "w") as f:
        json.dump({
            "date":       race_date,
            "race_count": len(races),
            "results":    races,
        }, f, indent=2)
    return filepath


def save_cards_day(card_date: str, races: list) -> str:
    filepath = os.path.join(config.DIR_CARDS, f"{card_date}.json")
    with open(filepath, "w") as f:
        json.dump({
            "date":       card_date,
            "race_count": len(races),
            "racecards":  races,
        }, f, indent=2)
    return filepath


# ── Ratings quality check ─────────────────────────────────────────────────────

def ratings_coverage(races: list) -> float:
    """Return fraction of runners in these races that have a valid TSR or RPR."""
    total = rated = 0
    for race in races:
        for r in race.get("runners") or []:
            total += 1
            tsr = str(r.get("tsr") or "").strip()
            rpr = str(r.get("rpr") or "").strip()
            if (tsr and tsr not in ("", "–", "-")) or (rpr and rpr not in ("", "–", "-")):
                rated += 1
    return rated / total if total else 0.0


# ── Main ──────────────────────────────────────────────────────────────────────

def _run_fetch(dates: list[str], today_str: str, fetch_cards: bool) -> dict:
    """
    Core fetch + enrich logic. Returns dict of {date: enriched_races}.
    Separated so --refetch-recent can call it without re-parsing args.
    """
    # Phase 1: results
    all_days = {}
    for race_date in dates:
        print(f"  {race_date}")
        races = fetch_results_for_date(race_date)
        if races:
            all_days[race_date] = races
            cov = ratings_coverage(races)
            print(f"  ✓ {len(races)} races  (ratings coverage: {cov:.0%})")
        else:
            print(f"  — no results")
        time.sleep(1)

    # Phase 1b: today's card
    today_cards = []
    if fetch_cards:
        print()
        print(f"Phase 1b: Fetching today's racecards ({today_str})...")
        today_cards = fetch_cards_for_date(today_str)
        if today_cards:
            cov = ratings_coverage(today_cards)
            print(f"  ✓ {len(today_cards)} races on today's card  (ratings coverage: {cov:.0%})")
        else:
            print(f"  — no races found for today")

    if not all_days and not today_cards:
        return {}

    # Phase 2: unique IDs
    print()
    print("Collecting unique IDs...")
    combined = dict(all_days)
    if today_cards:
        combined[today_str + "_cards"] = today_cards

    horse_ids, trainer_ids, jockey_ids = collect_ids(combined)
    print(f"  {len(horse_ids)} horses | {len(trainer_ids)} trainers | {len(jockey_ids)} jockeys")

    end_date    = today_str
    total_calls = len(horse_ids) + len(trainer_ids) + len(jockey_ids)
    est_mins    = round(total_calls * SLEEP / 60, 1)
    print(f"  ~{total_calls} API calls needed (~{est_mins} min)")

    # Phase 3: enrichment
    print()
    print("Prefetching enrichment data...")
    prefetch_horse_form(horse_ids)
    prefetch_trainer_stats(trainer_ids, end_date)
    prefetch_jockey_stats(jockey_ids, end_date)

    # Phase 4: save results
    print()
    print("Applying enrichment and saving...")
    for race_date, races in all_days.items():
        enriched = apply_enrichment(races)
        filepath = save_results_day(race_date, enriched)
        cov = ratings_coverage(enriched)
        print(f"  ✓ {race_date} → {filepath}  ({cov:.0%} rated)")

    if today_cards:
        enriched_cards = apply_enrichment(today_cards)
        filepath = save_cards_day(today_str, enriched_cards)
        cov = ratings_coverage(enriched_cards)
        print(f"  ✓ {today_str} cards → {filepath}  ({cov:.0%} rated)")

    return all_days


def main():
    parser = argparse.ArgumentParser(description="Fetch racing data")
    parser.add_argument("--days",          type=int, default=30,
                        help="Days of historical results to fetch (default: 30)")
    parser.add_argument("--no-cards",      action="store_true",
                        help="Skip fetching today's racecards")
    parser.add_argument("--refetch-recent", action="store_true",
                        help="Re-fetch recent days to pick up ratings published after initial fetch")
    parser.add_argument("--refetch-days",  type=int, default=5,
                        help="How many recent days to re-fetch (default: 5, used with --refetch-recent)")
    args  = parser.parse_args()
    today = date.today()
    today_str = today.strftime("%Y-%m-%d")

    if args.refetch_recent:
        # Re-pull the N most recent completed days to pick up delayed ratings
        refetch_dates = [
            (today - timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range(1, args.refetch_days + 1)
        ]
        print(f"Racing Data Fetch — REFETCH RECENT ({args.refetch_days} days)")
        print(f"  Dates: {refetch_dates[-1]} → {refetch_dates[0]}")
        print(f"  Ratings are published with a delay — re-fetching to capture them.")
        print()

        # Show current coverage before refetch
        print("Current ratings coverage:")
        for d in sorted(refetch_dates):
            path = os.path.join(config.DIR_RAW, f"{d}.json")
            if os.path.exists(path):
                with open(path) as f:
                    existing = json.load(f)
                cov = ratings_coverage(existing.get("results", []))
                print(f"  {d}: {cov:.0%}")
            else:
                print(f"  {d}: not yet fetched")
        print()

        print("Phase 1: Re-fetching results...")
        _run_fetch(sorted(refetch_dates), today_str, fetch_cards=False)

        print()
        print("Updated ratings coverage:")
        for d in sorted(refetch_dates):
            path = os.path.join(config.DIR_RAW, f"{d}.json")
            if os.path.exists(path):
                with open(path) as f:
                    updated = json.load(f)
                cov = ratings_coverage(updated.get("results", []))
                print(f"  {d}: {cov:.0%}")
        print()
        print("Done. Re-run predict_v2.py to see updated predictions.")
        return

    # Normal full fetch
    dates = [
        (today - timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(args.days, 0, -1)
    ]

    print(f"Racing Data Fetch")
    print(f"  Historical: {args.days} days  ({dates[0]} → {dates[-1]})")
    print(f"  Today's card: {'yes' if not args.no_cards else 'no'} ({today_str})")
    print()

    print("Phase 1: Fetching historical results...")
    all_days = _run_fetch(dates, today_str, fetch_cards=not args.no_cards)

    if not all_days:
        print("No data found. Exiting.")
        return

    print()
    print("Done.")
    total_results = sum(len(v) for v in all_days.values())
    print(f"  {len(all_days)} days | {total_results} result races")
    print(f"  Cache: {len(horse_cache)} horses | {len(trainer_cache)} trainers | "
          f"{len(jockey_cache)} jockeys")


if __name__ == "__main__":
    main()
