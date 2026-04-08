"""
core/api_client.py

Single wrapper around The Racing API (Pro plan).
All HTTP calls go through here — nothing else touches requests directly.
Rate limiting enforced internally at 0.6s per call.
"""

import time
import logging
import requests
from datetime import date, timedelta
from requests.auth import HTTPBasicAuth

import config

logger = logging.getLogger(__name__)

SLEEP = 0.6  # seconds between calls — stays under 2 req/sec


class RacingAPIClient:

    def __init__(self):
        self.auth    = HTTPBasicAuth(config.RACING_API_USERNAME, config.RACING_API_PASSWORD)
        self.base    = config.RACING_API_BASE_URL.rstrip("/")
        self.session = requests.Session()
        self.session.auth = self.auth

    # ── Internal ──────────────────────────────────────────────────────────────

    def _get(self, endpoint: str, params: dict = None) -> dict | None:
        url = f"{self.base}/{endpoint.lstrip('/')}"
        try:
            r = self.session.get(url, params=params, timeout=15)
            if r.status_code == 422:
                return None  # No data for this entity — silent skip
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError:
            if r.status_code == 404:
                logger.debug(f"HTTP 404 — {endpoint} (not yet available)")
            else:
                logger.warning(f"HTTP {r.status_code} — {endpoint}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed — {endpoint}: {e}")
            return None

    def _get_paged(self, endpoint: str, params: dict,
                   result_key: str = "results") -> list:
        """Fetch all pages of a paginated endpoint automatically."""
        all_items = []
        limit = 50
        skip  = 0
        while True:
            data = self._get(endpoint, {**params, "limit": limit, "skip": skip})
            time.sleep(SLEEP)
            if not data:
                break
            batch = data.get(result_key) or []
            total = data.get("total", 0)
            all_items.extend(batch)
            if len(all_items) >= total or not batch:
                break
            skip += limit
        return all_items

    # ── Racecards ─────────────────────────────────────────────────────────────

    def get_todays_racecards(self, region_codes: list = None) -> list:
        """
        Standard racecard for today.
        Includes OR, TSR, RPR, trainer_14_days, form, going.
        Uses the same endpoint and params as v1 (known working).
        """
        params = {"day": "today"}
        if region_codes:
            params["region_codes"] = region_codes
        data = self._get("racecards/standard", params)
        time.sleep(SLEEP)
        return data.get("racecards", []) if data else []

    def get_tomorrows_racecards(self, region_codes: list = None) -> list:
        """Standard racecard for tomorrow — used by midnight pre-fetch job."""
        params = {"day": "tomorrow"}
        if region_codes:
            params["region_codes"] = region_codes
        data = self._get("racecards/standard", params)
        time.sleep(SLEEP)
        return data.get("racecards", []) if data else []

    def get_race_racecard(self, race_id: str) -> dict | None:
        """
        Attempt a pro racecard for a single race (T-10min live odds refresh).
        Falls back to None if pro endpoint unavailable — pre-race alert
        will use the cached standard data instead.
        """
        data = self._get(f"racecards/pro/{race_id}")
        if data is None:
            # Pro endpoint not available on this plan — try standard
            data = self._get(f"racecards/{race_id}")
        time.sleep(SLEEP)
        return data

    # ── Results ───────────────────────────────────────────────────────────────

    def get_results_today(self, region_codes: list = None) -> list:
        """All results confirmed so far today."""
        params = {}
        if region_codes:
            params["region"] = region_codes
        return self._get_paged("results/today", params, "results")

    def get_result_by_race_id(self, race_id: str) -> dict | None:
        """Single race result by race_id. Used for targeted result polling."""
        data = self._get(f"results/{race_id}")
        time.sleep(SLEEP)
        return data

    def get_results_by_date(self, race_date: str,
                             region_codes: list = None) -> list:
        """Historical results for a specific date."""
        params = {"start_date": race_date, "end_date": race_date}
        if region_codes:
            params["region"] = region_codes
        return self._get_paged("results", params, "results")

    # ── Horse / Jockey / Trainer enrichment ──────────────────────────────────

    def get_horse_form(self, horse_id: str, limit: int = 6) -> list:
        """Last N race results for a horse."""
        data = self._get(f"horses/{horse_id}/results", {"limit": limit})
        time.sleep(SLEEP)
        return data.get("results", []) if data else []

    def get_trainer_stats_14d(self, trainer_id: str, end_date: str = None) -> dict:
        """Trainer win%, P/L, a/e over the 14 days ending on end_date."""
        if end_date is None:
            end_date = date.today().strftime("%Y-%m-%d")
        start_date = (
            date.fromisoformat(end_date) - timedelta(days=14)
        ).strftime("%Y-%m-%d")

        data = self._get(f"trainers/{trainer_id}/analysis/distances", {
            "start_date": start_date,
            "end_date":   end_date,
        })
        time.sleep(SLEEP)

        if not data:
            return {}

        distances  = data.get("distances") or []
        total_runs = sum(d.get("runners", 0) for d in distances)
        total_wins = sum(d.get("1st", 0) for d in distances)
        total_pl   = sum(d.get("1_pl", 0.0) for d in distances)
        avg_ae     = (
            sum(d.get("a/e", 0.0) for d in distances) / len(distances)
            if distances else 0.0
        )
        return {
            "runs":    total_runs,
            "wins":    total_wins,
            "win_pct": round(total_wins / total_runs, 3) if total_runs else 0.0,
            "pl":      round(total_pl, 2),
            "ae":      round(avg_ae, 2),
        }

    def get_jockey_stats_14d(self, jockey_id: str, end_date: str = None) -> dict:
        """Jockey win%, P/L, a/e over the 14 days ending on end_date."""
        if end_date is None:
            end_date = date.today().strftime("%Y-%m-%d")
        start_date = (
            date.fromisoformat(end_date) - timedelta(days=14)
        ).strftime("%Y-%m-%d")

        data = self._get(f"jockeys/{jockey_id}/analysis/distances", {
            "start_date": start_date,
            "end_date":   end_date,
        })
        time.sleep(SLEEP)

        if not data:
            return {}

        distances  = data.get("distances") or []
        total_runs = sum(d.get("rides", 0) for d in distances)
        total_wins = sum(d.get("1st", 0) for d in distances)
        total_pl   = sum(d.get("1_pl", 0.0) for d in distances)
        avg_ae     = (
            sum(d.get("a/e", 0.0) for d in distances) / len(distances)
            if distances else 0.0
        )
        return {
            "runs":    total_runs,
            "wins":    total_wins,
            "win_pct": round(total_wins / total_runs, 3) if total_runs else 0.0,
            "pl":      round(total_pl, 2),
            "ae":      round(avg_ae, 2),
        }

    # ── Bulk enrichment ───────────────────────────────────────────────────────

    def enrich_runners(self, races: list, end_date: str = None) -> list:
        """
        For every runner in every race, attach:
          - form_detail  (from horse form history)
          - trainer_14d  (14-day trainer stats)
          - jockey_14d   (14-day jockey stats)
        Uses caching so each unique horse/trainer/jockey is only fetched once.
        """
        if end_date is None:
            end_date = date.today().strftime("%Y-%m-%d")

        horse_cache   = {}
        trainer_cache = {}
        jockey_cache  = {}

        horse_ids   = set()
        trainer_ids = set()
        jockey_ids  = set()
        for race in races:
            for r in race.get("runners", []):
                if r.get("horse_id"):   horse_ids.add(r["horse_id"])
                if r.get("trainer_id"): trainer_ids.add(r["trainer_id"])
                if r.get("jockey_id"):  jockey_ids.add(r["jockey_id"])

        logger.info(f"Enriching: {len(horse_ids)} horses, "
                    f"{len(trainer_ids)} trainers, {len(jockey_ids)} jockeys")

        for horse_id in horse_ids:
            results = self.get_horse_form(horse_id)
            horse_cache[horse_id] = self._derive_form(horse_id, results)

        for trainer_id in trainer_ids:
            trainer_cache[trainer_id] = self.get_trainer_stats_14d(
                trainer_id, end_date
            )

        for jockey_id in jockey_ids:
            jockey_cache[jockey_id] = self.get_jockey_stats_14d(
                jockey_id, end_date
            )

        # Apply to all runners
        enriched = []
        for race in races:
            enriched_runners = []
            for runner in race.get("runners", []):
                norm = self.normalise_runner(runner)
                hid  = norm.get("horse_id", "")
                tid  = norm.get("trainer_id", "")
                jid  = norm.get("jockey_id", "")

                horse_form = horse_cache.get(hid)
                if horse_form and horse_form.get("recent_positions"):
                    norm["form_detail"] = horse_form

                trainer_stats = trainer_cache.get(tid)
                if trainer_stats and trainer_stats.get("runs", 0) > 0:
                    norm["trainer_14d"] = trainer_stats

                jockey_stats = jockey_cache.get(jid)
                if jockey_stats:
                    norm["jockey_14d"] = jockey_stats

                enriched_runners.append(norm)
            enriched.append({**race, "runners": enriched_runners})

        return enriched

    @staticmethod
    def normalise_runner(runner: dict) -> dict:
        """
        Map racecard API field names to the field names our scorer expects.
        Extracts Bet365 odds, Betfair odds, and exchange odds separately.
        """
        from utils.helpers import safe_float

        # Extract odds by bookmaker
        sp_dec      = None
        sp_frac     = None
        bf_sp_dec   = None   # Betfair SP
        bf_sp_frac  = None

        odds_list = runner.get("odds") or []
        if odds_list:
            for book in odds_list:
                bm   = (book.get("bookmaker") or "").lower()
                frac = book.get("fractional", "")
                dec  = safe_float(book.get("decimal"))

                if bm == "bet365" and not sp_dec:
                    if dec and dec > 1.0 and frac != "SP":
                        sp_dec  = dec
                        sp_frac = frac

                if bm in ("betfair", "betfair sp") and not bf_sp_dec:
                    if dec and dec > 1.0:
                        bf_sp_dec  = dec
                        bf_sp_frac = frac

            # Fall back to best available if no Bet365
            if not sp_dec:
                for book in odds_list:
                    frac = book.get("fractional", "")
                    if frac == "SP":
                        continue
                    dec = safe_float(book.get("decimal"))
                    if dec and dec > 1.0:
                        if sp_dec is None or dec < sp_dec:
                            sp_dec  = dec
                            sp_frac = frac

        or_val  = runner.get("ofr") or runner.get("or")
        tsr_val = runner.get("ts")  or runner.get("tsr")

        t14_raw = runner.get("trainer_14_days") or runner.get("trainer_14d") or {}
        t_runs  = safe_float(t14_raw.get("runs"), 0)
        t_wins  = safe_float(t14_raw.get("wins"), 0)
        t_pct   = safe_float(t14_raw.get("percent"), 0)
        if t_pct and t_pct > 1:
            t_pct = t_pct / 100
        elif t_runs and t_wins:
            t_pct = t_wins / t_runs
        trainer_14d = {
            "runs":    int(t_runs or 0),
            "wins":    int(t_wins or 0),
            "win_pct": round(t_pct or 0, 3),
            "pl":      0.0,
            "ae":      0.0,
        }

        jockey_14d  = runner.get("jockey_14d") or {}
        form_str    = runner.get("form") or ""
        form_detail = RacingAPIClient._parse_form_string(form_str)

        return {
            **runner,
            "or":          or_val,
            "tsr":         tsr_val,
            "sp":          sp_frac or "",
            "sp_dec":      sp_dec,
            "bf_sp":       bf_sp_frac or "",
            "bf_sp_dec":   bf_sp_dec,
            "trainer_14d": trainer_14d,
            "jockey_14d":  jockey_14d,
            "form_detail": form_detail,
        }

    @staticmethod
    def _parse_form_string(form_str: str) -> dict:
        """Build a form_detail dict from a form string like '1-2-1-P-3'."""
        bad_codes = {"P", "F", "U", "R", "PU", "BD", "RO", "UR"}
        if not form_str:
            return {
                "form_string":      "",
                "recent_positions": [],
                "placed_last_4":    0,
                "bad_recent":       0,
                "going_record":     {},
            }
        import re
        parts = re.split(r"[-/\s]", form_str.strip())
        parts = [p.strip() for p in parts if p.strip()]

        def is_placed(p):
            try: return int(p) <= 4
            except ValueError: return False

        last_4 = parts[-4:] if len(parts) >= 4 else parts
        last_3 = parts[-3:] if len(parts) >= 3 else parts

        return {
            "form_string":      form_str,
            "recent_positions": parts,
            "placed_last_4":    sum(1 for p in last_4 if is_placed(p)),
            "bad_recent":       sum(1 for p in last_3 if p.upper() in bad_codes),
            "going_record":     {},
        }

    @staticmethod
    def _derive_form(horse_id: str, races: list) -> dict:
        """Build form_detail dict from a horse's last N results."""
        positions    = []
        going_record = {}
        bad_codes    = {"P", "F", "U", "R", "PU", "BD", "RO", "UR"}

        for race in races:
            runners = race.get("runners") or []
            runner  = next(
                (r for r in runners if r.get("horse_id") == horse_id), None
            )
            if not runner:
                continue
            pos   = runner.get("position", "")
            going = race.get("going", "Unknown")
            positions.append(pos)
            if going not in going_record:
                going_record[going] = {"runs": 0, "wins": 0}
            going_record[going]["runs"] += 1
            if pos == "1":
                going_record[going]["wins"] += 1

        def is_placed(p):
            try: return int(p) <= 4
            except (ValueError, TypeError): return False

        last_4 = positions[-4:] if len(positions) >= 4 else positions
        last_3 = positions[-3:] if len(positions) >= 3 else positions

        return {
            "form_string":      "-".join(positions) if positions else "",
            "recent_positions": positions,
            "placed_last_4":    sum(1 for p in last_4 if is_placed(p)),
            "bad_recent":       sum(1 for p in last_3 if str(p).upper() in bad_codes),
            "going_record":     going_record,
        }
