"""
betfair/api.py

Betfair Exchange API wrapper for horse_bets_v3.
Handles: login, account balance, market discovery, live odds, bet placement.
Uses betfairlightweight under the hood.

All public functions log errors and return safe defaults on failure —
callers should always check for None / empty dict returns.
"""

import logging
import re
import os
import sys
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import config

logger = logging.getLogger("betfair.api")

COMMISSION        = 0.05   # 5% on winnings
MATCH_TOLERANCE_S = 180    # seconds tolerance when matching race start time

_client = None


# ── Login / session ───────────────────────────────────────────────────────────

def get_client():
    """Return (and re-login) the betfairlightweight client."""
    global _client
    import betfairlightweight
    if _client is None:
        certs = getattr(config, "BETFAIR_CERTS_DIR_SERVER", None)
        if not certs or not os.path.isdir(certs):
            certs = getattr(config, "BETFAIR_CERTS_DIR_MAC", None)
        _client = betfairlightweight.APIClient(
            username = config.BETFAIR_USERNAME,
            password = config.BETFAIR_PASSWORD,
            app_key  = config.BETFAIR_APP_KEY,
            certs    = certs,
        )
    try:
        _client.login()
    except Exception as e:
        logger.warning(f"Betfair re-login: {e}")
    return _client


def get_balance() -> float:
    try:
        funds = get_client().account.get_account_funds()
        return round(funds.available_to_bet_balance, 2)
    except Exception as e:
        logger.error(f"get_balance failed: {e}")
        return 0.0


# ── Name / time normalisation ─────────────────────────────────────────────────

def _norm_horse(name: str) -> str:
    name = re.sub(r"\s*\([^)]*\)", "", name)
    name = re.sub(r"[^a-z0-9 ]", "", name.lower())
    return re.sub(r"\s+", " ", name).strip()


def _norm_course(name: str) -> str:
    name = name.lower()
    name = re.sub(r"\s*\(.*?\)", "", name)
    name = re.sub(r"[-\s]+", " ", name).strip()
    aliases = {
        "kempton park":    "kempton",
        "chelmsford city": "chelmsford",
        "lingfield park":  "lingfield",
    }
    return aliases.get(name, name)


def _to_utc(val) -> datetime | None:
    if val is None:
        return None
    if isinstance(val, str):
        try:
            dt = datetime.fromisoformat(val)
        except ValueError:
            return None
    else:
        dt = val
    if not isinstance(dt, datetime):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _to_local_naive(utc_dt: datetime) -> datetime:
    return utc_dt.astimezone().replace(tzinfo=None)


# ── Market discovery ──────────────────────────────────────────────────────────

def find_win_market(race: dict):
    """
    Find the Betfair WIN market for a race.
    Returns (MarketCatalogue, diff_seconds) or (None, None).
    """
    from betfairlightweight.filters import market_filter, time_range

    off_dt = _to_utc(race.get("off_dt"))
    if off_dt is None:
        logger.warning(f"find_win_market: no off_dt for {race.get('course')}")
        return None, None

    course = _norm_course(race.get("course", ""))
    from_dt = off_dt - timedelta(minutes=10)
    to_dt   = off_dt + timedelta(minutes=10)

    try:
        markets = get_client().betting.list_market_catalogue(
            filter=market_filter(
                event_type_ids    = ["7"],
                market_countries  = ["GB", "IE"],
                market_type_codes = ["WIN"],
                market_start_time = time_range(
                    from_=from_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    to   =to_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                ),
            ),
            market_projection = ["MARKET_START_TIME", "RUNNER_DESCRIPTION", "EVENT"],
            max_results       = 20,
        )
    except Exception as e:
        logger.error(f"find_win_market catalogue error: {e}")
        return None, None

    best_mkt   = None
    best_diff  = None
    best_score = -1

    for mkt in markets:
        bf_time   = _to_utc(mkt.market_start_time)
        bf_event  = getattr(getattr(mkt, "event", None), "name", "") or ""
        bf_course = _norm_course(bf_event)

        if bf_time is None:
            continue

        diff      = abs((off_dt - bf_time).total_seconds())
        time_ok   = diff <= MATCH_TOLERANCE_S
        course_ok = (course in bf_course or bf_course in course or
                     course.split()[0] in bf_course)

        score = int(time_ok) * 2 + int(course_ok)
        if score > best_score or (score == best_score and diff < (best_diff or 999)):
            best_score = score
            best_mkt   = mkt
            best_diff  = diff

    if best_score < 2:
        logger.warning(
            f"find_win_market: no confident match for "
            f"{race.get('off')} {race.get('course')} (best_score={best_score})"
        )
        return None, None

    return best_mkt, best_diff


def get_market_odds(market_id: str) -> dict:
    """
    Return {selection_id: {back, back_size, lay, lay_size, status}} for all runners.
    """
    try:
        books = get_client().betting.list_market_book(
            market_ids       = [market_id],
            price_projection = {
                "priceData":             ["EX_BEST_OFFERS"],
                "exBestOffersOverrides": {"bestPricesDepth": 5},
            },
        )
    except Exception as e:
        logger.error(f"get_market_odds error: {e}")
        return {}

    result = {}
    if books:
        for r in books[0].runners:
            back      = r.ex.available_to_back or []
            lay       = r.ex.available_to_lay  or []
            best_back = back[0].price if back else None
            best_size = round(sum(p.size for p in back if p.price == best_back), 2) if back else 0.0
            best_lay  = lay[0].price  if lay  else None
            lay_size  = round(sum(p.size for p in lay  if p.price == best_lay),  2) if lay  else 0.0
            result[r.selection_id] = {
                "back":      best_back,
                "back_size": best_size,
                "lay":       best_lay,
                "lay_size":  lay_size,
                "status":    r.status,
            }
    return result


def find_selection_id(horse_name: str, bf_runners: list) -> int | None:
    """Fuzzy-match a horse name to a Betfair selection ID."""
    norm = _norm_horse(horse_name)
    for r in bf_runners:
        bf = _norm_horse(getattr(r, "runner_name", ""))
        if norm == bf or norm in bf or bf in norm:
            return r.selection_id
    return None


# ── Bet placement ─────────────────────────────────────────────────────────────

def _round_betfair_price(price: float) -> float:
    """Round DOWN to the nearest valid Betfair increment."""
    def floor_to(p, step):
        return round(round(p / step, 8) // 1 * step, 2)

    if price <= 2.0:     return round(floor_to(price, 0.01), 2)
    elif price <= 3.0:   return round(floor_to(price, 0.02), 2)
    elif price <= 4.0:   return round(floor_to(price, 0.05), 2)
    elif price <= 6.0:   return round(floor_to(price, 0.1),  2)
    elif price <= 10.0:  return round(floor_to(price, 0.2),  2)
    elif price <= 20.0:  return round(floor_to(price, 0.5),  2)
    elif price <= 30.0:  return round(floor_to(price, 1.0),  2)
    elif price <= 50.0:  return round(floor_to(price, 2.0),  2)
    elif price <= 100.0: return round(floor_to(price, 5.0),  2)
    else:                return round(floor_to(price, 10.0), 2)


def place_back(market_id: str, selection_id: int,
               price: float, stake: float) -> dict | None:
    """
    Place a back bet on the exchange.
    Returns bet dict on success, None on failure.
    bet dict keys: type, selection_id, price, size, size_matched, bet_id, pending
    """
    # No artificial minimum enforced here — caller controls stake via liquidity
    # check. If Betfair rejects it as below their minimum, the error is logged.
    valid_price = _round_betfair_price(price)

    try:
        result = get_client().betting.place_orders(
            market_id    = market_id,
            instructions = [{
                "selectionId": selection_id,
                "side":        "BACK",
                "orderType":   "LIMIT",
                "limitOrder": {
                    "size":            stake,
                    "price":           valid_price,
                    "persistenceType": "LAPSE",
                },
            }],
        )

        if result and result.status == "SUCCESS":
            order        = result.place_instruction_reports[0]
            size_matched = getattr(order, "size_matched", 0.0) or 0.0
            avg_price    = getattr(order, "average_price_matched", None) or valid_price
            bet_id       = getattr(order, "bet_id", "")

            if size_matched <= 0:
                logger.info(f"Back order resting in queue — bet_id={bet_id} @ {valid_price} £{stake}")
                return {
                    "type":         "BACK",
                    "selection_id": selection_id,
                    "price":        valid_price,
                    "size":         stake,
                    "size_matched": 0.0,
                    "bet_id":       bet_id,
                    "pending":      True,
                }

            logger.info(f"Back placed: {bet_id} @ {avg_price} matched=£{size_matched:.2f}")
            return {
                "type":         "BACK",
                "selection_id": selection_id,
                "price":        avg_price,
                "size":         stake,
                "size_matched": round(size_matched, 2),
                "bet_id":       bet_id,
                "pending":      False,
            }

        error   = getattr(result, "error_code", "?")
        reports = getattr(result, "place_instruction_reports", [])
        rep_err = [getattr(r, "error_code", "") for r in reports]
        logger.error(f"Back failed: {getattr(result,'status','?')} {error} {rep_err}")
        return None

    except Exception as e:
        logger.error(f"place_back error: {e}", exc_info=True)
        return None
