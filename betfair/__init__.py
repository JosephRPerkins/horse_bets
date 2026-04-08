"""
betfair/__init__.py

Betfair integration stub.
The Betfair bot will implement these functions when ready.
Everything here is a no-op — pre_race_hook() is called from race_jobs.py
so the wiring is already in place.
"""


def get_exchange_odds(race_id: str, horse_ids: list) -> dict:
    """
    Fetch live exchange odds for a list of horses.
    Returns {horse_id: decimal_odds} or {} until implemented.
    """
    return {}


def on_pre_race(race_id: str, win_pick: dict, place_picks: list,
                tier: int, tier_label: str) -> None:
    """
    Called 10 minutes before each race after the pre-race alert fires.
    Betfair bot will implement bet placement logic here.
    """
    pass
