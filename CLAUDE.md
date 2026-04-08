# Horse Bets — Claude Working Notes

## What This Project Does

A two-bot system for horse racing analysis and automated Bet Builder suggestions.

- **main.py** — Telegram alert bot. Fetches today's card, scores every runner, assigns star ratings, sends pre-race alerts 10 minutes before each race. No auto-betting.
- **betfair_main.py** — Betfair exchange bot. Places automated bets via the Betfair API.

The main bot's selection logic lives in `core/`:
- `core/scorer.py` — scores runners using RPR/OR gap, TSR/OR gap, SP bands, form placed_4, trainer & jockey 14-day stats
- `core/positions.py` — assigns Bet Builder Top-N positions targeting Evens–2/1 combined odds
- `core/star_rating.py` — assigns 1–5 star rating per race based on combined score, field size, distance, going, SP profile

**SACRED FILES — never modify these three:**
`core/scorer.py`, `core/positions.py`, `core/star_rating.py`

---

## Data Available for Analysis

- `data/raw/YYYY-MM-DD.json` — 20 days of results (2026-03-12 to 2026-04-02), 606 races, 5531 runners
- `data/simulation/full_analysis.txt` — pre-existing 14-day backtest (425 races)
- Each runner has: `sp`, `sp_dec`, `bsp`, `position`, `or`, `rpr`, `tsr`, `form_detail` (placed_last_4, bad_recent, recent_positions), `trainer_14d`, `jockey_14d`, `draw`, `headgear`, `age`, `sex`, surface/going from race

---

## Current Bot Performance (from existing 14-day backtest)

Source: `data/simulation/full_analysis.txt`

| Metric | Result |
|---|---|
| Horse A wins outright | 58% (246/425) |
| Horse A places top-3 | 88% (372/425) |
| Conservative Bet Builder pair wins | **78% (148/190)** |
| 4-star races — conservative pair | **94% (34/36)** |
| 3-star races — conservative pair | 80% (65/81) |

---

## Key Signals Found in the 20-Day Data

These were derived from fresh analysis of `data/raw/*.json`, independent of the existing backtest.

### Signal Strength Ranking

| Signal | Win Rate | Top-3 Rate | n |
|---|---|---|---|
| **TSR > OR (single runner in race)** | **91.1%** | **97.8%** | 45 |
| TSR > OR + profitable trainer | 77.8% | 97.2% | 36 |
| RPR >= OR + SP < 4.0 | 48.0% | 92.1% | 304 |
| Trainer P&L positive 14d (≥5 runs) | 22.5% | — | 1047 |
| RPR >= OR | 31.5% | 78.5% | 1296 |
| RPR < OR | 1.2% | — | 2075 |
| Favourite (SP < 2.0) wins | 56.6% | — | 99 |

### SP Tier Win Rates

| SP Range | Win Rate | n |
|---|---|---|
| Odds-on (< 2.0) | 56.6% | 99 |
| Evens–3/1 (2–4) | 32.1% | 601 |
| 4/1–5/1 (4–6) | 20.0% | 654 |
| 6/1–9/1 (6–10) | 11.2% | 966 |
| 10/1–15/1 | 7.1% | 915 |
| 16/1+ | 2.6% | 1793 |

### Race Type Pair Win Rate (SP top-2 both finish top-3)

| Race Type | Pair Win Rate |
|---|---|
| NH Flat | **63.6%** |
| Chase | **54.5%** |
| Hurdle | 47.9% |
| Flat | 38.9% |

### Surface

| Surface | Pair Win Rate |
|---|---|
| Turf | **49.2%** |
| AW | 40.3% |

### Form (placed in last 4 runs)

| Placed | Win Rate | n |
|---|---|---|
| 4/4 | **23.1%** | 65 |
| 3/4 | 17.3% | 156 |
| 2/4 | 15.2% | 231 |
| 1/4 | 11.2% | 178 |
| 0/4 | 11.4% | 4398 |

### Odds-on Favourite Races (99 races)

- Fav wins: 56.6%
- Fav + 2nd both top-3: **71.0%**
- Fav + 2nd both top-2: 46.5%

---

## Proposed Selection Strategies (to compare vs bot)

### Strategy 1: TSR > OR Trigger (highest-confidence standalone bet)

**Rule:** In any race where exactly one runner has TSR above their OR, back that horse to win or place top-3. Back on Betfair exchange rather than Bet Builder.

**Evidence:** 41/45 wins (91%), 44/45 top-3 (98%). N=45 races across 20 days ≈ ~2–3 qualifying races per day.

**vs Bot:** The bot uses TSR/OR as one component in a composite score, but does not isolate this as a standalone trigger. The bot may or may not pick the TSR>OR runner as Horse A (depends on SP/form combo). This strategy would fire independently of star rating.

**Practical use:** Flag in pre-race alert when exactly one runner has TSR > OR. Treat as a "special trigger" above and beyond the normal pair strategy.

---

### Strategy 2: Avoid Flat / AW Races for Betting

**Rule:** For rolling strategy bets, skip Flat races on AW surfaces.

**Evidence:**
- Flat pair win rate: 38.9% (lowest of all types)
- AW pair win rate: 40.3% vs Turf 49.2%
- Flat AW: approximately 33–35% (by extension)

**vs Bot:** The bot's star rating does not explicitly penalise Flat or AW races. The bot's `surface` field is stored but not used in `star_rating.py`.

**Recommendation:** Consider limiting rolling strategy bets to Hurdle, Chase, NH Flat, or Turf Flat races.

---

### Strategy 3: Profitable-Trainer Filter

**Rule:** When choosing between marginal pair candidates, prefer the horse whose trainer has positive P&L over the last 14 days (≥5 runs).

**Evidence:** Profitable trainer: 22.5% win rate vs losing trainer: 9.0% — a 2.5x difference.

**vs Bot:** The bot already awards +0.5 to +1.5 score points for a profitable trainer. This is already baked in — not a gap, but a confirmation that the bot's trainer weight is correct.

---

### Strategy 4: Concentrate on 4-Star Races Only

**Rule:** Only bet on races the bot rates 4 or 5 stars.

**Evidence (from existing backtest):**
- 4-star conservative pair: 94% (34/36 races)
- 5-star conservative pair: 78% (28/36)
- 3-star conservative pair: 80% (65/81)
- 2-star: 59%, 1-star: 40%

**vs Bot:** The bot already sends alerts for all starred races. The rolling strategy in `star_rating.py` recommends 4+ stars. This strategy says: ignore anything below 4 stars for actual bets.

---

### Strategy 5: Long-Distance Races (14f+)

**Rule:** Prioritise bets on races at 14 furlongs or more.

**Evidence (from existing backtest, full_analysis.txt):**
- 14f–2m4f: conservative pair 90% (19/21)
- 2m5f+: conservative pair 84% (41/49)
- Sprint (<7f): conservative pair 72% (46/64)

**vs Bot:** The bot already applies a `dist_bonus` for 14f+ in `star_rating.py`. This confirms that's correct. Sprint races cap at 3 stars.

---

### Strategy 6: Odds-on Favourite as Anchor

**Rule:** When the race favourite is odds-on (SP < 2.0), pair them with the next-best horse at 4/1 or bigger for a high-probability Bet Builder.

**Evidence:** Odds-on fav + 2nd choice both top-3 = 71% (66/93 races). Optimal SP profile for Horse B is 4/1–9/1 (the bot already targets this via `optimal_profile` in `star_rating.py`).

**vs Bot:** This is already partially encoded. The key addition: if the bot's Horse A is odds-on, it should be treated as a near-certain qualifier and the search for Horse B should be the decisive filter.

---

## What the Bot Does NOT Currently Do

1. **Isolate the TSR > OR signal** as a standalone high-confidence flag (biggest gap)
2. **Penalise AW / Flat racing** in the star rating
3. **Track rolling P&L** across days to adjust bet sizing
4. **Handle non-runners** after pre-race alert fires (horse withdrawn post-analysis)

---

## Files to Know

| File | Purpose |
|---|---|
| `main.py` | Entry point — scheduler, Telegram startup |
| `core/scorer.py` | Runner scoring (SACRED) |
| `core/positions.py` | Bet Builder position calc (SACRED) |
| `core/star_rating.py` | Star rating (SACRED) |
| `scheduler/daily_jobs.py` | Morning briefing, end-of-day, analysis |
| `scheduler/race_jobs.py` | Pre-race alerts, result polling |
| `core/api_client.py` | Racing API wrapper |
| `notifications/formatter.py` | Telegram message templates |
| `config.py` | API keys, paths, timing constants |
| `data/raw/YYYY-MM-DD.json` | Historical race results |
| `data/simulation/full_analysis.txt` | 14-day backtest results |
