# horse_bets_v3

Automated horse racing analysis and Betfair exchange betting bot. Two independent processes:

- **main.py** — Telegram alert bot. Fetches the day's race card, scores every runner, and sends pre-race alerts 10 minutes before each race.
- **betfair_main.py** — Betfair exchange bot. Places automated back bets 10 minutes before each qualifying race, settles results, and tracks daily P&L.

Both bots are controlled via Telegram commands and run as background processes (designed for a Raspberry Pi or VPS).

---

## Requirements

- Python 3.11+
- Betfair Exchange account with API access and SSL certificates
- [The Racing API](https://www.theracingapi.com) subscription
- Telegram bot tokens (create via [@BotFather](https://t.me/botfather))

---

## Setup

### 1. Clone and install dependencies

```bash
git clone https://github.com/AlexAnderson220994/horse_bets_v3.git
cd horse_bets_v3
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure credentials

```bash
cp config.example.py config.py
```

Edit `config.py` and fill in:
- Racing API username and password
- Telegram chat ID and bot tokens (one per bot)
- Betfair username, password, and app key
- Path to your Betfair SSL certificate directory

### 3. Betfair SSL certificates

Betfair requires SSL certificates for non-interactive (bot) login. Generate them and place in a `certs/` directory:

```
certs/
  client-2048.crt
  client-2048.key
```

Guide: [Betfair non-interactive login](https://docs.developer.betfair.com/display/1smk3cen4v3lu3yomq5qye0ni/Non-Interactive+%28bot%29+login)

### 4. Create data directories

```bash
mkdir -p data/raw data/cards data/history data/results data/logs data/streaks logs
```

---

## Running

### Main alert bot

```bash
python main.py
```

Or as a background process:

```bash
nohup python main.py >> logs/main.log 2>&1 &
```

### Betfair exchange bot

```bash
python betfair_main.py
```

Or as a background process:

```bash
nohup python betfair_main.py >> logs/betfair.log 2>&1 &
```

Both bots can run simultaneously and independently.

---

## Betfair bot — Telegram commands

| Command | Description |
|---|---|
| `/paper` | Switch to paper trading (simulated, no real money) — **default** |
| `/live` | Switch to live trading (real bets on Betfair Exchange) |
| `/stop` | Pause all betting |
| `/start` | Resume betting |
| `/status` | Balance, mode, today's P&L |
| `/races` | Today's qualifying races with projected stakes |
| `/mute` | Silence notifications |
| `/unmute` | Resume notifications |
| `/help` | Command list |

---

## Strategy

**Qualification:** All race tiers (Turf only, no Heavy going, no Irish NH staying races on soft/heavy)

**Picks:** The model scores every runner and selects Pick 1 and Pick 2.

**Staking cascade** (compounding — no ceiling, liquidity is the real cap):

| Balance | Stake per horse |
|---|---|
| £0–£19 | £2 |
| £20–£39 | £4 |
| £40–£79 | £8 |
| £80–£159 | £16 |
| £160–£319 | £32 |
| £320–£639 | £64 |
| £640+ | £128+ |

Good/Skip tier races are always capped at £2/horse regardless of balance.

**Price gates:** Pick 2 must be 3/1+ (≥4.0 dec). Pick 1 must be 2/1+ (≥2.0 dec) or its stake is redirected to Pick 2.

**Liquidity:** Both picks are always staked equally at `min(tier_stake, liquidity_pick1, liquidity_pick2)`. Race is skipped if either pick has less than £2 available.

---

## Backtesting

Run the backtest across all historical data in `data/raw/`:

```bash
# Full compounding run
python backtest_strategy.py --daily

# Reset to £10 each day (per-day performance)
python backtest_strategy.py --reset-daily

# Last 10 days, show every race
python backtest_strategy.py --last 10 --reset-daily --races

# Filter to one tier
python backtest_strategy.py --tier supreme --daily
```

Historical data files (`data/raw/YYYY-MM-DD.json`) are not included in the repo. Run `fetch_data.py` to populate them.

---

## Project structure

```
horse_bets_v3/
├── main.py                  # Main alert bot entry point
├── betfair_main.py          # Betfair exchange bot entry point
├── config.example.py        # Credential template — copy to config.py
├── predict.py               # Runner scoring model
├── predict_v2.py            # Confidence tier classifier
├── backtest_strategy.py     # Historical backtest runner
├── fetch_data.py            # Fetch race results into data/raw/
├── core/
│   ├── scorer.py            # Runner scoring logic
│   ├── positions.py         # Bet Builder position calculator
│   ├── star_rating.py       # Race star rating
│   └── api_client.py        # Racing API wrapper
├── betfair/
│   ├── api.py               # Betfair Exchange API wrapper
│   ├── strategy.py          # Staking and qualification logic
│   ├── settlement.py        # Live bet settlement via Racing API
│   ├── commands.py          # Telegram command handler
│   ├── notify.py            # Telegram notifications
│   ├── state.py             # Persistent bot state
│   └── balance_log.py       # Balance audit log
├── scheduler/
│   ├── daily_jobs.py        # Morning briefing, EOD summary
│   ├── race_jobs.py         # Pre-race alerts, result polling
│   └── main_scheduler.py    # APScheduler setup
├── notifications/
│   ├── formatter.py         # Telegram message templates
│   └── streak_tracker.py    # Theoretical compounding tracker
├── data/
│   └── simulation/          # Backtest reference results
└── requirements.txt
```
