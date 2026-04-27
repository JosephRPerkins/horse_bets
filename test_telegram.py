import requests
import config
from requests.auth import HTTPBasicAuth

results = []

# ── 1. Racing API ─────────────────────────────────────────────────────────────
try:
    resp = requests.get(
        f"{config.RACING_API_BASE_URL}/racecards/standard",
        auth=HTTPBasicAuth(config.RACING_API_USERNAME, config.RACING_API_PASSWORD),
        params={"day": "today"}
    )
    racecards = resp.json().get("racecards", [])
    results.append(f"✅ Racing API — {len(racecards)} races today")
except Exception as e:
    results.append(f"❌ Racing API — {e}")

# ── 2. Betfair ────────────────────────────────────────────────────────────────
try:
    resp = requests.post(
        "https://identitysso-cert.betfair.com/api/certlogin",
        data={"username": config.BETFAIR_USERNAME, "password": config.BETFAIR_PASSWORD},
        headers={"X-Application": config.BETFAIR_APP_KEY, "Content-Type": "application/x-www-form-urlencoded"},
        cert=(f"{config.BETFAIR_CERTS_DIR_SERVER}/client-2048.crt", f"{config.BETFAIR_CERTS_DIR_SERVER}/client-2048.key")
    )
    status = resp.json().get("loginStatus")
    results.append(f"✅ Betfair — {status}" if status == "SUCCESS" else f"❌ Betfair — {status}")
except Exception as e:
    results.append(f"❌ Betfair — {e}")

# ── 3. Telegram bots ──────────────────────────────────────────────────────────
bots = {
    "Main alerts bot":   config.TELEGRAM_BOT_TOKEN,
    "Betfair bot":       config.BETFAIR_TELEGRAM_BOT_TOKEN,
    "Bet365 bot":        config.BET365_TELEGRAM_BOT_TOKEN,
    "Results bot":       config.RESULTS_TELEGRAM_BOT_TOKEN,
}

for name, token in bots.items():
    if not token or token.startswith("your_"):
        results.append(f"⏭️  Telegram {name} — skipped (not configured)")
        continue
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data={"chat_id": config.TELEGRAM_CHAT_ID, "text": f"✅ {name} test message"}
        )
        ok = resp.json().get("ok")
        results.append(f"✅ Telegram {name}" if ok else f"❌ Telegram {name} — {resp.json()}")
    except Exception as e:
        results.append(f"❌ Telegram {name} — {e}")

# ── Summary ───────────────────────────────────────────────────────────────────
print("\n── Test Results ──────────────────────────────────────────")
for r in results:
    print(r)
print("──────────────────────────────────────────────────────────\n")
