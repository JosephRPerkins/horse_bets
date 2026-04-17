"""
betfair/commands.py
Telegram command listener for the Betfair bot.

Commands:
  /paper   — switch to paper trading (simulated bets, no real money)
  /live    — switch to live trading (real bets on Betfair Exchange)
  /mute    — silence all Betfair bot Telegram notifications
  /unmute  — resume notifications
  /stop    — pause betting (both modes)
  /start   — resume betting
  /status  — balance, mode, today's P&L (live + paper)
  /races   — today's qualifying races
  /help    — command list
"""

import logging
import time
import threading
import requests
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import config

from .strategy import get_stake, get_tsr_stake, STOP_FLOOR, stake_display
from .state    import save
from .notify   import send, send_chunks, set_muted, is_muted
from .api      import get_balance

logger = logging.getLogger("betfair.commands")

CARD_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "data", "cards", "today.json"
)

HELP_TEXT = """\
🤖 <b>Betfair Bot v3 Commands</b>
══════════════════════════════
<b>Mode</b>
/paper   — paper trading (simulated, safe — default)
/live    — live trading (real bets on exchange)

<b>Notifications</b>
/mute    — silence all Betfair bot notifications
/unmute  — resume notifications

<b>Control</b>
/stop         — pause betting (both modes)
/start        — resume betting
/resetprofit  — reset all paper P&L before going live
/status       — balance, mode, today's P&L
/races        — today's qualifying races
/help         — command list
══════════════════════════════
<b>Strategy</b>
Qualifying: All tiers except GOOD | Turf | Not Heavy
Excludes Irish NH staying races (2m4f+) on soft/heavy going
<b>Pick 2 is primary</b> — backed at 3/1+ | Pick 1 only at 2/1+
If Pick 1 odds-on: stake doubles on Pick 2 instead
SUPREME (TSR): Pick 1 gets one tier higher stake when it qualifies
<b>Staking cascade</b>
£5–£19: £2 → £20–£39: £4 → £40–£79: £8 → £80–£159: £16
£160–£319: £32 → £320–£599: £64 → £600+: £100
Stops below £5\
"""


def _races_status() -> str:
    if not os.path.exists(CARD_PATH):
        return "⚠️ today.json not found — main bot may not have run yet."
    try:
        with open(CARD_PATH) as f:
            data = json.load(f)
    except Exception as e:
        return f"⚠️ Could not read today.json: {e}"

    from .strategy import (
        qualifies, is_tsr_trigger, pick_stakes,
        MIN_PICK1_PRICE, MIN_PICK2_PRICE,
    )
    races      = data.get("races", [])
    qualifying = [r for r in races if qualifies(r)]
    if not qualifying:
        return "No qualifying races (AW, Heavy, Irish staying chase filtered)."

    bal = get_balance()
    lines = [
        f"🏇 <b>Today's qualifying races</b> ({len(qualifying)} of {len(races)})",
        f"Balance: £{bal:.2f} | {stake_display(bal)}",
        f"Pick 2 primary (3/1+) | Pick 1 only at 2/1+ | Good/Skip → £2 cap",
        "──────────────────────────────",
    ]
    for r in sorted(qualifying, key=lambda x: x.get("off", "99:99")):
        badge   = (r.get("tier_label") or "·").split()[0]
        top1    = r.get("top1") or {}
        top2    = r.get("top2") or {}
        tsr     = is_tsr_trigger(r)
        tsr_tag = " 🔥" if tsr else ""
        a_price = top1.get("sp_dec")
        b_price = top2.get("sp_dec")
        s_a, s_b = pick_stakes(bal, tsr, a_price, b_price, tier=r.get("tier", 0))
        p1_note  = " ⚠️odds-on" if (a_price and a_price < MIN_PICK1_PRICE) else ""
        p2_warn  = " ⚠️under 3/1" if (b_price and b_price < MIN_PICK2_PRICE) else ""
        lines.append(
            f"{badge} <b>{r.get('off','?')} {r.get('course','?')}</b>{tsr_tag}\n"
            f"  ⭐ {top1.get('horse','?')} ({top1.get('sp','?')}){p1_note} £{s_a:.2f} | "
            f"🔵 {top2.get('horse','?')} ({top2.get('sp','?')}){p2_warn} £{s_b:.2f}"
        )
    return "\n".join(lines)


def handle_command(cmd: str, state: dict) -> None:
    cmd = cmd.strip().lower()
    if "@" in cmd:
        cmd = cmd[:cmd.index("@")]

    # ── Mode ──────────────────────────────────────────────────────────────────

    if cmd == "/paper":
        old_mode = state.get("mode", "paper")
        state["mode"] = "paper"
        save(state)
        icon = "📝"
        msg  = (
            f"{icon} <b>Switched to PAPER mode</b>\n"
            f"Bets will be simulated — no real money placed.\n"
            f"Paper P&L today: {'+' if state.get('paper_daily_pnl', 0) >= 0 else ''}"
            f"£{state.get('paper_daily_pnl', 0.0):.2f}"
        )
        if old_mode == "live":
            msg += "\n⚠️ Switched from LIVE — no pending orders affected."
        send(msg)
        logger.info("Switched to paper mode")

    elif cmd == "/live":
        bal        = get_balance()
        cum_profit = state.get("cumulative_profit", 0.0)
        banked     = state.get("banked_profit", 0.0)
        state["mode"] = "live"
        save(state)
        warning = ""
        if cum_profit > 0 or banked > 0:
            warning = (
                f"\n⚠️ Paper cumulative profit: £{cum_profit:.2f} | Banked: £{banked:.2f}\n"
                f"Send /resetprofit to clear paper figures before live trading.\n"
                f"Without resetting, live stakes will be inflated by paper profits."
            )
        send(
            f"💰 <b>Switched to LIVE mode</b>\n"
            f"Real bets will now be placed on Betfair Exchange.\n"
            f"Balance: £{bal:.2f} | {stake_display(cum_profit)}{warning}\n"
            f"⚠️ This uses real money. Send /paper to return to simulation."
        )
        logger.info("Switched to live mode")

    elif cmd == "/resetprofit":
        old_cum    = state.get("cumulative_profit", 0.0)
        old_banked = state.get("banked_profit", 0.0)
        old_place  = state.get("paper_place_pnl", 0.0)
        # Reset all paper-derived profit figures
        state["cumulative_profit"]  = 0.0
        state["banked_profit"]      = 0.0
        state["paper_daily_pnl"]    = 0.0
        state["paper_daily_bets"]   = []
        state["paper_place_pnl"]    = 0.0
        state["profit_milestone"]   = 0.0
        save(state)
        send(
            f"🔄 <b>Profit reset for live trading</b>\n"
            f"Cleared:\n"
            f"  Cumulative P&L: £{old_cum:.2f} → £0.00\n"
            f"  Banked profit:  £{old_banked:.2f} → £0.00\n"
            f"  Place P&L:      £{old_place:.2f} → £0.00\n"
            f"Stakes now reset to £{get_stake(0.0):.0f}/horse (base tier).\n"
            f"Live P&L tracking starts fresh from zero."
        )
        logger.info(f"Profit reset: cum={old_cum:.2f} banked={old_banked:.2f}")
    # ── Notifications ─────────────────────────────────────────────────────────

    elif cmd == "/mute":
        state["muted"] = True
        save(state)
        set_muted(True)
        # Send confirmation BEFORE muting takes effect (set_muted called after send)
        send("🔕 <b>Notifications muted</b>\nSend /unmute to resume.")
        logger.info("Notifications muted")

    elif cmd == "/unmute":
        state["muted"] = False
        save(state)
        set_muted(False)
        bal = get_balance()
        send(
            f"🔔 <b>Notifications resumed</b>\n"
            f"Balance: £{bal:.2f} | Mode: {state.get('mode','paper').upper()}"
        )
        logger.info("Notifications unmuted")

    # ── Betting control ───────────────────────────────────────────────────────

    elif cmd == "/stop":
        state["betting_paused"] = True
        save(state)
        send("⏸️ <b>Betting paused</b>\nNo new bets (live or paper) will be placed.\nSend /start to resume.")
        logger.info("Betting paused")

    elif cmd == "/start":
        state["betting_paused"] = False
        save(state)
        mode = state.get("mode", "paper").upper()
        bal  = get_balance()
        send(
            f"▶️ <b>Betting resumed</b>\n"
            f"Mode: {mode} | Balance: £{bal:.2f}\n"
            f"Stake: {stake_display(bal)}"
        )
        logger.info("Betting resumed")

    # ── Info ──────────────────────────────────────────────────────────────────

    elif cmd == "/status":
        bal    = get_balance()
        mode   = state.get("mode", "paper").upper()
        paused = state.get("betting_paused", False)
        muted  = is_muted()

        live_pnl   = state.get("daily_pnl", 0.0)
        paper_pnl  = state.get("paper_daily_pnl", 0.0)
        live_bets  = state.get("daily_bets", [])
        paper_bets = state.get("paper_daily_bets", [])
        live_wins  = sum(1 for b in live_bets  if b.get("total_pnl", 0) > 0)
        paper_wins = sum(1 for b in paper_bets if b.get("total_pnl", 0) > 0)

        mode_icon = "💰" if mode == "LIVE" else "📝"
        lines = [
            "📊 <b>Betfair Bot Status</b>",
            "──────────────────────────────",
            f"Mode:          {mode_icon} {mode}",
            f"Betting:       {'⏸️ PAUSED' if paused else '▶️ ACTIVE'}",
            f"Notifications: {'🔕 MUTED' if muted else '🔔 ON'}",
            f"Balance:       £{bal:.2f}",
            f"Stake:         {stake_display(bal)}",
            "──────────────────────────────",
        ]
        if live_bets or mode == "LIVE":
            sign = "+" if live_pnl >= 0 else ""
            lines.append(
                f"💰 Live P&L:  {sign}£{live_pnl:.2f} "
                f"({live_wins}W / {len(live_bets)-live_wins}L)"
            )
        if paper_bets or mode == "PAPER":
            sign = "+" if paper_pnl >= 0 else ""
            lines.append(
                f"📝 Paper P&L: {sign}£{paper_pnl:.2f} "
                f"({paper_wins}W / {len(paper_bets)-paper_wins}L)"
            )

        # Show last 5 settled races (whichever mode is active)
        recent = (live_bets if mode == "LIVE" else paper_bets)[-5:]
        if recent:
            lines.append("──────────────────────────────")
            for b in recent:
                icon = "✅" if b.get("total_pnl", 0) > 0 else "❌"
                sign = "+" if b.get("total_pnl", 0) >= 0 else ""
                lines.append(f"  {icon} {b['race']} → {sign}£{b['total_pnl']:.2f}")

        send("\n".join(lines))

    elif cmd == "/races":
        send_chunks(_races_status())

    elif cmd == "/help":
        send(HELP_TEXT)

    else:
        send(f"❓ Unknown command: {cmd}\nSend /help for available commands.")


def start_command_listener(state: dict) -> None:
    """Poll Telegram for commands every 5 seconds in a background thread."""
    offset = [0]

    def _poll():
        while True:
            try:
                url  = (f"https://api.telegram.org/"
                        f"bot{config.BETFAIR_TELEGRAM_BOT_TOKEN}/getUpdates")
                resp = requests.get(
                    url, params={"offset": offset[0], "timeout": 10}, timeout=15
                )
                if resp.status_code == 200:
                    for update in resp.json().get("result", []):
                        offset[0] = update["update_id"] + 1
                        txt = (update.get("message", {}).get("text") or "").strip()
                        if txt.startswith("/"):
                            logger.info(f"Command: {txt}")
                            handle_command(txt, state)
            except Exception as e:
                logger.error(f"Command listener error: {e}")
            time.sleep(5)

    t = threading.Thread(target=_poll, daemon=True, name="BetfairCommands")
    t.start()
    logger.info("Betfair command listener started")
