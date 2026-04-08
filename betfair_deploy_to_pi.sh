#!/bin/bash
# betfair_deploy_to_pi.sh — horse_bets_v3 Betfair Bot
#
# Deploys the Betfair bot to the Pi. Runs alongside main.py.
# Reads data/cards/today.json written by the main bot.
# Uses the same data/ directory as the main bot (no separate dir needed).
#
# Usage:
#   ./betfair_deploy_to_pi.sh start    — first deploy: dirs, certs, venv, start
#   ./betfair_deploy_to_pi.sh stop     — stop the Betfair bot
#   ./betfair_deploy_to_pi.sh update   — deploy updated code, restart (data preserved)
#   ./betfair_deploy_to_pi.sh certs    — copy Betfair SSL certs only
#   ./betfair_deploy_to_pi.sh status   — check if running
#   ./betfair_deploy_to_pi.sh logs     — tail live logs
#   ./betfair_deploy_to_pi.sh refresh  — full wipe (data preserved) + redeploy

PI_USER="alex_server"
PI_HOST="raspberrypi"
PI_DIR="/home/alex_server/ssd-projects/horse_bets_v3"
PI_LOG="$PI_DIR/logs/betfair.log"
PID_FILE="$PI_DIR/betfair.pid"

# Betfair SSL certs — on Mac:
MAC_CERTS_DIR="/Users/alexanderson/Projects/horses/horse_bets/certs"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log()  { echo -e "${GREEN}[betfair-deploy]${NC} $1"; }
warn() { echo -e "${YELLOW}[betfair-deploy]${NC} $1"; }
err()  { echo -e "${RED}[betfair-deploy]${NC} $1"; exit 1; }

if [ -z "$1" ]; then
    echo "Usage: ./betfair_deploy_to_pi.sh [start|stop|update|certs|status|logs|refresh]"
    exit 1
fi

if [ ! -f "betfair_main.py" ]; then
    err "Run this script from the horse_bets_v3 project root (where betfair_main.py lives)"
fi

# ── Functions ──────────────────────────────────────────────────────────────────

deploy_files() {
    log "Creating betfair directory on Pi..."
    ssh "$PI_USER@$PI_HOST" "
        mkdir -p $PI_DIR/betfair
        mkdir -p $PI_DIR/data/cards
        mkdir -p $PI_DIR/data/history
        mkdir -p $PI_DIR/data/logs
        mkdir -p $PI_DIR/logs
        mkdir -p $PI_DIR/certs
    "

    log "Copying Betfair bot files..."

    # Entry point
    scp betfair_main.py "$PI_USER@$PI_HOST:$PI_DIR/"

    # Betfair package
    rsync -av --exclude='__pycache__' betfair/ \
        "$PI_USER@$PI_HOST:$PI_DIR/betfair/"

    # Shared files needed by betfair_main.py
    scp config.py predict_v2.py "$PI_USER@$PI_HOST:$PI_DIR/"

    log "Files copied ✅"
}

copy_certs() {
    log "Copying Betfair SSL certs to Pi..."
    if [ ! -f "$MAC_CERTS_DIR/client-2048.crt" ]; then
        err "Certs not found at $MAC_CERTS_DIR — check MAC_CERTS_DIR in this script"
    fi
    scp "$MAC_CERTS_DIR/client-2048.crt" "$PI_USER@$PI_HOST:$PI_DIR/certs/"
    scp "$MAC_CERTS_DIR/client-2048.key" "$PI_USER@$PI_HOST:$PI_DIR/certs/"
    log "Certs copied ✅"
}

install_deps() {
    log "Installing Betfair dependency (betfairlightweight)..."
    ssh "$PI_USER@$PI_HOST" "
        cd $PI_DIR
        source venv/bin/activate
        pip install betfairlightweight --quiet
        echo 'betfairlightweight installed'
    "
    log "Deps installed ✅"
}

start_bot() {
    log "Starting Betfair bot on Pi..."
    ssh "$PI_USER@$PI_HOST" "
        cd $PI_DIR
        if [ -f $PID_FILE ]; then
            OLD_PID=\$(cat $PID_FILE)
            if kill -0 \$OLD_PID 2>/dev/null; then
                kill \$OLD_PID
                echo 'Stopped existing Betfair bot (pid '\$OLD_PID')'
                sleep 2
            fi
            rm -f $PID_FILE
        fi
        source venv/bin/activate
        nohup python betfair_main.py >> $PI_LOG 2>&1 &
        echo \$! > $PID_FILE
        echo 'Betfair bot started with pid '\$(cat $PID_FILE)
    "
    sleep 3
    ssh "$PI_USER@$PI_HOST" "
        if [ -f $PID_FILE ] && kill -0 \$(cat $PID_FILE) 2>/dev/null; then
            echo '✅ Betfair bot running (pid '\$(cat $PID_FILE)')'
        else
            echo '❌ Betfair bot did not start — check logs'
            tail -20 $PI_LOG
        fi
    "
    log "Logs: ssh $PI_USER@$PI_HOST 'tail -f $PI_LOG'"
}

stop_bot() {
    log "Stopping Betfair bot on Pi..."
    ssh "$PI_USER@$PI_HOST" "
        if [ -f $PID_FILE ]; then
            PID=\$(cat $PID_FILE)
            if kill -0 \$PID 2>/dev/null; then
                kill \$PID
                echo 'Betfair bot stopped (pid '\$PID')'
            else
                echo 'Betfair bot was not running'
            fi
            rm -f $PID_FILE
        else
            PID=\$(pgrep -f 'python betfair_main.py' | head -1)
            if [ -n \"\$PID\" ]; then
                kill \$PID
                echo 'Betfair bot stopped (pid '\$PID')'
            else
                echo 'No running Betfair bot found'
            fi
        fi
    "
    log "Done ✅"
}

check_status() {
    ssh "$PI_USER@$PI_HOST" "
        if [ -f $PID_FILE ]; then
            PID=\$(cat $PID_FILE)
            if kill -0 \$PID 2>/dev/null; then
                echo '✅ Betfair bot running (pid '\$PID')'
                UPTIME=\$(ps -o etime= -p \$PID 2>/dev/null | xargs)
                echo '   Uptime: '\$UPTIME
            else
                echo '❌ Betfair bot NOT running (stale pid)'
            fi
        else
            PID=\$(pgrep -f 'python betfair_main.py' | head -1)
            if [ -n \"\$PID\" ]; then
                echo '✅ Betfair bot running (pid '\$PID')'
            else
                echo '❌ Betfair bot is NOT running'
            fi
        fi
        if [ -f $PI_LOG ]; then
            echo ''
            echo 'Last 5 log lines:'
            tail -5 $PI_LOG
        fi
    "
}

# ── Commands ───────────────────────────────────────────────────────────────────

case "$1" in

    start)
        log "=== FIRST DEPLOY — BETFAIR BOT v3 ==="
        log "The main bot (main.py) must be running on the Pi first."
        log "This bot reads data/cards/today.json written by the main bot."
        deploy_files
        copy_certs
        install_deps
        start_bot
        ;;

    stop)
        log "=== STOPPING BETFAIR BOT ==="
        stop_bot
        ;;

    update)
        log "=== UPDATING BETFAIR BOT ==="
        log "Data, certs, and state are preserved."
        stop_bot
        sleep 1
        deploy_files
        start_bot
        ;;

    certs)
        log "=== COPYING CERTS ONLY ==="
        copy_certs
        ;;

    status)
        check_status
        ;;

    logs)
        log "Tailing Betfair bot logs (Ctrl+C to stop)..."
        ssh "$PI_USER@$PI_HOST" "tail -f $PI_LOG"
        ;;

    refresh)
        log "=== FULL REFRESH — BETFAIR BOT ==="
        warn "This wipes betfair_state.json and betfair_balance_log.json"
        warn "Certs and today.json are preserved."
        read -p "[betfair-deploy] Continue? (y/N) " confirm
        if [ "$confirm" != "y" ] && [ "$confirm" != "Y" ]; then
            log "Cancelled."
            exit 0
        fi
        stop_bot
        ssh "$PI_USER@$PI_HOST" "
            rm -f $PI_DIR/data/betfair_state.json
            rm -f $PI_DIR/data/betfair_balance_log.json
            rm -f $PI_DIR/logs/betfair.log
            echo 'Betfair state wiped'
        "
        deploy_files
        start_bot
        ;;

    *)
        echo "Usage: ./betfair_deploy_to_pi.sh [start|stop|update|certs|status|logs|refresh]"
        echo ""
        echo "  start   — first deploy: certs, deps, start bot"
        echo "  stop    — stop the Betfair bot"
        echo "  update  — deploy updated code and restart (data + certs preserved)"
        echo "  certs   — copy SSL certs only"
        echo "  status  — check if running + last 5 log lines"
        echo "  logs    — tail live logs"
        echo "  refresh — wipe state/logs and redeploy (certs preserved)"
        exit 1
        ;;

esac
