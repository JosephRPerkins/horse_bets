#!/bin/bash
# deploy_to_pi.sh — horse_bets_v3
#
# Usage:
#   ./deploy_to_pi.sh start    — deploy files, setup venv, start bot
#   ./deploy_to_pi.sh stop     — stop the bot on the Pi
#   ./deploy_to_pi.sh update   — deploy updated code files and restart (keeps data)
#   ./deploy_to_pi.sh refresh  — stop, wipe all cards/logs, redeploy, restart fresh
#   ./deploy_to_pi.sh status   — check if bot is running
#   ./deploy_to_pi.sh logs     — tail live bot logs

PI_USER="alex_server"
PI_HOST="raspberrypi"
PI_DIR="/home/alex_server/ssd-projects/horse_bets_v3"
PI_VENV="$PI_DIR/venv"
PI_LOG="$PI_DIR/logs/bot.log"
PID_FILE="$PI_DIR/bot.pid"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log()  { echo -e "${GREEN}[deploy]${NC} $1"; }
warn() { echo -e "${YELLOW}[deploy]${NC} $1"; }
err()  { echo -e "${RED}[deploy]${NC} $1"; exit 1; }

# ── Sanity checks ──────────────────────────────────────────────────────────────

if [ -z "$1" ]; then
    echo "Usage: ./deploy_to_pi.sh [start|stop|update|refresh|status|logs]"
    exit 1
fi

if [ ! -f "main.py" ]; then
    err "Run this script from the horse_bets_v3 project root (where main.py lives)"
fi

# ── Functions ──────────────────────────────────────────────────────────────────

deploy_files() {
    log "Creating directory structure on Pi..."
    ssh "$PI_USER@$PI_HOST" "
        mkdir -p $PI_DIR/utils
        mkdir -p $PI_DIR/core
        mkdir -p $PI_DIR/notifications
        mkdir -p $PI_DIR/scheduler
        mkdir -p $PI_DIR/betfair
        mkdir -p $PI_DIR/data/raw
        mkdir -p $PI_DIR/data/cards
        mkdir -p $PI_DIR/data/history
        mkdir -p $PI_DIR/data/simulation
        mkdir -p $PI_DIR/data/results
        mkdir -p $PI_DIR/data/logs
        mkdir -p $PI_DIR/logs
    "

    log "Copying files to Pi..."

    # Root files
    scp main.py config.py predict.py predict_v2.py requirements.txt \
        "$PI_USER@$PI_HOST:$PI_DIR/"

    # utils
    scp utils/__init__.py utils/helpers.py \
        "$PI_USER@$PI_HOST:$PI_DIR/utils/"

    # core
    scp core/__init__.py core/api_client.py \
        "$PI_USER@$PI_HOST:$PI_DIR/core/"

    # notifications
    scp notifications/__init__.py notifications/telegram.py \
        notifications/formatter.py \
        "$PI_USER@$PI_HOST:$PI_DIR/notifications/"

    # scheduler
    scp scheduler/__init__.py scheduler/daily_jobs.py \
        scheduler/race_jobs.py scheduler/main_scheduler.py \
        scheduler/command_listener.py \
        "$PI_USER@$PI_HOST:$PI_DIR/scheduler/"

    # betfair package (stub __init__ + full bot modules)
    rsync -av --exclude='__pycache__' betfair/ \
        "$PI_USER@$PI_HOST:$PI_DIR/betfair/"

    log "Files copied ✅"
}

setup_venv() {
    log "Setting up Python virtual environment on Pi..."
    ssh "$PI_USER@$PI_HOST" "
        cd $PI_DIR
        if [ ! -d venv ]; then
            python3 -m venv venv
            echo 'venv created'
        fi
        source venv/bin/activate
        pip install --upgrade pip -q
        pip install -r requirements.txt -q
        echo 'dependencies installed'
    "
    log "venv ready ✅"
}

start_bot() {
    log "Starting bot on Pi..."
    ssh "$PI_USER@$PI_HOST" "
        cd $PI_DIR
        if [ -f $PID_FILE ]; then
            OLD_PID=\$(cat $PID_FILE)
            if kill -0 \$OLD_PID 2>/dev/null; then
                kill \$OLD_PID
                echo 'Stopped existing bot (pid '\$OLD_PID')'
                sleep 2
            fi
            rm -f $PID_FILE
        fi
        source venv/bin/activate
        nohup python main.py >> $PI_LOG 2>&1 &
        echo \$! > $PID_FILE
        echo 'Bot started with pid '\$(cat $PID_FILE)
    "
    log "Bot running on Pi ✅"
    log "Logs: ssh $PI_USER@$PI_HOST 'tail -f $PI_LOG'"
}

stop_bot() {
    log "Stopping bot on Pi..."
    ssh "$PI_USER@$PI_HOST" "
        if [ -f $PID_FILE ]; then
            PID=\$(cat $PID_FILE)
            if kill -0 \$PID 2>/dev/null; then
                kill \$PID
                echo 'Bot stopped (pid '\$PID')'
            else
                echo 'Bot was not running'
            fi
            rm -f $PID_FILE
        else
            PID=\$(pgrep -f 'python main.py' | head -1)
            if [ -n \"\$PID\" ]; then
                kill \$PID
                echo 'Bot stopped (pid '\$PID')'
            else
                echo 'No running bot found'
            fi
        fi
    "
    log "Done ✅"
}

wipe_data() {
    log "Wiping cards, logs and cache on Pi (keeping raw data)..."
    ssh "$PI_USER@$PI_HOST" "
        # Clear today's card so bot fetches fresh on startup
        rm -f $PI_DIR/data/cards/today.json
        rm -f $PI_DIR/data/cards/tomorrow.json

        # Clear history (bet records)
        rm -f $PI_DIR/data/history/*.json

        # Clear results
        rm -f $PI_DIR/data/results/*.json

        # Clear simulation data
        rm -f $PI_DIR/data/simulation/*.json
        rm -f $PI_DIR/data/simulation/*.txt
        rm -f $PI_DIR/data/simulation/*.csv

        # Clear logs
        rm -f $PI_DIR/logs/*.log

        echo 'Data wiped (raw data preserved)'
    "
    log "Data wiped ✅"
}

check_status() {
    log "Checking bot status on Pi..."
    ssh "$PI_USER@$PI_HOST" "
        if [ -f $PID_FILE ]; then
            PID=\$(cat $PID_FILE)
            if kill -0 \$PID 2>/dev/null; then
                echo '✅ Bot is running (pid '\$PID')'
                UPTIME=\$(ps -o etime= -p \$PID 2>/dev/null | xargs)
                echo '   Uptime: '\$UPTIME
            else
                echo '❌ Bot is NOT running (stale pid file)'
            fi
        else
            PID=\$(pgrep -f 'python main.py' | head -1)
            if [ -n \"\$PID\" ]; then
                echo '✅ Bot is running (pid '\$PID')'
            else
                echo '❌ Bot is NOT running'
            fi
        fi
        # Show last 5 log lines
        if [ -f $PI_LOG ]; then
            echo ''
            echo 'Last 5 log lines:'
            tail -5 $PI_LOG
        fi
    "
}

tail_logs() {
    log "Tailing bot logs on Pi (Ctrl+C to stop)..."
    ssh "$PI_USER@$PI_HOST" "tail -f $PI_LOG"
}

# ── Commands ───────────────────────────────────────────────────────────────────

case "$1" in

    start)
        log "=== DEPLOYING AND STARTING BOT ==="
        deploy_files
        setup_venv
        start_bot
        sleep 2
        check_status
        ;;

    stop)
        log "=== STOPPING BOT ==="
        stop_bot
        ;;

    update)
        log "=== UPDATING CODE AND RESTARTING BOT ==="
        log "Data and raw files are preserved"
        stop_bot
        sleep 1
        deploy_files
        start_bot
        sleep 2
        check_status
        ;;

    refresh)
        log "=== FULL REFRESH — WIPING CARDS/LOGS AND RESTARTING ==="
        warn "This will delete all cards, history, logs and simulation data"
        warn "Raw race data (data/raw/) is preserved"
        read -p "Are you sure? (y/N) " confirm
        if [ "$confirm" != "y" ] && [ "$confirm" != "Y" ]; then
            log "Cancelled"
            exit 0
        fi
        stop_bot
        sleep 1
        wipe_data
        deploy_files
        setup_venv
        start_bot
        sleep 2
        check_status
        ;;

    status)
        check_status
        ;;

    logs)
        tail_logs
        ;;

    *)
        echo "Usage: ./deploy_to_pi.sh [start|stop|update|refresh|status|logs]"
        exit 1
        ;;

esac
