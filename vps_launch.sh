#!/bin/bash
# Antigravity v31.02 - Ultra-Stable Bootstrapper
cd /root/antigravity-trading-bot
export PYTHONPATH=$PYTHONPATH:.
source venv/bin/activate

echo "🧹 [CLEANUP] Freeing ports and killing old instances..."
fuser -k 8080/tcp 2>/dev/null || true
pkill -9 -f 'backend/main.py' 2>/dev/null || true
sleep 1

echo "🚀 [BOOT] Starting Trading Bot Cluster (Port 8080)..."
nohup python3 backend/main.py > /root/antigravity-trading-bot/backend/bot_run.log 2>&1 &
echo "✅ [DONE] Process PID: $!"
echo "📊 [LOGS] Tailing log for first indicators..."
sleep 2
tail -n 20 /root/antigravity-trading-bot/backend/bot_run.log
