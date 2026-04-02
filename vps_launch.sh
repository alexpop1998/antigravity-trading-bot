#!/bin/bash
# Antigravity v31.05 - FINAL STABLE BOOTSTRAPPER
cd /root/antigravity-trading-bot
export PYTHONPATH=/root/antigravity-trading-bot
VENV_PYTHON="/root/antigravity-trading-bot/venv/bin/python3"

# 🧹 Clean existing processes (Aggressive)
echo "🧹 [CLEANUP] Freeing port 8080..."
lsof -t -i:8080 | xargs kill -9 || true
fuser -k -9 8080/tcp || true
sleep 5
pkill -9 -f 'backend/main.py' 2>/dev/null || true
sleep 1

echo "🚀 [BOOT] Starting Trading Bot Cluster..."
nohup $VENV_PYTHON backend/main.py > backend/bot_run.log 2>&1 &
echo "✅ [DONE] Cluster PID: $!"
echo "📊 [LOGS] Tailing for boot indicators..."
sleep 3
cat backend/bot_run.log
