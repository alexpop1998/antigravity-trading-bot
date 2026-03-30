#!/bin/bash
# ---------------------------------------------------------
# Antigravity Trading Terminal - Safe Start Script
# ---------------------------------------------------------

# Dynamic path to the backend directory (works locally & on VPS)
BACKEND_DIR="$(cd "$(dirname "$0")" && pwd)/backend"
VENV_PYTHON="$BACKEND_DIR/venv/bin/python3"
ENV_FILE="$BACKEND_DIR/.env"

echo "🚀 Starting Antigravity Trading Terminal..."

# 1. Check for Virtual Environment
if [ ! -f "$VENV_PYTHON" ]; then
    echo "❌ Error: Virtual environment not found at $VENV_PYTHON"
    echo "💡 Run './setup_env.sh' to initialize the environment."
    exit 1
fi

# 2. Strict Process Cleanup (Prevent Ghost Instances)
# Kill any existing main.py, bot.py or uvicorn instances to avoid port 8000 conflicts and dual-trading.
echo "🧹 Cleaning up existing processes..."
pkill -9 -f "main.py" 2>/dev/null
pkill -9 -f "python3 bot.py" 2>/dev/null
pkill -9 -f "uvicorn" 2>/dev/null
sleep 2

# 3. Check for .env Configuration
if [ ! -f "$ENV_FILE" ]; then
    echo "❌ Error: .env configuration file missing in $BACKEND_DIR"
    echo "💡 Run './setup_env.sh' to create a template, then fill in your API keys."
    exit 1
fi

echo "🐍 Using Interpreter: $VENV_PYTHON"

# Run the server
cd "$BACKEND_DIR"
# Use nohup or just backgrounding to ensure it stays up
nohup "$VENV_PYTHON" main.py > ../start_output.log 2>&1 &

echo "✅ Antigravity Terminal started in background (PID: $!)"
echo "📂 Logs available at start_output.log"
