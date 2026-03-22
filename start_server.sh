#!/bin/bash
# ---------------------------------------------------------
# Antigravity Trading Terminal - Safe Start Script
# ---------------------------------------------------------

# Absolute path to the backend directory
BACKEND_DIR="/Users/alex/.gemini/antigravity/scratch/trading-terminal/backend"
VENV_PYTHON="$BACKEND_DIR/venv/bin/python3"
ENV_FILE="$BACKEND_DIR/.env"

echo "🚀 Starting Antigravity Trading Terminal..."

# 1. Check for Virtual Environment
if [ ! -f "$VENV_PYTHON" ]; then
    echo "❌ Error: Virtual environment not found at $VENV_PYTHON"
    echo "💡 Run './setup_env.sh' to initialize the environment."
    exit 1
fi

# 2. Check for .env Configuration
if [ ! -f "$ENV_FILE" ]; then
    echo "❌ Error: .env configuration file missing in $BACKEND_DIR"
    echo "💡 Run './setup_env.sh' to create a template, then fill in your API keys."
    exit 1
fi

echo "🐍 Using Interpreter: $VENV_PYTHON"

# Run the server
cd "$BACKEND_DIR"
"$VENV_PYTHON" main.py
