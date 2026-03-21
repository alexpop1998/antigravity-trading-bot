#!/bin/bash
# ---------------------------------------------------------
# Antigravity Trading Terminal - Safe Start Script
# ---------------------------------------------------------

# Absolute path to the backend directory
BACKEND_DIR="/Users/alex/.gemini/antigravity/scratch/trading-terminal/backend"
VENV_PYTHON="$BACKEND_DIR/venv/bin/python3"

echo "🚀 Starting Antigravity Trading Terminal..."
echo "🐍 Using Interpreter: $VENV_PYTHON"

if [ ! -f "$VENV_PYTHON" ]; then
    echo "❌ Error: Virtual environment not found at $VENV_PYTHON"
    exit 1
fi

# Run the server
cd "$BACKEND_DIR"
"$VENV_PYTHON" main.py
