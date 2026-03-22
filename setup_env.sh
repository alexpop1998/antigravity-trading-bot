#!/bin/bash
# ---------------------------------------------------------
# Antigravity Trading Terminal - Environment Setup Script
# ---------------------------------------------------------

BASE_DIR=$(pwd)
BACKEND_DIR="$BASE_DIR/backend"
VENV_PATH="$BACKEND_DIR/venv"

echo "🔍 Checking Python Environment..."

# 1. Check if venv exists
if [ ! -d "$VENV_PATH" ]; then
    echo "📦 Creating virtual environment in $VENV_PATH..."
    python3 -m venv "$VENV_PATH"
else
    echo "✅ Virtual environment found."
fi

# 2. Install/Update dependencies
echo "📝 Installing/Updating dependencies from requirements.txt..."
"$VENV_PATH/bin/pip" install --upgrade pip
"$VENV_PATH/bin/pip" install -r "$BACKEND_DIR/requirements.txt"

# 3. Check for .env file
if [ ! -f "$BACKEND_DIR/.env" ]; then
    echo "⚠️ .env file not found. Creating a template..."
    cat <<EOT > "$BACKEND_DIR/.env"
# Exchange API Configuration
EXCHANGE_API_KEY=YOUR_BINANCE_API_KEY
EXCHANGE_API_SECRET=YOUR_BINANCE_API_SECRET
BINANCE_SANDBOX=true

# LLM Configuration (OpenAI/Gemini)
LLM_API_KEY=YOUR_LLM_KEY
LLM_BASE_URL=https://api.openai.com/v1
LLM_MODEL_NAME=gpt-4-o

# Telegram Notification (Optional)
TELEGRAM_BOT_TOKEN=YOUR_BOT_TOKEN
TELEGRAM_CHAT_ID=YOUR_CHAT_ID

# Database Configuration
DATABASE_PATH=bot_data.db
EOT
    echo "🚀 Template .env created at backend/.env"
    echo "👉 Please edit it with your API keys."
else
    echo "✅ .env file found."
fi

echo "✨ Environment setup complete!"
echo "🚀 You can now start the server with: ./start_server.sh"
