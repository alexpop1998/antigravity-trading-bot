#!/bin/bash
cd /root/antigravity-trading-bot
export PYTHONPATH=$PYTHONPATH:.
source venv/bin/activate
fuser -k 8000/tcp
pkill -f 'backend/main.py'
python3 backend/main.py
