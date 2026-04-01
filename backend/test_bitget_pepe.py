import ccxt
import os
from dotenv import load_dotenv

load_dotenv()

exchange = ccxt.bitget({
    'apiKey': os.getenv('BITGET_API_KEY'),
    'secret': os.getenv('BITGET_API_SECRET'),
    'password': os.getenv('BITGET_PASSWORD'),
    'options': {'defaultType': 'swap'}
})

symbol = "PEPE/USDT:USDT"
try:
    bal = exchange.fetch_balance()
    if isinstance(bal, list):
        info = bal[0].get('info', {}) if len(bal) > 0 else {}
    else:
        info = bal.get('info', {})
        
    positions = info.get('positions', [])
    for p in positions:
        if p.get('symbol') == symbol or p.get('symbol') == "PEPEUSDT_UMCBL":
            print(f"🔥 Symbol: {p.get('symbol')} | Leverage: {p.get('leverage')} | Side: {p.get('holdSide')}")
except Exception as e:
    print(f"Error: {e}")
