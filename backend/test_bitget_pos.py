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

try:
    positions = exchange.fetch_positions()
    for p in positions:
        if float(p.get('contracts', 0)) != 0 or float(p.get('amount', 0)) != 0:
            print(f"--- {p['symbol']} ---")
            print(f"Side: {p['side']}")
            print(f"Entry: {p.get('entryPrice')}")
            print(f"Avg: {p.get('averagePrice')}")
            print(f"Mark: {p.get('markPrice')}")
            print(f"Lev: {p.get('leverage')}")
            print(f"Amount: {p.get('amount')}")
            print(f"Info Keys: {p.get('info', {}).keys()}")
            print(f"Entry from Info: {p.get('info', {}).get('entryPrice')}")
except Exception as e:
    print(f"Error: {e}")
