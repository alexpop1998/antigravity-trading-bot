import ccxt
import os
from dotenv import load_dotenv

load_dotenv(override=True)

def diagnostic_check():
    active_exchange = os.getenv("ACTIVE_EXCHANGE", "binance").lower()
    
    if active_exchange == "bitget":
        api_key = os.getenv("BITGET_API_KEY")
        api_secret = os.getenv("BITGET_API_SECRET")
        password = os.getenv("BITGET_PASSWORD")
        
        print(f"DEBUG: Using BITGET API Key starting with: {api_key[:5]}...")
        
        exchange = ccxt.bitget({
            'apiKey': api_key,
            'secret': api_secret,
            'password': password,
            'enableRateLimit': True,
            'options': {'defaultType': 'swap'}
        })
    else:
        api_key = os.getenv("EXCHANGE_API_KEY")
        api_secret = os.getenv("EXCHANGE_API_SECRET")
        print(f"DEBUG: Using BINANCE API Key starting with: {api_key[:5]}...")
        
        exchange = ccxt.binanceusdm({
            'apiKey': api_key,
            'secret': api_secret,
            'enableRateLimit': True,
        })
        sandbox_mode = os.getenv("BINANCE_SANDBOX", "true").lower() == "true"
        exchange.set_sandbox_mode(sandbox_mode)
        print(f"DEBUG: Binance Sandbox mode: {sandbox_mode}")

    print(f"\n[1] Testing {active_exchange.upper()} Connection...")
    try:
        balance = exchange.fetch_balance()
        print(f"✅ Connection OK.")
        
        # Print actual balance
        usdt_balance = balance.get('USDT', {})
        total = usdt_balance.get('total', 0)
        free = usdt_balance.get('free', 0)
        print(f"💰 USDT Balance: Total={total}, Free={free}")
        
        if active_exchange == 'bitget':
             # Try to fetch positions to verify futures access
             positions = exchange.fetch_positions()
             print(f"📊 Active Positions: {len(positions)}")
             for pos in positions:
                 if float(pos.get('amount', pos.get('contracts', 0) or 0)) != 0:
                     leverage = pos.get('leverage', 1)
                     side = pos['side']
                     entry = float(pos['entryPrice'])
                     mark = float(pos['markPrice'])
                     pnl_pct = (mark - entry) / entry if side == 'long' else (entry - mark) / entry
                     roe = pnl_pct * float(leverage) * 100
                     print(f"  - {pos['symbol']}: {side} {pos['contracts']} @ {entry} (Mark: {mark}) | Lev: {leverage}x | ROE: {roe:.2f}%")
             
    except Exception as e:
        print(f"❌ Connection Error: {e}")

if __name__ == "__main__":
    diagnostic_check()
