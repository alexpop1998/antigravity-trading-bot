
import asyncio
import os
import sys
from dotenv import load_dotenv
import ccxt.async_support as ccxt

async def close_positions():
    load_dotenv()
    
    exchange = ccxt.bitget({
        'apiKey': os.getenv('BITGET_API_KEY'),
        'secret': os.getenv('BITGET_API_SECRET'),
        'password': os.getenv('BITGET_PASSWORD'),
        'enableRateLimit': True,
        'options': {'defaultType': 'swap'}
    })
    
    try:
        print("🔍 Recupero posizioni da Bitget...")
        positions = await exchange.fetch_positions()
        active = [p for p in positions if float(p.get('amount', p.get('contracts', 0) or 0)) != 0]
        
        for pos in active:
            symbol = pos['symbol']
            side = pos['side'].lower()
            amount = abs(float(pos.get('amount', pos.get('contracts', 0))))
            
            print(f"🚀 Chiusura d'emergenza per {symbol} ({side} {amount})...")
            close_side = 'sell' if side == 'long' else 'buy'
            order = await exchange.create_order(symbol, 'market', close_side, amount, None, {'reduceOnly': True})
            print(f"✅ Ordine inviato: {order['id'] if isinstance(order, dict) else 'OK'}")
                
    except Exception as e:
        print(f"❌ Errore: {e}")
    finally:
        await exchange.close()

if __name__ == "__main__":
    asyncio.run(close_positions())
