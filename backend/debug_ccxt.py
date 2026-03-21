import asyncio
import ccxt.async_support as ccxt
import os
from dotenv import load_dotenv
import decimal

async def debug():
    load_dotenv()
    exchange = ccxt.binanceusdm({
        'apiKey': os.getenv('BINANCE_API_KEY'),
        'secret': os.getenv('BINANCE_API_SECRET'),
        'enableRateLimit': True,
        'options': {'defaultType': 'future'}
    })
    exchange.set_sandbox_mode(True)
    
    print("--- Testing Balance ---")
    try:
        balance = await exchange.fetch_balance()
        print(f"Keys in balance: {list(balance.keys())}")
        print(f"Total Wallet Balance (USDT): {balance.get('total', {}).get('USDT') or balance.get('USDT', {}).get('total')}")
        # Binance Futures balance structure in ccxt:
        print(f"Info keys: {list(balance['info'].keys()) if 'info' in balance else 'No info'}")
    except Exception as e:
        print(f"Balance error: {e}")

    print("\n--- Testing Precision and Orders ---")
    symbol = 'SXP/USDT:USDT'
    try:
        await exchange.load_markets()
        market = exchange.market(symbol)
        print(f"Market for {symbol}: precision={market['precision']}, amount_min={market['limits']['amount']['min']}")
        
        amount = 18450.1
        amount_str = exchange.amount_to_precision(symbol, amount)
        print(f"Amount {amount} to precision -> '{amount_str}' (type: {type(amount_str)})")
        
        # Test Decimal parsing of that string
        try:
            d = decimal.Decimal(amount_str)
            print(f"Decimal parsed successfully: {d}")
        except Exception as de:
            print(f"Decimal parsing FAILED: {de}")
            
    except Exception as e:
        print(f"Market/Order test error: {e}")

    await exchange.close()

if __name__ == "__main__":
    asyncio.run(debug())
