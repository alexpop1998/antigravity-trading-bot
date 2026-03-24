import ccxt.async_support as ccxt
import asyncio
import os
from dotenv import load_dotenv

async def check_limits():
    load_dotenv(override=True)
    
    api_key = os.getenv("BITGET_API_KEY")
    api_secret = os.getenv("BITGET_API_SECRET")
    password = os.getenv("BITGET_PASSWORD")
    
    exchange = ccxt.bitget({
        'apiKey': api_key,
        'secret': api_secret,
        'password': password,
    })
    
    try:
        await exchange.load_markets()
        symbol = 'BTC/USDT:USDT'
        if symbol in exchange.markets:
            market = exchange.market(symbol)
            print(f"--- Limits for {symbol} ---")
            print(f"Min Amount (Contracts): {market['limits']['amount']['min']}")
            print(f"Min Cost (USDT): {market['limits']['cost']['min']}")
            
            ticker = await exchange.fetch_ticker(symbol)
            price = ticker['last']
            min_notional = float(market['limits']['amount']['min'] or 0) * price
            print(f"Current Price: {price}")
            print(f"Min Notional Value: {min_notional:.4f} USDT")
            
            balance = await exchange.fetch_balance()
            total_usdt = balance.get('USDT', {}).get('total', 0)
            print(f"Account Balance: {total_usdt} USDT")
            
            if total_usdt < min_notional:
                print("❌ ERROR: Balance too low for minimum order size.")
            else:
                print("✅ TEST: Attempting small order...")
                # We won't actually do it yet without user confirmation if it hits real money
        else:
            print(f"Symbol {symbol} not found on Bitget.")
            
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        await exchange.close()

if __name__ == "__main__":
    asyncio.run(check_limits())
