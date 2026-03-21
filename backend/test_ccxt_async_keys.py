import ccxt.async_support as ccxt
import asyncio
import time
import os
from dotenv import load_dotenv

load_dotenv(override=True)

async def test():
    try:
        api_key = os.getenv("EXCHANGE_API_KEY")
        api_secret = os.getenv("EXCHANGE_API_SECRET")
        sandbox_mode = os.getenv("BINANCE_SANDBOX", "true").lower() == "true"
        
        print(f"Creating async exchange (Sandbox={sandbox_mode})...")
        exchange = ccxt.binance({
            'apiKey': api_key,
            'secret': api_secret,
            'enableRateLimit': True,
            'verbose': True,
            'timeout': 10000,
            'options': {'defaultType': 'future'}
        })
        exchange.set_sandbox_mode(sandbox_mode)
        
        print("Loading markets (async with keys)...")
        start = time.time()
        markets = await exchange.load_markets()
        end = time.time()
        print(f"Loaded {len(markets)} markets in {end - start:.2f}s")
        await exchange.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(test())
