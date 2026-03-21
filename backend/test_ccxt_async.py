import ccxt.async_support as ccxt
import asyncio
import time

async def test():
    try:
        print("Creating async exchange...")
        exchange = ccxt.binance({
            'enableRateLimit': True,
            'verbose': True,
            'timeout': 10000,
            'options': {'defaultType': 'future'}
        })
        exchange.set_sandbox_mode(True)
        print("Loading markets (async)...")
        start = time.time()
        markets = await exchange.load_markets()
        end = time.time()
        print(f"Loaded {len(markets)} markets in {end - start:.2f}s")
        await exchange.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(test())
