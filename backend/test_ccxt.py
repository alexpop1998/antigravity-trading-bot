import ccxt
import time

def test():
    try:
        print("Creating exchange...")
        exchange = ccxt.binanceusdm({
            'enableRateLimit': True,
            'verbose': True,
            'timeout': 10000
        })
        exchange.set_sandbox_mode(True)
        print("Loading markets...")
        start = time.time()
        markets = exchange.load_markets()
        end = time.time()
        print(f"Loaded {len(markets)} markets in {end - start:.2f}s")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test()
