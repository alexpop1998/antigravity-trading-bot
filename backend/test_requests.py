import requests
import time

def test():
    try:
        url = "https://testnet.binancefuture.com/fapi/v1/exchangeInfo"
        print(f"Testing requests GET {url}")
        start = time.time()
        r = requests.get(url, timeout=10)
        end = time.time()
        print(f"Status: {r.status_code}, Time: {end - start:.2f}s, Content Length: {len(r.content)}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test()
