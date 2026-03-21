import httpx
import asyncio
import json

BASE_URL = "http://localhost:8000"

async def test_all():
    print("🚀 Starting Comprehensive Bot Functionality Test...")
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        # 1. Test MiroFish (Positive News)
        print("\n--- Testing MiroFish (Positive) ---")
        try:
            resp = await client.post(f"{BASE_URL}/api/test-news", json={
                "title": "SEC Approves Spot Bitcoin ETF for All Institutional Banks",
                "content": "A historic day for crypto as the SEC finally gives the green light..."
            })
            print(f"Status: {resp.status_code}, Response: {resp.json()}")
        except Exception as e:
            print(f"Error: {e}")

        # 2. Test MiroFish (Negative News)
        print("\n--- Testing MiroFish (Negative) ---")
        try:
            resp = await client.post(f"{BASE_URL}/api/test-news", json={
                "title": "Major Exchange Hacked: $1 Billion in Assets Drained",
                "content": "Users are advised to withdraw funds immediately as hackers exploit a critical vulnerability..."
            })
            print(f"Status: {resp.status_code}, Response: {resp.json()}")
        except Exception as e:
            print(f"Error: {e}")

        # 3. Test Whale Tracker
        print("\n--- Testing Whale Tracker ---")
        try:
            resp = await client.post(f"{BASE_URL}/api/test-whale", json={
                "btc_amount": 1500.0
            })
            print(f"Status: {resp.status_code}, Response: {resp.json()}")
        except Exception as e:
            print(f"Error: {e}")

        # 4. Test Liquidation Hunter
        print("\n--- Testing Liquidation Hunter ---")
        try:
            resp = await client.post(f"{BASE_URL}/api/test-liquidation")
            print(f"Status: {resp.status_code}, Response: {resp.json()}")
        except Exception as e:
            print(f"Error: {e}")

        # 5. Test LLM Analysis
        print("\n--- Testing LLM Analysis ---")
        try:
            resp = await client.post(f"{BASE_URL}/api/analyze", json={
                "ticker": "BTC/USDT",
                "price": 65000.0,
                "rsi": 25.0
            })
            print(f"Status: {resp.status_code}, Response: {resp.json()}")
        except Exception as e:
            print(f"Error: {e}")

    print("\n✅ All tests triggered. Check the backend logs for trading execution details.")

if __name__ == "__main__":
    asyncio.run(test_all())
