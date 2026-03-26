import httpx
import asyncio

BASE_URL = "http://localhost:8000"

async def test_all():
    print("🚀 Starting Comprehensive Bot Functionality Test...")
    
    async with httpx.AsyncClient(timeout=30.0) as client:

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
