import ccxt.async_support as ccxt
import asyncio
import os
from dotenv import load_dotenv

async def test_connection():
    load_dotenv(override=True)
    
    api_key = os.getenv("BITGET_API_KEY")
    api_secret = os.getenv("BITGET_API_SECRET")
    password = os.getenv("BITGET_PASSWORD")
    
    print(f"Testing Bitget connection with Key: {api_key[:6]}...")
    
    exchange = ccxt.bitget({
        'apiKey': api_key,
        'secret': api_secret,
        'password': password,
        'enableRateLimit': True
    })
    
    # We will try to fetch balance to verify keys
    try:
        # Check if markets load
        print("Loading markets...")
        await exchange.load_markets()
        print("✅ Markets loaded successfully.")
        
        # Check balance
        print("Fetching balance...")
        balance = await exchange.fetch_balance()
        print("✅ Connection successful!")
        print(f"Wallet Balance (USDT): {balance.get('USDT', {}).get('total', 0)}")
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
    finally:
        await exchange.close()

if __name__ == "__main__":
    asyncio.run(test_connection())
