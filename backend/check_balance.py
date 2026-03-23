import ccxt
import os
from dotenv import load_dotenv

load_dotenv()

def diagnostic_check():
    api_key = os.getenv("EXCHANGE_API_KEY")
    api_secret = os.getenv("EXCHANGE_API_SECRET")
    
    print(f"DEBUG: Using API Key starting with: {api_key[:5]}...")
    
    # 1. Test Spot Connection (Simplest)
    print("\n[1] Testing SPOT Wallet Connection...")
    spot_exchange = ccxt.binance({
        'apiKey': api_key,
        'secret': api_secret,
        'enableRateLimit': True,
        'options': {'defaultType': 'spot'}
    })
    
    sandbox_mode = os.getenv("BINANCE_SANDBOX", "true").lower() == "true"
    spot_exchange.set_sandbox_mode(sandbox_mode)
    print(f"DEBUG: Sandbox mode: {sandbox_mode}")
    
    try:
        spot_exchange.fetch_balance()
        print("✅ SPOT Connection OK.")
    except Exception as e:
        print(f"❌ SPOT Error: {e}")

    # 2. Test Futures Connection
    print("\n[2] Testing FUTURES Wallet Connection...")
    futures_exchange = ccxt.binance({
        'apiKey': api_key,
        'secret': api_secret,
        'enableRateLimit': True,
        'options': {'defaultType': 'future'}
    })
    futures_exchange.set_sandbox_mode(sandbox_mode)
    
    try:
        futures_exchange.fetch_balance()
        print("✅ FUTURES Connection OK.")
    except Exception as e:
        print(f"❌ FUTURES Error: {e}")
        
    # 3. Test generic account info (Permissions check)
    try:
        permissions = futures_exchange.fetch_api_key_permissions()
        print(f"\n[3] API Permissions: {permissions}")
    except Exception as e:
        print(f"\n[3] Could not fetch permissions: {e}")

if __name__ == "__main__":
    diagnostic_check()
