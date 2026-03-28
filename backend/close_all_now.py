import asyncio
import os
import logging
from bot import CryptoBot

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("EmergencyKill")

async def main():
    print("🆘 EMERGENCY KILL SWITCH ACTIVATED")
    print("This script will attempt to close ALL open positions on the exchange.")
    
    bot = CryptoBot()
    # Initialize without leverage setting to be faster
    try:
        await bot.exchange.load_markets()
        print("✅ Markets loaded.")
        
        await bot.emergency_cleanup_all()
        print("🏁 EMERGENCY CLEANUP COMPLETE.")
        
    except Exception as e:
        print(f"❌ CRITICAL ERROR DURING CLEANUP: {e}")
    finally:
        await bot.exchange.close()

if __name__ == "__main__":
    asyncio.run(main())
