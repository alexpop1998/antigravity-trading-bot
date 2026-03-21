import asyncio
from bot import CryptoBot
from database import BotDatabase
from telegram_notifier import TelegramNotifier
import logging

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("TestInit")

async def test_init():
    print("Initializing CryptoBot components manually...")
    
    print("Instantiating CryptoBot...")
    bot = CryptoBot()
    
    print("Running bot.initialize() (This is where it used to hang)...")
    try:
        await bot.initialize()
        print("✅ Markets loaded successfully in test script!")
        
        print(f"Validated symbols: {bot.symbols}")
        
        # Test a small exchange call to be sure
        print("Testing fetch_ohlcv for first symbol...")
        if bot.symbols:
            symbol = bot.symbols[0]
            ohlcv = await bot.exchange.fetch_ohlcv(symbol, timeframe='15m', limit=5)
            print(f"✅ Successfully fetched {len(ohlcv)} candles for {symbol}")
        
        await bot.exchange.close()
        print("Test completed successfully.")
        
    except Exception as e:
        print(f"❌ Error during initialization: {e}")
        import traceback
        traceback.print_exc()
        if hasattr(bot, 'exchange'):
            await bot.exchange.close()

if __name__ == "__main__":
    asyncio.run(test_init())
