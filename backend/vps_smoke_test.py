import os
import sys
import logging
from dotenv import load_dotenv

# Set up logging to stdout
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SmokeTest")

try:
    logger.info("Starting VPS Smoke Test...")
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    load_dotenv(override=True)
    
    from exchange_gateway import ExchangeGateway
    from safety_shield import SafetyShield
    from strategy_engine import StrategyEngine
    from bot import CryptoBot
    
    logger.info("Modules imported successfully.")
    
    bot = CryptoBot()
    logger.info(f"Bot instance created. Profile: {bot.profile_type}")
    
    # Try exchange load markets
    import asyncio
    async def test_init():
        logger.info("Initializing bot...")
        await bot.initialize()
        logger.info("Bot initialized successfully!")
        await bot.gateway.close()

    asyncio.run(test_init())
    logger.info("Smoke test PASSED.")
except Exception as e:
    logger.error(f"Smoke test FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
