import os
import sys
import logging
from dotenv import load_dotenv

# Set up logging to stdout
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ImportTest")

try:
    logger.info("Starting VPS FULL Import Test...")
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    load_dotenv(override=True)
    
    logger.info("Importing core modules...")
    from exchange_gateway import ExchangeGateway
    from safety_shield import SafetyShield
    from strategy_engine import StrategyEngine
    from bot import CryptoBot
    
    logger.info("Importing extended modules from main.py...")
    from whale_tracker import WhaleTracker
    from social_scraper import SocialScraper
    from liquidation_hunter import LiquidationHunter
    from macro_calendar import MacroCalendar
    from rl_tuner import RLTuner
    from dex_sniper import DEXSniper
    from listing_radar import ListingRadar
    from news_radar import NewsRadar
    
    logger.info("ALL Modules imported successfully.")
    logger.info("Import test PASSED.")
except Exception as e:
    logger.error(f"Import test FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
