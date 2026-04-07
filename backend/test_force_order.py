import asyncio
import os
import sys
import logging
import json
import time
from dotenv import load_dotenv

# Ensure we can import from the current directory
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Silence verbose logging for the test script itself, but keep bot logs visible
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("TestOpening")

from bot import CryptoBot

async def test_opening():
    logger.info("🚀 [TEST] Initializing Diagnostic Test Opening (v44.1.0)...")
    
    # Ensure .env is loaded
    load_dotenv(override=True)
    os.environ["CONFIG_PROFILE"] = "blitz" # Force blitz
    
    bot = CryptoBot()
    
    try:
        logger.info("📡 Connecting to exchange...")
        await bot.gateway.exchange.load_markets()
        
        # --- [FIX] Mock Strategy Engine / Analyst ---
        async def mock_decide_strategy(*args, **kwargs):
            logger.warning("🦾 [Test Bypass] Mocking LLM Analyst approval...")
            # (approved, confidence, strength, leverage, sl_mult, tp_mult, tp_p, reason, side)
            return True, 0.95, 1.0, 25, 1.0, 1.0, None, "TEST_FORCE_APPROVAL", "buy"
        
        bot.strategy.analyst.decide_strategy = mock_decide_strategy
        # --------------------------------------------------------
        
        # Select BTC for the test
        symbol = "BTC/USDT:USDT"
        if symbol not in bot.gateway.exchange.markets:
             symbol = next((k for k in bot.gateway.exchange.markets.keys() if "BTC/USDT" in k), None)
        
        if not symbol:
            logger.error("❌ BTC/USDT not found. Aborting.")
            return

        logger.info(f"🔍 [TEST] Fetching current price for {symbol}...")
        ticker = await bot.gateway.exchange.fetch_ticker(symbol)
        price = float(ticker.get('last') or ticker.get('close'))
        logger.info(f"💰 Current Price: {price} USDT")
        
        # Update bot data state so execute_order has price
        bot.latest_data[symbol] = {'price': price}
        
        # Manually trigger execute_order with correct signature
        logger.info(f"🔥 [EXECUTION] Forcing Market BUY for {symbol}...")
        test_analysis = {
            'score': 0.99,
            'side': 'buy',
            'leverage': 25,
            'reference_price': price,
            'confidence': 0.95
        }
        
        await bot.execute_order(symbol, 'buy', test_analysis)
        
        logger.info("⏳ Waiting for 10 seconds for position confirmation...")
        await asyncio.sleep(10)
        
        # Verify real exchange positions
        pos_all = await bot.gateway.fetch_positions_robustly()
        exchange_pos = [p for p in pos_all if p['symbol'] == symbol]
        
        if exchange_pos:
            logger.info(f"✅ Position detected! Amount: {exchange_pos[0].get('amount')}")
            
            logger.info(f"🆘 [CLOSURE] Closing test position for {symbol}...")
            await bot.gateway.close_all_for_symbol(symbol)
            logger.info("✅ Position closed successfully.")
        else:
            logger.error("❌ No active position detected after execute_order. Check Bitget logs.")
            
    except Exception as e:
        logger.error(f"❌ [ERROR] Diagnostic test failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if hasattr(bot, 'gateway') and bot.gateway:
            await bot.gateway.exchange.close()
        logger.info("🏁 Test finished.")

if __name__ == "__main__":
    asyncio.run(test_opening())
