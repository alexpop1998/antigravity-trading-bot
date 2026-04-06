import asyncio
import os
import sys
import logging
import json
from dotenv import load_dotenv

# Ensure we can import from the current directory
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Silence verbose logging for the test script itself, but keep bot logs visible
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("TestOpening")

from bot import CryptoBot

async def test_opening():
    logger.info("🚀 [TEST] Initializing Diagnostic Test Opening...")
    
    # Force profile to blitz for the test
    os.environ["CONFIG_PROFILE"] = "blitz"
    
    # Ensure .env is loaded
    load_dotenv(override=True)
    
    bot = CryptoBot(config_file="config_blitz.json")
    
    try:
        logger.info("📡 Connecting to exchange...")
        await bot.initialize()
        
        # --- [NEW] Mock LLM Analyst to Force Approval for Test ---
        async def mock_decide_strategy(*args, **kwargs):
            logger.warning("🦾 [Test Bypass] Mocking LLM Analyst approval...")
            return True, 1.0, bot.leverage, 1.0, 1.0, None, "TEST_FORCE_APPROVAL"
        
        bot.analyst.decide_strategy = mock_decide_strategy
        # --------------------------------------------------------
        
        # Select BTC for the test
        symbol = "BTC/USDT:USDT"
        
        # Verify symbol exists in markets
        if symbol not in bot.exchange.markets:
            logger.warning(f"❌ Symbol {symbol} not found on Bitget. Checking alternatives...")
            btc_markets = [s for s in bot.exchange.markets.keys() if "BTC/USDT" in s]
            if btc_markets:
                symbol = btc_markets[0]
                logger.info(f"✅ Found alternative: {symbol}")
            else:
                logger.error("❌ BTC/USDT not found. Aborting.")
                return

        logger.info(f"🔍 [TEST] Fetching current price for {symbol}...")
        ticker = await bot.exchange.fetch_ticker(symbol)
        price = float(ticker.get('last') or ticker.get('close'))
        logger.info(f"💰 Current Price: {price} USDT")
        
        # --- [NEW] Check Indicators for Analysis ---
        indicators = bot.latest_data.get(symbol, {})
        logger.info(f"📊 Indicators for {symbol}: {json.dumps(indicators, indent=2)}")
        
        # Manually trigger execute_order
        logger.info(f"🔥 [EXECUTION] Forcing Market BUY for {symbol}...")
        # consensus_score=10 ensures it passes most internal filters
        await bot.execute_order(
            symbol=symbol, 
            side='buy', 
            current_price=price, 
            consensus_score=10.0, 
            signal_type='MANUAL_TEST'
        )
        
        logger.info("⏳ Waiting for 10 seconds for position confirmation...")
        await asyncio.sleep(10)
        
        # Refresh account state to verify position
        await bot._update_account_state()
        
        # Check active positions in bot state
        is_active = bot.active_positions.get(symbol) == 'LONG'
        
        # Check real exchange positions via latest_account_data
        exchange_pos = [p for p in bot.latest_account_data.get('positions', []) 
                       if (p.get('symbol') == symbol or p.get('instId') == bot.exchange.market(symbol)['id'])
                       and float(p.get('contracts', p.get('amount', p.get('total', 0)))) != 0]
        
        if is_active or exchange_pos:
            logger.info(f"✅ Position detected! Bot State: {bot.active_positions.get(symbol)}, Exchange Scan: {len(exchange_pos)} positions.")
            
            trade = bot.trade_levels.get(symbol)
            if trade:
                logger.info(f"📦 [TRADE DATA] Amount: {trade.get('amount')}, Entry: {trade.get('entry_price')}")
            
            logger.info(f"🆘 [CLOSURE] Closing position for {symbol}...")
            # If trade is missing, we create a dummy one for the closer
            if not trade:
                trade = {'side': 'buy', 'amount': abs(float(exchange_pos[0].get('contracts', exchange_pos[0].get('amount', 1.0)))) if exchange_pos else 1.0}
            
            await bot.close_position(symbol, trade, reason="MANUAL_TEST_COMPLETED")
            logger.info("✅ Position closed successfully.")
        else:
            logger.error("❌ No active position detected after execute_order.")
            
    except Exception as e:
        logger.error(f"❌ [ERROR] Diagnostic test failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if hasattr(bot, 'exchange') and bot.exchange:
            await bot.exchange.close()
        logger.info("🏁 Test finished.")

if __name__ == "__main__":
    asyncio.run(test_opening())
