import ccxt.async_support as ccxt
import asyncio
import os
import logging
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("EmergencyCleanup")

async def cleanup():
    load_dotenv(override=True)
    api_key = os.getenv("EXCHANGE_API_KEY")
    api_secret = os.getenv("EXCHANGE_API_SECRET")
    
    exchange = ccxt.binanceusdm({
        'apiKey': api_key,
        'secret': api_secret,
        'enableRateLimit': True,
    })
    
    sandbox_mode = os.getenv("BINANCE_SANDBOX", "true").lower() == "true"
    exchange.set_sandbox_mode(sandbox_mode)
    
    try:
        logger.info(f"Connecting to {'TESTNET' if sandbox_mode else 'MAINNET'}...")
        await exchange.load_markets()
        
        logger.info("Fetching active positions...")
        positions = await exchange.fetch_positions()
        active_pos = [p for p in positions if float(p.get('contracts', 0) or 0) != 0]
        
        if not active_pos:
            logger.info("No active positions found.")
            return

        logger.warning(f"Found {len(active_pos)} active positions. Closing all...")
        
        for pos in active_pos:
            symbol = pos['symbol']
            side = pos['side'].lower()
            close_side = 'sell' if side == 'long' else 'buy'
            amount = float(pos['contracts'])
            
            logger.info(f"Closing {symbol} ({amount} {side})...")
            try:
                await exchange.create_order(symbol, 'market', close_side, amount, params={'reduceOnly': True})
                logger.info(f"✅ Closed {symbol}")
            except Exception as e:
                logger.error(f"❌ Failed to close {symbol}: {e}")
        
        logger.info("🎉 All positions closed.")
        
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")
    finally:
        await exchange.close()

if __name__ == "__main__":
    asyncio.run(cleanup())
