import ccxt
import os
from dotenv import load_dotenv
import logging

# Load environment
load_dotenv(override=True)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Liquidator")

def liquidate():
    api_key = os.getenv("EXCHANGE_API_KEY")
    api_secret = os.getenv("EXCHANGE_API_SECRET")
    sandbox = os.getenv("BINANCE_SANDBOX", "true").lower() == "true"

    exchange = ccxt.binanceusdm({
        'apiKey': api_key,
        'secret': api_secret,
        'enableRateLimit': True,
        'options': {'defaultType': 'future'}
    })
    exchange.set_sandbox_mode(sandbox)

    logger.info(f"🚀 EMERGENCY LIQUIDATION STARTING (Mode: {'TESTNET' if sandbox else 'PRODUCTION'})")

    try:
        # 1. Cancel all open orders first
        # We need to do this symbol by symbol for Binance Futures or use the global endpoint if available
        # But safest is to fetch positions first.
        
        positions = exchange.fetch_positions()
        active_positions = [p for p in positions if float(p.get('contracts', 0) or p.get('amount', 0) or 0) != 0]

        if not active_positions:
            logger.info("✅ No active positions found to liquidate.")
            return

        logger.warning(f"⚠️ Found {len(active_positions)} active positions. LIQUIDATING...")

        for pos in active_positions:
            symbol = pos['symbol']
            side = pos['side'] # 'long' or 'short'
            amount = abs(float(pos.get('contracts', pos.get('amount', 0))))
            
            logger.info(f"🔥 Closing {side.upper()} {symbol} (Amount: {amount})")
            
            try:
                # To close, we go the opposite way
                order_side = 'sell' if side.lower() == 'long' else 'buy'
                
                # Binance Futures Market Order to close
                exchange.create_market_order(symbol, order_side, amount, params={'reduceOnly': True})
                logger.info(f"✅ {symbol} closed successfully.")
            except Exception as e:
                logger.error(f"❌ Failed to close {symbol}: {e}")

        logger.info("🏁 Liquidation process complete.")

    except Exception as e:
        logger.error(f"💥 FATAL ERROR during liquidation: {e}")

if __name__ == "__main__":
    liquidate()
