import ccxt.async_support as ccxt
import asyncio
import os
import logging
from dotenv import load_dotenv
from collections import defaultdict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("BitgetCleanup")

async def cleanup():
    load_dotenv(override=True)
    
    exchange = ccxt.bitget({
        'apiKey': os.getenv('BITGET_API_KEY'),
        'secret': os.getenv('BITGET_API_SECRET'),
        'password': os.getenv('BITGET_PASSWORD'),
        'options': {'defaultType': 'swap'}
    })
    
    try:
        logger.info("Connecting to Bitget...")
        await exchange.load_markets()
        positions = await exchange.fetch_positions()
        
        active_list = []
        for p in positions:
            # v43.3.10 [BITGET FIX] Use 'contracts' as primary, fallback to 'amount' or 'info.total'
            amt = abs(float(p.get('contracts') or p.get('amount') or p.get('info', {}).get('total', 0) or 0))
            if amt > 0:
                active_list.append(p)
        
        if not active_list:
            logger.info("No active positions found.")
            return

        # Group by symbol
        by_symbol = defaultdict(list)
        for p in active_list:
            by_symbol[p['symbol']].append(p)

        target_leverage = 15 # Minimum for Blitz
        
        for symbol, pos_list in by_symbol.items():
            logger.info(f"Checking {symbol} (Found {len(pos_list)} directions)")
            
            # 1. Fix Leverage for all directions
            for p in pos_list:
                side = p['side']
                current_lev = float(p.get('leverage', 0))
                if current_lev < target_leverage:
                    logger.warning(f"🔧 [LEVERAGE] Updating {symbol} {side} from {current_lev}x to {target_leverage}x")
                    try:
                        await exchange.set_leverage(target_leverage, symbol, params={'holdSide': side, 'marginCoin': 'USDT'})
                    except Exception as e:
                        logger.error(f"Failed to set leverage for {symbol}: {e}")

            # 2. Close opposite if multiple exist (Anti-Hedge)
            if len(pos_list) > 1:
                logger.warning(f"⚠️ [HEDGE] Conflicting positions on {symbol}. Closing the smaller one or SHORT if equal.")
                # Strategy: Keep the one with larger value/notional or just the first one for simplicity
                # Here we keep the first one and close the rest
                primary = pos_list[0]
                for extra in pos_list[1:]:
                    e_side = extra['side']
                    e_amt = float(extra.get('contracts', extra.get('amount', 0)))
                    logger.warning(f"🛑 [CLOSE] Closing {symbol} {e_side} {e_amt}...")
                    try:
                        # Market close
                        close_side = 'sell' if e_side == 'long' else 'buy'
                        await exchange.create_order(symbol, 'market', close_side, e_amt, params={'reduceOnly': True})
                        logger.info(f"✅ Closed {symbol} {e_side}")
                    except Exception as e:
                        logger.error(f"Failed to close {symbol} {e_side}: {e}")
        
        logger.info("🎉 Bitget Cleanup Complete.")
        
    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        await exchange.close()

if __name__ == "__main__":
    asyncio.run(cleanup())
