import asyncio
import logging
import time
from typing import Set

logger = logging.getLogger("ListingRadar")

class ListingRadar:
    def __init__(self, bot_instance):
        self.bot = bot_instance
        self.exchange = bot_instance.exchange
        self.known_symbols: Set[str] = set()
        self.is_running = False

    async def initialize(self):
        """Initial catch-up of all currently available symbols."""
        try:
            logger.info("📡 Initializing Listing Radar: Caching current markets...")
            markets = await self.exchange.fetch_markets()
            # Filter for USDT-M Perpetual Futures
            self.known_symbols = {
                m['symbol'] for m in markets 
                if m['active'] and m.get('linear') and m.get('settle') == 'USDT'
            }
            logger.info(f"✅ Cached {len(self.known_symbols)} existing USDT-M markets.")
        except Exception as e:
            logger.error(f"❌ Failed to initialize Listing Radar: {e}")

    async def start_polling(self, interval_seconds: int = 20):
        """Periodically polls for new market listings."""
        self.is_running = True
        logger.info(f"🚀 Listing Radar polling started (Interval: {interval_seconds}s)")
        
        while self.is_running:
            try:
                # Reload markets from exchange (bypass cache if possible)
                markets = await self.exchange.fetch_markets()
                current_symbols = {
                    m['symbol'] for m in markets 
                    if m['active'] and m.get('linear') and m.get('settle') == 'USDT'
                }
                
                new_listings = current_symbols - self.known_symbols
                
                if new_listings:
                    for symbol in new_listings:
                        logger.warning(f"🚨 [NEW LISTING DETECTED] {symbol} is now available!")
                        # Trigger immediate HIGH PRIORITY signal
                        # Listing scalps bypass standard consensus
                        asyncio.create_task(self.bot.handle_signal(
                            symbol, 
                            "NEW_LISTING", 
                            "buy", 
                            is_black_swan=True # Bypasses AI review and some filters
                        ))
                    
                    # Update cache to include new ones
                    self.known_symbols.update(new_listings)
                
            except Exception as e:
                logger.error(f"❌ Listing Radar Error: {e}")
            
            await asyncio.sleep(interval_seconds)

    def stop(self):
        self.is_running = False
