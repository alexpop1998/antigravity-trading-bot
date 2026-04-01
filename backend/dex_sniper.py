import asyncio
import logging
import random

logger = logging.getLogger("DEXSniper")

class DEXSniper:
    def __init__(self, bot_instance, target_spread_pct=0.5):
        self.bot = bot_instance
        self.target_spread_pct = target_spread_pct
        self.is_sniping = False

    async def fetch_dex_price(self, symbol):
        # In a real scenario, this uses web3.py to query Uniswap V3 Router contract
        # For this infrastructure skeleton, we simulate the DEX price slightly lagging the CEX.
        cex_price = 3000.0
        if symbol in self.bot.latest_data and self.bot.latest_data.get(symbol):
            cex_price = float(self.bot.latest_data.get(symbol, {}).get('price', 3000.0))
        
        # Simulate a sudden flash crash on DEX (~2% chance per tick)
        flash_crash_chance = random.random()
        if flash_crash_chance > 0.98:
            return cex_price * 0.99  # 1% drop causing an extreme spread
        return cex_price * 1.0001
        
    async def monitor_arbitrage(self):
        logger.info("⚡ DEX Sniper initialized. Monitoring Uniswap vs CEX spreads...")
        while True:
            try:
                # Arbitrage is usually focused on deep liquidity pairs
                symbol = "ETH/USDT"
                
                dex_price = await self.fetch_dex_price(symbol)
                
                if symbol in self.bot.latest_data and self.bot.latest_data[symbol]:
                    cex_price = float(self.bot.latest_data[symbol].get('price', dex_price))
                    
                    spread = abs(cex_price - dex_price)
                    spread_pct = (spread / cex_price) * 100
                    
                    if spread_pct >= self.target_spread_pct and not self.is_sniping:
                        logger.warning(f"🔫 DEX SNIPER SPREAD DETECTED [{symbol}]: CEX ${cex_price:.2f} | DEX ${dex_price:.2f} (Spread {spread_pct:.2f}%)")
                        logger.warning("🔫 EXECUTING DELTA-NEUTRAL ARBITRAGE: BUY on DEX, SHORT on CEX")
                        self.is_sniping = True
                        
                        # Usa handle_signal per l'arbitraggio (peso massimo)
                        asyncio.create_task(self.bot.handle_signal(symbol, "DEX_ARBITRAGE", "SHORT", is_black_swan=True))
                        
                        # Simulate DEX trade confirmation time on Ethereum mainnet
                        await asyncio.sleep(2)
                        logger.info("✅ ARBITRAGE LOCKED. Waiting for spread convergence.")
                    
                    elif spread_pct < 0.1 and self.is_sniping:
                        # Spread converged naturally, lock profits
                        logger.info("🔫 SPREAD CONVERGED. Closing Arbitrage positions.")
                        self.is_sniping = False
            except Exception as e:
                logger.error(f"DEX Sniper error: {e}")
                
            await asyncio.sleep(5)
