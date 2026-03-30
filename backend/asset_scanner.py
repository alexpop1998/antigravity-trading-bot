import logging
import asyncio
import pandas as pd
from typing import List, Dict, Any

logger = logging.getLogger("AssetScanner")

class AssetScanner:
    def __init__(self, exchange, allowed_symbols: List[str] = None):
        self.exchange = exchange
        self.allowed_symbols = allowed_symbols # Mainnet Symbols only
        # Mandatory symbols to always keep in rotation
        self.mandatory_symbols = ["BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT"]
        # Symbols to ignore (stables, delisted, etc.)
        self.blacklist = ["USDC/USDT:USDT", "BUSD/USDT:USDT", "FDUSD/USDT:USDT", "TUSD/USDT:USDT"]

    def set_allowed_symbols(self, symbols: List[str]):
        """Updates the list of confirmed real market symbols."""
        self.allowed_symbols = symbols
        logger.info(f"🛡️ [AssetScanner] Filter updated: {len(symbols)} Mainnet symbols allowed.")

    async def get_top_performing_assets(self, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Scansiona tutti i mercati Futures USDT-M e restituisce i top N per (Volume * Volatilità).
        """
        try:
            logger.info("🔍 Scanning Binance Markets for top opportunities...")
            # Fetch all tickers
            tickers = await self.exchange.fetch_tickers()
            
            scored_assets = []
            
            for symbol, data in tickers.items():
                # Filter: Only USDT-M Perpetual Futures
                if not (symbol.endswith(":USDT") or ":USDT" in symbol):
                    continue
                
                if symbol in self.blacklist:
                    continue
                
                # Extract metrics
                volume = float(data.get('quoteVolume') or 0) # 24h Volume in USDT
                change_pct = abs(float(data.get('percentage') or 0)) # 24h Absolute change
                
                # Rule: Minimum Liquidity (20M USDT) to avoid pump & dumps without exit liquidity
                if volume < 20_000_000:
                    continue
                
                # Momentum Score: A mix of high volume and high volatility
                score = volume * change_pct
                
                scored_assets.append({
                    'symbol': symbol,
                    'score': score,
                    'volume': volume,
                    'change': change_pct
                })
            
            # Sort by score descending
            scored_assets.sort(key=lambda x: x['score'], reverse=True)
            
            # Take top N
            top_symbols = [a['symbol'] for a in scored_assets[:limit]]
            
            # Ensure mandatory symbols are present
            for mandatory in self.mandatory_symbols:
                if mandatory not in top_symbols and mandatory in tickers:
                    # Replace the last one with mandatory
                    top_symbols[-1] = mandatory
            
            logger.info(f"✅ Scanner identified {len(top_symbols)} high-opportunity assets. Top 3: {top_symbols[:3]}")
            return scored_assets # Return full dict list for AI to refine
            
        except Exception as e:
            logger.error(f"❌ Error during market scan: {e}")
            return []
