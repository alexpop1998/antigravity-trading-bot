import logging
import asyncio
import pandas as pd
from typing import List, Dict, Any

logger = logging.getLogger("AssetScanner")

class AssetScanner:
    def __init__(self, bot_instance, allowed_symbols: List[str] = None):
        self.bot = bot_instance
        self.exchange = bot_instance.gateway.exchange
        self.allowed_symbols = allowed_symbols # Mainnet Symbols only
        filters = bot_instance.config.get('market_filters', {})
        self.mandatory_symbols = filters.get('mandatory_symbols', ["BTC/USDT:USDT", "ETH/USDT:USDT"])
        self.blacklist = filters.get('blacklist', [])

    def set_allowed_symbols(self, symbols: List[str]):
        """Updates the list of confirmed real market symbols."""
        self.allowed_symbols = symbols
        logger.info(f"🛡️ [AssetScanner] Filter updated: {len(symbols)} Mainnet symbols allowed.")

    async def scan(self, active_symbols: List[str] = None, limit: int = 150) -> List[Dict[str, Any]]:
        """Alias for get_top_performing_assets (v30.0 Compatibility)."""
        return await self.get_top_performing_assets(active_symbols, limit)

    async def get_top_performing_assets(self, active_symbols: List[str] = None, limit: int = 150) -> List[Dict[str, Any]]:
        """
        Scansiona tutti i mercati Futures USDT-M e restituisce i top N per (Volume * Volatilità).
        """
        try:
            logger.info("🔍 Scanning Bitget Live Markets for top opportunities...")
            # Fetch all tickers
            tickers = await self.exchange.fetch_tickers()
            
            scored_assets = []
            
            for symbol, data in tickers.items():
                # Filter: Only USDT-M Perpetual Futures
                if not (symbol.endswith(":USDT") or ":USDT" in symbol):
                    continue
                
                if any(b in symbol for b in self.blacklist):
                    logger.debug(f"🚫 [BLACKLIST] Filtering out {symbol}")
                    continue
                
                # Extract metrics
                volume = float(data.get('quoteVolume') or 0) # 24h Volume in USDT
                change_pct = abs(float(data.get('percentage') or 0)) # 24h Absolute change
                
                # v55.9.1 [TOTAL INJECTION] Dynamic market thresholds
                strategic = self.bot.config.get('strategic_params', {})
                min_vol_usdt = strategic.get('min_liquidity_threshold', 300_000)
                min_chg_pct = strategic.get('min_volatility_threshold', 0.1)

                # Rule 1: Minimum Liquidity
                if volume < min_vol_usdt:
                    continue
                
                # Rule 2: Minimum Volatility
                if change_pct < min_chg_pct:
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
            top_scored = scored_assets[:limit]
            top_symbols = [a['symbol'] for a in top_scored]
            
            # --- STICKY SYMBOLS (V9.7) ---
            # Ensure symbols with active positions are ALWAYS in the list
            if active_symbols:
                for active in active_symbols:
                    if active not in top_symbols and active in tickers:
                        logger.info(f"📌 [STICKY] Preserving {active} (Active Position)")
                        # Insert at the beginning of the list
                        # Find full data for the active symbol
                        active_data = tickers[active]
                        top_scored.insert(0, {
                            'symbol': active,
                            'score': 999_999_999, # Max priority
                            'volume': float(active_data.get('quoteVolume', 0)),
                            'change': abs(float(active_data.get('percentage', 0)))
                        })
            
            # Ensure mandatory symbols are present but with their organic score
            for mandatory in self.mandatory_symbols:
                if mandatory not in [a['symbol'] for a in top_scored] and mandatory in tickers:
                    active_data = tickers[mandatory]
                    real_vol = float(active_data.get('quoteVolume', 0))
                    real_chg = abs(float(active_data.get('percentage', 0)))
                    top_scored.append({
                        'symbol': mandatory,
                        'score': real_vol * real_chg, # Organic priority
                        'volume': real_vol,
                        'change': real_chg
                    })
            
            # Re-sort to respect the organic momentum if mandatory symbols were appended
            top_scored.sort(key=lambda x: x['score'], reverse=True)
            
            final_selection = top_scored[:limit]
            logger.info(f"✅ Scanner identified {len(final_selection)} assets. Top 3: {[a['symbol'] for a in final_selection[:3]]}")
            return final_selection 
            
        except Exception as e:
            logger.error(f"❌ Error during market scan: {e}")
            return []
