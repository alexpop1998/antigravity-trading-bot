import logging

logger = logging.getLogger("SectorManager")

class SectorManager:
    def __init__(self):
        # Initial mapping of major assets and sectors
        self.sector_map = {
            # --- AI / DePIN ---
            "FET/USDT:USDT": "AI", "TAO/USDT:USDT": "AI", "RENDER/USDT:USDT": "AI", "NEAR/USDT:USDT": "AI",
            "AR/USDT:USDT": "DEPIN", "HNT/USDT:USDT": "DEPIN", "FIL/USDT:USDT": "DEPIN",
            
            # --- L1 / L2 ---
            "BTC/USDT:USDT": "MAJOR", "ETH/USDT:USDT": "MAJOR", "SOL/USDT:USDT": "L1", 
            "AVAX/USDT:USDT": "L1", "DOT/USDT:USDT": "L1", "ADA/USDT:USDT": "L1",
            "ARB/USDT:USDT": "L2", "OP/USDT:USDT": "L2", "MATIC/USDT:USDT": "L2",
            
            # --- Meme ---
            "DOGE/USDT:USDT": "MEME", "SHIB/USDT:USDT": "MEME", "PEPE/USDT:USDT": "MEME", 
            "WIF/USDT:USDT": "MEME", "BONK/USDT:USDT": "MEME", "FLOKI/USDT:USDT": "MEME",
            
            # --- DeFi ---
            "UNI/USDT:USDT": "DEFI", "AAVE/USDT:USDT": "DEFI", "LINK/USDT:USDT": "DEFI",
            "RUNE/USDT:USDT": "DEFI", "JUP/USDT:USDT": "DEFI", "LDO/USDT:USDT": "DEFI"
        }
        self.default_sector = "ALTCOIN"
        self.max_sector_exposure = 0.20 # 20% Max per sector

    def get_sector(self, symbol):
        return self.sector_map.get(symbol, self.default_sector)

    def is_exposure_safe(self, symbol, active_trades, total_balance):
        """
        Checks if opening/adding to a trade on 'symbol' would exceed the sector limit.
        """
        target_sector = self.get_sector(symbol)
        
        # Calculate current exposure for the target sector
        sector_value = 0.0
        for s, trade in active_trades.items():
            if trade and self.get_sector(s) == target_sector:
                # We use nominal value (entry_price * amount)
                sector_value += float(trade.get('entry_price', 0)) * float(trade.get('amount', 0))
        
        current_exposure_pct = sector_value / total_balance if total_balance > 0 else 0
        
        if current_exposure_pct >= self.max_sector_exposure:
            logger.warning(f"🚫 [SECTOR LIMIT] {target_sector} exposure ({current_exposure_pct:.1%}) exceeds limit ({self.max_sector_exposure:.1%}). Blocking {symbol}")
            return False
            
        return True
