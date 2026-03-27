import logging

logger = logging.getLogger("SectorManager")

class SectorManager:
    def __init__(self):
        # Initial mapping of major assets and sectors
        self.sector_map = {
            # --- AI / DePIN ---
            "FET/USDT:USDT": "AI", "TAO/USDT:USDT": "AI", "RENDER/USDT:USDT": "AI", "NEAR/USDT:USDT": "AI",
            "AR/USDT:USDT": "DEPIN", "HNT/USDT:USDT": "DEPIN", "FIL/USDT:USDT": "DEPIN",
            "AGT/USDT:USDT": "AI", # Antigravity (Likely AI)
            
            # --- L1 / L2 ---
            "BTC/USDT:USDT": "MAJOR", "ETH/USDT:USDT": "MAJOR", "SOL/USDT:USDT": "L1", 
            "AVAX/USDT:USDT": "L1", "DOT/USDT:USDT": "L1", "ADA/USDT:USDT": "L1",
            "ARB/USDT:USDT": "L2", "OP/USDT:USDT": "L2", "MATIC/USDT:USDT": "L2", "SUI/USDT:USDT": "L1", "APT/USDT:USDT": "L1",
            "THE/USDT:USDT": "L1", # THE Protocol
            
            # --- Meme ---
            "DOGE/USDT:USDT": "MEME", "SHIB/USDT:USDT": "MEME", "PEPE/USDT:USDT": "MEME", 
            "WIF/USDT:USDT": "MEME", "BONK/USDT:USDT": "MEME", "FLOKI/USDT:USDT": "MEME",
            "PIPPIN/USDT:USDT": "MEME", "FARTCOIN/USDT:USDT": "MEME", "BAN/USDT:USDT": "MEME",
            "BEAT/USDT:USDT": "MEME", # Just in case it's a meme
            
            # --- DeFi ---
            "UNI/USDT:USDT": "DEFI", "AAVE/USDT:USDT": "DEFI", "LINK/USDT:USDT": "DEFI",
            "RUNE/USDT:USDT": "DEFI", "JUP/USDT:USDT": "DEFI", "LDO/USDT:USDT": "DEFI",
            "PLAY/USDT:USDT": "DEFI"
        }
        self.default_sector = "ALTCOIN"
        self.max_sector_exposure = 10.0 # Relaxed for Testnet/Data Collection (1000%)

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
