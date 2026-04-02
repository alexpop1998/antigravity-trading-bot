import time
import logging
from typing import Dict, Any, List, Optional

logger = logging.getLogger("SafetyShield")

class SafetyShield:
    """
    High-frequency monitor for Stop Loss and Take Profit enforcement.
    Operates independently of analysis loops to ensure reflexes are fast and robust.
    """
    
    def __init__(self, bot_instance):
        self.bot = bot_instance
        self.session_start_time = time.time()
        self.startup_shield_seconds = 300
        self.trade_protection_seconds = 60
        self.price_history_2m: Dict[str, List[float]] = {} # 12 samples (10s each)

    def is_startup_shield_active(self) -> bool:
        return (time.time() - self.session_start_time) < self.startup_shield_seconds

    def is_trade_shield_active(self, opened_at: float) -> bool:
        return (time.time() - opened_at) < self.trade_protection_seconds

    def _update_price_history(self, symbol: str, price: float):
        if symbol not in self.price_history_2m:
            self.price_history_2m[symbol] = []
        self.price_history_2m[symbol].append(price)
        if len(self.price_history_2m[symbol]) > 12:
            self.price_history_2m[symbol].pop(0)

    def _check_velocity_exit(self, symbol: str, current_price: float, side: str, atr: float) -> bool:
        history = self.price_history_2m.get(symbol, [])
        if len(history) < 6: return False # Need at least 1 min of data
        
        ref_price = history[0]
        change_pct = (current_price - ref_price) / ref_price
        is_long = side == 'long'
        
        # Velocity Threshold: 1.5x ATR expressed as % (or 1.2% hard floor)
        atr_pct = (atr / current_price) if current_price > 0 else 0.012
        sos_threshold = max(0.012, atr_pct * 1.5)
        
        if (is_long and change_pct < -sos_threshold) or (not is_long and change_pct > sos_threshold):
            logger.critical(f"🆘 [VELOCITY EXIT] {symbol} Panic Spike ({change_pct:.2%}) against {side.upper()}.")
            return True
        return False

    async def check_position(self, symbol: str, trade_info: Dict[str, Any], current_price: float, current_atr: float = 0):
        """
        Main logic for SL/TP check on a single position.
        """
        if not trade_info or not symbol:
            return False, ""

        side = str(trade_info.get('side', 'long')).lower()
        is_long = side == 'long'
        entry_price = float(trade_info.get('entry_price', current_price))
        sl = float(trade_info.get('sl', 0))
        tp1 = float(trade_info.get('tp1', 0))
        
        # 🛡️ Update Price Velocity History
        self._update_price_history(symbol, current_price)
        
        # 🛡️ Level 0: Velocity Emergency Exit (Panic Protection)
        if self._check_velocity_exit(symbol, current_price, side, current_atr):
            return True, "VELOCITY_EXIT"
        
        # Calculate PnL for logging
        pnl_pct = (current_price - entry_price) / entry_price if is_long else (entry_price - current_price) / entry_price
        
        # 🛡️ Level 1: Hard Stop Loss (The Floor)
        # Always trigger if current_price hits the hard SL in trade_info
        if (is_long and current_price <= sl) or (not is_long and current_price >= sl):
            if sl > 0:
                logger.warning(f"🚨 [HARD SL] {symbol} hit stop level {sl}. PnL: {pnl_pct:.2%}")
                return True, "HARD_STOP_LOSS"

        # 🛡️ Level 2: Technical Trailing Stop (The Reflex)
        # Requires ATR and passed Shield periods
        startup_shield = self.is_startup_shield_active()
        trade_shield = self.is_trade_shield_active(float(trade_info.get('opened_at', 0)))
        
        # 🔄 [BREAK-EVEN LOGIC] - If TP1 was hit, we override old SL to Entry + 0.3%
        if trade_info.get('tp1_hit', False):
            be_price = entry_price * (1.003 if is_long else 0.997)
            # Only update if it's "safer" than current SL
            if (is_long and be_price > sl) or (not is_long and be_price < sl):
                sl = be_price
                logger.info(f"🛡️ [BREAK-EVEN] TP1 Hit! Locked in {symbol} at {sl:.4f}")

        # We only use technical (volatility-based) stops if we have data AND aren't shielded
        if not startup_shield and not trade_shield and current_atr > 0:
            tech_multiplier = 5.0 if trade_info.get('tp1_hit', False) else 3.5
            sl_distance = current_atr * tech_multiplier
            
            # Dynamic Stop Calculation
            dynamic_sl = entry_price - sl_distance if is_long else entry_price + sl_distance
            
            if (is_long and current_price <= dynamic_sl) or (not is_long and current_price >= dynamic_sl):
                logger.warning(f"🔔 [TECHNICAL STOP] {symbol} hit trailing ATR level {dynamic_sl:.4f}. PnL: {pnl_pct:.2%}")
                return True, "TECHNICAL_STOP_LOSS"
        
        # 🎯 Level 3: Take Profit Enforcement
        if tp1 > 0:
            if (is_long and current_price >= tp1) or (not is_long and current_price <= tp1):
                logger.info(f"🎯 [TAKE PROFIT 1] {symbol} hit TP1 level {tp1}. Profit: {pnl_pct:.2%}")
                return True, "TAKE_PROFIT_1"

        return False, ""
