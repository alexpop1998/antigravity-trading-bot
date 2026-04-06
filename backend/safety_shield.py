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
        self.price_history_5m: Dict[str, List[float]] = {} # 30 samples (10s each)
        self.panic_drawdown_notified = False
        self.peak_prices: Dict[str, float] = {} # Highest/Lowest reached per symbol

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

        if symbol not in self.price_history_5m:
            self.price_history_5m[symbol] = []
        self.price_history_5m[symbol].append(price)
        if len(self.price_history_5m[symbol]) > 30:
            self.price_history_5m[symbol].pop(0)

    def _check_flash_detection(self, symbol: str, current_price: float) -> Optional[str]:
        """
        [V19.0 FLASH DETECTION]
        Detects pump/dump (> 1.2% in 5m).
        v43.3 [GWEN FIX] reference: uses median of history instead of oldest sample
        """
        history = self.price_history_5m.get(symbol, [])
        if len(history) < 15: return None # Need at least 2.5 min

        ref_price = sum(history) / len(history)
        change_pct = (current_price - ref_price) / ref_price
        
        if change_pct > 0.012:
            return "PUMP"
        elif change_pct < -0.012:
            return "DUMP"
        return None

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

    async def check_panic_drawdown(self, equity: float, initial_balance: float) -> bool:
        """
        [V15.2 HOLISTIC PANIC SHIELD]
        Checks if the global drawdown exceeds the panic threshold.
        """
        if initial_balance <= 0: 
            logger.info("🛡️ [PANIC SHIELD] Waiting for initial balance sync...")
            return False
        
        drawdown = (equity - initial_balance) / initial_balance
        panic_threshold = -float(self.bot.config.get("trading_parameters", {}).get("panic_drawdown_threshold", 0.15))
        
        if drawdown <= panic_threshold:
            if not self.panic_drawdown_notified:
                logger.critical(f"🆘 [GLOBAL PANIC] Drawdown reached {drawdown:.2%}. Activation threshold: {panic_threshold:.2%}")
                self.panic_drawdown_notified = True
                return True
        else:
            self.panic_drawdown_notified = False
        return False

    async def check_daily_circuit_breaker(self) -> bool:
        """
        [v43.3 CIRCUIT BREAKER]
        Halt trading if total realized daily loss exceeds profile limit.
        """
        daily_pnl = self.bot.db.get_daily_pnl()
        loss_limit = -float(self.bot.config.get("trading_parameters", {}).get("daily_loss_limit", 15.0))
        
        if daily_pnl <= loss_limit:
            logger.critical(f"⚡ [CIRCUIT BREAKER] Daily Loss reached ${daily_pnl:.2f}. Limit: ${loss_limit:.2f}. HALTING.")
            return True
        return False

    async def get_atomic_guard_status(self, symbol: str) -> Optional[str]:
        """
        [V19.0 ATOMIC GUARD]
        Wrapper for real-time exchange position check.
        """
        pos = await self.bot.gateway.fetch_atomic_position(symbol)
        if pos:
            return pos['side'].upper() # LONG or SHORT
        return None

    async def check_position(self, symbol: str, trade: Dict[str, Any], current_price: float, current_atr: float = 0) -> (bool, str):
        """
        [v43.3 HARDENED SAFETY]
        Multi-stage safety check: Hard SL -> Trailing/Adaptive -> TP -> AI Monitor.
        """
        try:
            if not trade or not symbol: return False, ""
            
            side = trade['side']
            is_long = side == 'long'
            entry_price = trade['entry_price']
            sl = trade.get('sl', 0)
            tp1 = trade.get('tp1', 0)
            
            # v43.3 [GWEN FIX] Leverage-aware PnL
            leverage = float(trade.get('leverage', 1))
            raw_pnl = (current_price - entry_price) / entry_price if is_long else (entry_price - current_price) / entry_price
            pnl_pct = raw_pnl * leverage
            
            # --- 0. PRE-FLIGHT (History & Velocity) ---
            self._update_price_history(symbol, current_price)
            if self._check_velocity_exit(symbol, current_price, side, current_atr):
                return True, "VELOCITY_EXIT"

            flash_event = self._check_flash_detection(symbol, current_price)
            if flash_event:
                if (is_long and flash_event == "DUMP") or (not is_long and flash_event == "PUMP"):
                    logger.critical(f"🆘 [FLASH EXIT] {symbol} due to {flash_event}.")
                    return True, f"FLASH_{flash_event}_EXIT"

            # --- 1. PEAK TRACKING (For Trailing) ---
            old_peak = self.peak_prices.get(symbol, current_price)
            if is_long:
                self.peak_prices[symbol] = max(old_peak, current_price)
            else:
                self.peak_prices[symbol] = min(old_peak, current_price)

            # --- 2. HARD STOP LOSS ---
            if (is_long and current_price <= sl) or (not is_long and current_price >= sl):
                if sl > 0:
                    logger.warning(f"🚨 [HARD SL] {symbol} hit level {sl}. PnL: {pnl_pct:.2%}")
                    return True, "HARD_STOP_LOSS"

            # --- 3. TRAILING & ADAPTIVE SL ---
            trailing_config = self.bot.config.get("trailing_params", {})
            activation_pnl = float(trailing_config.get("activation_pnl_pct", 1.5)) / 100.0
            
            # 3.1 Algorithmic Trailing
            if pnl_pct >= activation_pnl and current_atr > 0:
                atr_mult = float(trailing_config.get("base_atr_multiplier", 3.0))
                if trade.get('sl_tightness') == 'TIGHTEN':
                    atr_mult *= float(trailing_config.get("tighten_ratio", 0.7))
                
                dist = current_atr * atr_mult
                new_tsl = self.peak_prices[symbol] - dist if is_long else self.peak_prices[symbol] + dist
                
                if (is_long and new_tsl > sl) or (not is_long and new_tsl < sl):
                    sl = new_tsl
                    trade['sl'] = sl
                    logger.debug(f"📈 [TRAILING] {symbol} moved SL to {sl:.4f}")

            # 3.2 Adaptive Tightening (v43.3 [GWEN FIX] - Never widen)
            if pnl_pct < 0 and trade.get('sl_tightness') == 'TIGHTEN':
                original_sl = float(trade.get('original_sl', sl))
                dist_to_entry = abs(entry_price - original_sl)
                new_asl = entry_price - (dist_to_entry * 0.6) if is_long else entry_price + (dist_to_entry * 0.6)
                
                if is_long:
                    if new_asl > sl:
                        sl = new_asl
                        trade['sl'] = sl
                        logger.warning(f"🛡️ [ADAPTIVE SL] {symbol} tightened to {sl:.4f}")
                else:
                    if new_asl < sl:
                        sl = new_asl
                        trade['sl'] = sl
                        logger.warning(f"🛡️ [ADAPTIVE SL] {symbol} tightened to {sl:.4f}")

            # --- 4. BREAK-EVEN GUARD ---
            if trade.get('tp1_hit', False):
                be_price = entry_price * (1.003 if is_long else 0.997)
                if (is_long and be_price > sl) or (not is_long and be_price < sl):
                    sl = be_price
                    trade['sl'] = sl
                    logger.info(f"🛡️ [BREAK-EVEN] TP1 Hit! Locked {symbol} at {sl:.4f}")

            # --- 5. TAKE PROFIT & STAGNATION ---
            if tp1 > 0 and not trade.get('tp1_hit', False):
                if (is_long and current_price >= tp1) or (not is_long and current_price <= tp1):
                    return True, "TAKE_PROFIT_1"

            opened_at = float(trade.get('opened_at', 0))
            if opened_at > 0:
                if (time.time() - opened_at) > 14400 and abs(pnl_pct) < 0.005: 
                    return True, "STAGNATION_EXIT"

            return False, ""
        except Exception as e:
            logger.error(f"Error in safety check for {symbol}: {e}")
            return False, ""
