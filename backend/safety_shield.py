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
        """
        history = self.price_history_5m.get(symbol, [])
        if len(history) < 15: return None # Need at least 2.5 min

        ref_price = history[0]
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
        if initial_balance <= 0: return False
        
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

    async def get_atomic_guard_status(self, symbol: str) -> Optional[str]:
        """
        [V19.0 ATOMIC GUARD]
        Wrapper for real-time exchange position check.
        """
        pos = await self.bot.gateway.fetch_atomic_position(symbol)
        if pos:
            return pos['side'].upper() # LONG or SHORT
        return None

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

        # 🛡️ Level 0.1: Flash Detection (Pump/Dump)
        flash_event = self._check_flash_detection(symbol, current_price)
        if flash_event:
            # For now we just log it and potentially trigger a tight SL move in the future
            logger.warning(f"⚡ [FLASH] {flash_event} detected on {symbol}!")
            # If DUMP during LONG or PUMP during SHORT, trigger tight exit
            if (is_long and flash_event == "DUMP") or (not is_long and flash_event == "PUMP"):
                logger.critical(f"🆘 [FLASH EXIT] Immediate exit for {symbol} due to counter {flash_event}.")
                return True, f"FLASH_{flash_event}_EXIT"
        
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
        if tp1 > 0 and not trade_info.get('tp1_hit', False):
            if (is_long and current_price >= tp1) or (not is_long and current_price <= tp1):
                logger.info(f"🎯 [TAKE PROFIT 1] {symbol} hit TP1 level {tp1}. Profit: {pnl_pct:.2%}")
                return True, "TAKE_PROFIT_1"

        # 🎯 Level 3.1: Dynamic TP2 (Gemini-Managed)
        tp2 = float(trade_info.get('tp2', 0))
        if trade_info.get('tp1_hit', False) and tp2 > 0:
            # If we already hit TP1, we check for TP2 or AI Exit
            if (is_long and current_price >= tp2) or (not is_long and current_price <= tp2):
                 logger.info(f"🎯 [TAKE PROFIT 2] {symbol} hit FINAL level {tp2}. Profit: {pnl_pct:.2%}")
                 return True, "TAKE_PROFIT_2"
            
            # AI Check for TP2 (Periodical)
            # We use timing to avoid spamming the AI
            last_ai_check = trade_info.get('last_tp2_check', 0)
            if (time.time() - last_ai_check) > 300: # Every 5 mins
                 trade_info['last_tp2_check'] = time.time()
                 action, conf, reason = await self.bot.strategy.analyst.evaluate_active_position(
                     symbol, side, {'rsi': 0, 'macd_hist': 0}, pnl_pct*100 # Simplified indicators for now
                 )
                 if action in ["CLOSE", "PIVOT"]:
                     logger.warning(f"🧠 [AI TP2 EXIT] Gemini suggests {action} for {symbol}. Reason: {reason}")
                     return True, f"AI_TP2_{action}"

        # ⏳ Level 4: Stagnation Exit (v30.60)
        # If position is flat for too long (+/- 0.5% PnL after 4 hours)
        opened_at = float(trade_info.get('opened_at', 0))
        if opened_at > 0:
            hold_time = time.time() - opened_at
            if hold_time > 14400: # 4 Hours
                if abs(pnl_pct) < 0.005: # Less than 0.5% movement
                    logger.info(f"⏳ [STAGNATION] {symbol} flat for 4h ({pnl_pct:.2%}). Closing to free equity.")
                    return True, "STAGNATION_EXIT"

        return False, ""
