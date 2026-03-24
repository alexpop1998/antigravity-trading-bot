import logging
import time

logger = logging.getLogger("SignalManager")

class SignalManager:
    def __init__(self, bot_instance):
        self.bot = bot_instance
        self.signals = {} # symbol -> { 'side': 'buy'/'sell', 'score': 0.0, 'last_update': timestamp }
        self.min_conviction = 3.5 # Increased from 2.5 to filter noise after high drawdown period
        self.window_seconds = 300 # 5 minute window for signal aggregation
        
        self.weights = {
            "TECH": 1.0,     # Basic Technicals
            "AI": 1.2,       # ML Prophet
            "NEWS": 1.5,     # Sentiment (Social Scraper)
            "GATEKEEPER": 2.0, # High impact news (AI Filtered)
            "WHALE": 2.5,    # Massive on-chain moves
            "LIQUIDATION": 1.5, # Exchange cascades
            "EVENT_DUMP": 2.5, # Major 15m drop
            "EVENT_PUMP": 2.5, # Major 15m pump
            "RECOVERY": 3.0,   # V-Shape recovery start
            "REJECTION": 3.0,  # Rejection after pump start
            "VELOCITY_MOMENTUM": 2.5, # Rapid 2m breakout/breakdown
            "VELOCITY_REVERSAL": 3.0  # Overextension + exhaustion signal
        }

    async def add_signal(self, symbol, type, side, weight_modifier=1.0, current_price=None, ema200=None):
        now = time.time()
        weight = self.weights.get(type, 0.5) * weight_modifier
        
        # --- TREND BONUS: +0.5 if aligning with 200 EMA ---
        if current_price and ema200:
            is_long = side.lower() in ['buy', 'long']
            is_uptrend = current_price > ema200
            
            if (is_long and is_uptrend) or (not is_long and not is_uptrend):
                weight += 1.5
                logger.info(f"🛡️ [Trend Bonus] +1.5 added to {type} for {symbol} (Trend Aligned)")
            else:
                weight -= 1.0 # Significant penalty for counter-trend
                logger.info(f"⚠️ [Counter Trend] -1.0 penalty to {type} for {symbol}")

        score_diff = weight if side.lower() in ['buy', 'long'] else -weight
        
        if symbol not in self.signals or (now - self.signals[symbol]['last_update']) > self.window_seconds:
            # New signal window
            self.signals[symbol] = {'side': side, 'score': score_diff, 'last_update': now}
        else:
            # Aggregate within window
            self.signals[symbol]['score'] += score_diff
            self.signals[symbol]['last_update'] = now
            # Update side based on total score polarity
            self.signals[symbol]['side'] = 'buy' if self.signals[symbol]['score'] > 0 else 'sell'

        current_total_score = self.signals[symbol]['score']
        
        # --- DYNAMIC THRESHOLD: Lower for High Impact signals ---
        dynamic_threshold = self.min_conviction
        if type in ["WHALE", "GATEKEEPER"]:
            dynamic_threshold = 2.0
            
        logger.info(f"🚦 [Consensus] {symbol} Total Score: {current_total_score:.2f} / {dynamic_threshold} (Added {type}: {side})")
        
        if abs(current_total_score) >= dynamic_threshold:
            logger.warning(f"🔥 CONSENSUS REACHED for {symbol} (Score: {current_total_score:.2f}). Approving execution.")
            final_score = abs(current_total_score)
            # Reset score after approval to prevent double-firing in the same window
            self.signals[symbol]['score'] = 0 
            return True, final_score
            
        return False, 0
