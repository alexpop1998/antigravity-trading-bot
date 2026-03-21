import logging
import asyncio
import time

logger = logging.getLogger("SignalManager")

class SignalManager:
    def __init__(self, bot_instance):
        self.bot = bot_instance
        self.signals = {} # symbol -> { 'side': 'buy'/'sell', 'score': 0.0, 'last_update': timestamp }
        self.min_conviction = 5.0 # Even higher threshold for consensus (Overhaul requirement)
        self.window_seconds = 300 # 5 minute window for signal aggregation
        
        self.weights = {
            "TECH": 1.0,     # Basic Technicals
            "AI": 1.2,       # ML Prophet
            "NEWS": 1.5,     # Sentiment (Social Scraper)
            "GATEKEEPER": 2.0, # High impact news (AI Filtered)
            "WHALE": 2.5,    # Massive on-chain moves
            "LIQUIDATION": 1.5 # Exchange cascades
        }

    async def add_signal(self, symbol, type, side, weight_modifier=1.0):
        now = time.time()
        weight = self.weights.get(type, 0.5) * weight_modifier
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
        logger.info(f"🚦 [Consensus] {symbol} Total Score: {current_total_score:.2f} (Added {type}: {side})")
        
        if abs(current_total_score) >= self.min_conviction:
            logger.warning(f"🔥 CONSENSUS REACHED for {symbol} (Score: {current_total_score:.2f}). Approving execution.")
            final_score = abs(current_total_score)
            # Reset score after approval to prevent double-firing in the same window
            self.signals[symbol]['score'] = 0 
            return True, final_score
            
        return False, 0
