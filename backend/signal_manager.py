import logging
import time

logger = logging.getLogger("SignalManager")

class SignalManager:
    def __init__(self, bot_instance):
        self.bot = bot_instance
        self.signals = {} # symbol -> { type: { 'side': 'buy'/'sell', 'weight': 0.0, 'timestamp': ts } }
        self.min_conviction = getattr(bot_instance, 'consensus_threshold', 2.5)
        
        # Signal TTL (Time-To-Live in seconds)
        self.ttls = {
            "LIQUIDATION": 10,
            "DEX_ARBITRAGE": 10,
            "WHALE": 120,
            "GATEKEEPER": 900,  # 15m
            "AI": 60,          # ML Prophet (1m)
            "TECH": 300,        # Technicals (5m)
            "NEWS": 600,        # Social/Panic (10m)
            "VELOCITY_MOMENTUM": 30,
            "RECOVERY": 120,
            "REJECTION": 120,
            "SENTIMENT": 600,      # 10m
            "NEW_LISTING": 60    # 1m (High speed)
        }

        self.weights = {
            "TECH": 1.0, "AI": 1.2, "NEWS": 1.5, "GATEKEEPER": 3.0, "WHALE": 2.5,
            "LIQUIDATION": 2.5, "DEX_ARBITRAGE": 3.0, "EVENT_DUMP": 2.5, "EVENT_PUMP": 2.5,
            "RECOVERY": 3.0, "REJECTION": 3.0, "VELOCITY_MOMENTUM": 2.5,
            "SENTIMENT": 1.5, "NEW_LISTING": 5.0
        }

    async def add_signal(self, symbol, type, side, weight_modifier=1.0, current_price=None, ema200=None, ai_confidence=0.0):
        now = time.time()
        
        if type in ["LIQUIDATION", "DEX_ARBITRAGE", "NEW_LISTING"]:
            logger.warning(f"⚡ [HFT PATH] Immediate execution triggered for {symbol} via {type} ({side})")
            return True, 5.0 # HFT signals bypass consensus map

        # 2. Strategic Consensus Path (Path B)
        weight = self.weights.get(type, 0.5) * weight_modifier
        
        # Trend Bonus logic remains...
        if current_price and ema200:
            is_long = side.lower() in ['buy', 'long']
            is_uptrend = current_price > ema200
            weight += 1.5 if (is_long and is_uptrend) or (not is_long and not is_uptrend) else -1.0

        if symbol not in self.signals:
            self.signals[symbol] = {}
        
        # Store signal with timestamp
        self.signals[symbol][type] = {
            'side': side.lower(),
            'weight': weight,
            'timestamp': now
        }

        # Calculate Consensus with TTL Decay
        total_score = 0.0
        active_side = None
        
        # Clean expired signals and calculate sum
        expired_types = []
        for s_type, s_data in self.signals[symbol].items():
            ttl = self.ttls.get(s_type, 300)
            if (now - s_data['timestamp']) > ttl:
                expired_types.append(s_type)
                continue
            
            # Use polarity for scoring
            score_diff = s_data['weight'] if s_data['side'] in ['buy', 'long'] else -s_data['weight']
            total_score += score_diff
        
        # Remove expired
        for t in expired_types:
            del self.signals[symbol][t]

        # --- NEW: AI SPECULATIVE THRESHOLD (v3.5) ---
        # Lower the threshold to 1.5 if an AI signal has extreme confidence (>90%)
        effective_threshold = self.min_conviction
        if type == "AI" and ai_confidence > 0.90:
            effective_threshold = 1.5
            logger.warning(f"🚀 [AI SPECULATION] High Confidence AI Signal ({ai_confidence:.2f}) - Lowering threshold to {effective_threshold}")

        if abs(total_score) >= effective_threshold:
            logger.warning(f"🔥 STRATEGIC CONSENSUS REACHED for {symbol} (Score: {total_score:.2f} >= {effective_threshold})")
            final_score = abs(total_score)
            # Reset signals for this symbol after execution to avoid double-fire
            self.signals[symbol] = {}
            return True, final_score
            
        return False, 0
