import logging
import asyncio
from typing import Dict, Any, List, Optional
from ml_predictor import MLPredictor
from llm_analyst import LLMAnalyst
from regime_detector import RegimeDetector
from signal_manager import SignalManager

logger = logging.getLogger("StrategyEngine")

class StrategyEngine:
    """
    The "Brain" of the bot. 
    Aggregates Technical, ML, and AI signals into a unified consensus.
    """
    
    def __init__(self, bot_instance):
        self.bot = bot_instance
        self.predictor = MLPredictor()
        self.analyst = LLMAnalyst(bot_instance)
        self.regime_detector = RegimeDetector()
        self.signal_manager = SignalManager(bot_instance)
        self.last_consensus: Dict[str, float] = {}

    async def analyze_opportunity(self, symbol: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Deep analysis of a single symbol.
        Returns a consensus score and recommended action.
        """
        try:
            # 1. Technical Regime Detection
            regime = self.regime_detector.detect_regime(data)
            
            # 2. ML Prediction — use full DataFrame if available (100 candles)
            prediction = None
            df = data.get('df') if data else None
            if df is not None and hasattr(df, '__len__') and len(df) >= 50:
                prediction = await asyncio.to_thread(self.predictor.train_and_predict, symbol, df)
            
            # 3. AI Decision via LLMAnalyst.decide_strategy (correct method name)
            indicators = data if data else {}
            result = await self.analyst.decide_strategy(
                symbol=symbol,
                side="buy",
                signal_type="DELIBERATIVE_SCAN",
                indicators=indicators
            )
            # Safe unpack: decide_strategy returns (approved, confidence, leverage, tp_mult, sl_mult, tp_price, reason)
            approved = result[0] if result else False
            confidence = result[1] if result and len(result) > 1 else 0.0
            leverage = result[2] if result and len(result) > 2 else self.bot.leverage
            reason = result[6] if result and len(result) > 6 else (result[5] if result and len(result) > 5 else "N/A")
            
            # 4. Consensus Score
            score = self.calculate_consensus_score(regime, prediction, approved, confidence)
            
            direction = "LONG 🟢" if score >= 0.8 else "SKIP ⏭️"
            logger.info(
                f"🧠 [STRATEGY] {symbol} → Regime: {regime} | "
                f"ML: {'UP' if prediction and prediction.get('direction') == 1 else 'N/A'} | "
                f"AI: {reason} | Score: {score:.2f} → {direction}"
            )
            
            return {
                'symbol': symbol,
                'score': score,
                'regime': regime,
                'ai_approved': approved,
                'ai_reason': reason,
                'confidence': confidence,
                'leverage': leverage,
            }
            
        except Exception as e:
            logger.error(f"❌ Error during strategy evaluation for {symbol}: {e}")
            return {'symbol': symbol, 'score': 0.0, 'error': str(e)}

    def calculate_consensus_score(self, regime, prediction, ai_approved, ai_confidence):
        """
        Calculates a final decision score (0.0 to 1.0) based on modular consensus.
        Threshold for execution is typically 0.80.
        """
        score = 0.0
        
        # 1. AI Weight (40%)
        if ai_approved:
            score += (0.4 * ai_confidence)
            
        # 2. ML Weight (30%)
        if prediction and isinstance(prediction, dict) and prediction.get('confidence'):
            score += (0.3 * prediction['confidence'])
            
        # 3. Technical Regime Weight (30%)
        if regime and regime[0] != 'UNKNOWN':
            # Simple mapping: 0.3 for positive regimes
            if regime[0] in ["TRENDING_UP", "BREAKOUT"]:
                score += 0.3
            elif regime[0] == "NEUTRAL":
                score += 0.1
            
        # --- BLITZ PROFILE OVERRIDE (v30.28) ---
        # In Blitz mode, we prioritize speed. If AI is extremely confident, 
        # we allow the trade even if ML/Regime are still calibrating.
        profile = getattr(self.bot, 'profile_type', 'aggressive').lower()
        if profile == 'blitz' and ai_approved and ai_confidence > 0.85:
            if score < 0.81:
                logger.info(f"⚡ [BLITZ BOOST] High AI confidence ({ai_confidence:.2f}) boosting score {score:.2f} -> 0.81")
                score = 0.81

        return round(score, 4)
