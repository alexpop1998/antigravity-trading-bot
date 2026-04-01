import logging
import asyncio
import pandas as pd
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
            
            # 2. ML Prediction — needs a DataFrame with OHLCV columns; skip gracefully if empty
            prediction = None
            if data and isinstance(data, dict) and 'close' in data:
                df = pd.DataFrame([data])
                prediction = await asyncio.to_thread(self.predictor.train_and_predict, symbol, df)
            
            # 3. AI Decision via LLMAnalyst.decide_strategy (correct method name)
            indicators = data if data else {}
            approved, confidence, leverage, tp_mult, sl_mult, reason = await self.analyst.decide_strategy(
                symbol=symbol,
                side="buy",
                signal_type="DELIBERATIVE_SCAN",
                indicators=indicators
            )
            
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

    def calculate_consensus_score(self, regime, prediction, ai_approved: bool, ai_confidence: float) -> float:
        """
        Weighted scoring:
        - Technical Regime: 30%
        - ML Prediction:    30%
        - AI Approval:      40%
        """
        score = 0.0
        
        # Technical Score (30%)
        if regime in ("TRENDING_UP", "BREAKOUT"):
            score += 0.3
        elif regime == "NEUTRAL":
            score += 0.1
        
        # ML Score (30%) — MLPredictor returns {'direction': 1, 'confidence': 0.x}
        if prediction and isinstance(prediction, dict):
            if prediction.get('direction') == 1:
                score += 0.3 * prediction.get('confidence', 0.5)
        
        # AI Score (40%)
        if ai_approved:
            score += 0.4 * min(ai_confidence, 1.0)
        
        return round(score, 4)
