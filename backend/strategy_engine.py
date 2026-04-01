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
        self.bot = bot_instance  # Temporarily maintain reference to config
        self.predictor = MLPredictor()
        self.analyst = LLMAnalyst(bot_instance)
        self.regime_detector = RegimeDetector()
        self.signal_manager = SignalManager(bot_instance)
        
        # Performance cache to avoid redundant AI calls
        self.last_consensus: Dict[str, float] = {}

    async def analyze_opportunity(self, symbol: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Deep analysis of a single symbol.
        Returns a consensus score and recommended action.
        """
        try:
            # 1. Technical Regime Detection
            regime = self.regime_detector.detect_regime(data)
            
            # 2. ML Prediction (Dynamic Random Forest)
            prediction = await asyncio.to_thread(self.predictor.train_and_predict, symbol, data)
            
            # 3. AI Sentiment (Gemini - if cooldown permits)
            ai_evaluation = await self.analyst.evaluate_symbol(symbol, data)
            
            # 4. Consensus Logic (The Synthesis)
            score = self.calculate_consensus_score(regime, prediction, ai_evaluation)
            
            logger.info(f"🧠 [STRATEGY] {symbol} Analysis: Consensus {score:.2f} (Regime: {regime}, AI: {ai_evaluation.get('sentiment')})")
            
            return {
                'symbol': symbol,
                'score': score,
                'regime': regime,
                'ai_sentiment': ai_evaluation.get('sentiment'),
                'is_black_swan': ai_evaluation.get('is_black_swan', False),
                'confidence': ai_evaluation.get('confidence', 0.0)
            }
            
        except Exception as e:
            logger.error(f"❌ Error during strategy evaluation for {symbol}: {e}")
            return {'symbol': symbol, 'score': 0.0, 'error': str(e)}

    def calculate_consensus_score(self, regime, prediction, ai_evaluation) -> float:
        """
        Weighted scoring system.
        """
        # (Example logic - will be refined based on profile_type)
        score = 0.0
        
        # Technical Score (30%)
        if regime == "TRENDING_UP": score += 0.3
        
        # ML Score (30%)
        if prediction == "BUY": score += 0.3
        
        # AI Score (40%)
        ai_sentiment = ai_evaluation.get('sentiment')
        if ai_sentiment == "STRONG_BUY": score += 0.4
        elif ai_sentiment == "BUY": score += 0.2
        
        return score
