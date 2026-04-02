import logging
import asyncio
from typing import Dict, Any, List, Optional
from ml_predictor import MLPredictor
from llm_analyst import LLMAnalyst
from regime_detector import RegimeDetector
from signal_manager import SignalManager
from sector_manager import SectorManager
import pandas as pd

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
        self.sector_manager = SectorManager()
        self.last_consensus: Dict[str, float] = {}

    async def analyze_opportunity(self, symbol: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Deep analysis of a single symbol.
        Returns a consensus score and recommended action.
        """
        try:
            # 1. Technical Regime Detection
            regime = self.regime_detector.detect_regime(data)
            
            # 2. ML Prediction - Enriched with Technical Indicators
            prediction = None
            df = data.get('df') if data else None
            if df is not None and len(df) >= 50:
                # Calculate required indicators for ML
                df['rsi'] = self._calculate_rsi(df['close'])
                df['atr'] = self._calculate_atr(df)
                df['macd'], _, df['macd_hist'] = self._calculate_macd(df['close'])
                df['bb_upper'], _, df['bb_lower'] = self._calculate_bollinger(df['close'])
                df.dropna(inplace=True)
                
                if len(df) >= 20:
                    prediction = await asyncio.to_thread(self.predictor.train_and_predict, symbol, df)
            
            # 2.5 Multi-Timeframe (MTF) Confirmation
            profile = getattr(self.bot, 'profile_type', 'aggressive').lower()
            mtf_approved = await self._check_mtf_trend(symbol)
            mtf_bonus = 0.1 if mtf_approved else -0.1
            
            if not mtf_approved and profile != 'blitz':
                logger.info(f"⏭️ [MTF GUARD] {symbol} rejected: Macro trend mismatch.")
                return {'symbol': symbol, 'score': 0.0, 'reason': 'MTF_MISMATCH'}
            elif not mtf_approved and profile == 'blitz':
                 logger.info(f"⚡ [BLITZ BYPASS] {symbol} macro mismatch, but continuing (Blitz mode).")

            # 2.6 Sector Guard (v29.x Restore)
            sector = self.sector_manager.get_sector(symbol)
            current_positions = getattr(self.bot, 'active_positions', {})
            sector_count = sum(1 for s, side in current_positions.items() 
                             if side is not None and self.sector_manager.get_sector(s) == sector)
            if sector_count >= 5:
                logger.warning(f"🛡️ [SECTOR GUARD] {symbol} rejected: Max exposure for {sector} reached ({sector_count}).")
                return {'symbol': symbol, 'score': 0.0, 'reason': 'SECTOR_EXPOSURE'}

            # 2.7 Funding Rate Penalty (v29.x Restore)
            funding_multiplier = 1.0
            try:
                ticker = await self.bot.gateway.exchange.fetch_ticker(symbol)
                funding = await self.bot.gateway.exchange.fetch_funding_rate(symbol)
                funding_rate = float(funding.get('fundingRate', 0))
                if funding_rate > 0.0005: # High funding for longs
                    funding_multiplier = 0.5
                    logger.warning(f"⚠️ [FUNDING PENALTY] {symbol} high funding: {funding_rate:.4%}")
            except: pass
            
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
            
            # 4. Consensus Score (MTF Bonus included)
            score = self.calculate_consensus_score(regime, prediction, approved, confidence, mtf_bonus)
            
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

    def calculate_consensus_score(self, regime, prediction, ai_approved, ai_confidence, mtf_bonus=0.0):
        """
        Calculates a final decision score (0.0 to 1.0) based on modular consensus.
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
            if regime[0] in ["TRENDING_UP", "BREAKOUT"]:
                score += 0.3
            elif regime[0] == "NEUTRAL":
                score += 0.1
        
        # 4. MTF Bonus/Penalty (for Blitz)
        score += mtf_bonus
        
        # --- BLITZ PROFILE OVERRIDE ---
        profile = getattr(self.bot, 'profile_type', 'aggressive').lower()
        if profile == 'blitz' and ai_approved and ai_confidence > 0.85:
            if score < 0.81:
                score = 0.81

        return max(0.0, min(1.0, round(score, 4)))

    async def _check_mtf_trend(self, symbol: str) -> bool:
        """Fetches 4h/1d candles to confirm macro direction."""
        try:
            ohlcv_4h = await self.bot.gateway.exchange.fetch_ohlcv(symbol, '4h', limit=50)
            df_4h = pd.DataFrame(ohlcv_4h, columns=['t','o','h','l','c','v'])
            ema200 = df_4h['c'].ewm(span=200, adjust=False).mean().iloc[-1]
            return df_4h['c'].iloc[-1] > ema200 # Standard Trend-Following
        except Exception as e:
            logger.error(f"MTF Error for {symbol}: {e}")
            return True # Neutral on error

    def _calculate_rsi(self, series, period=14):
        delta = series.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))

    def _calculate_atr(self, df, period=14):
        high_low = df['high'] - df['low']
        high_close = (df['high'] - df['close'].shift()).abs()
        low_close = (df['low'] - df['close'].shift()).abs()
        ranges = pd.concat([high_low, high_close, low_close], axis=1)
        true_range = ranges.max(axis=1)
        return true_range.rolling(window=period).mean()

    def _calculate_macd(self, series, fast=12, slow=26, signal=9):
        exp1 = series.ewm(span=fast, adjust=False).mean()
        exp2 = series.ewm(span=slow, adjust=False).mean()
        macd = exp1 - exp2
        signal_line = macd.ewm(span=signal, adjust=False).mean()
        return macd, signal_line, macd - signal_line

    def _calculate_bollinger(self, series, period=20, std=2):
        ma = series.rolling(window=period).mean()
        msd = series.rolling(window=period).std()
        return ma + std * msd, ma, ma - std * msd
