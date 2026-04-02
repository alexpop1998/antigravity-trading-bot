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
        self.analyst = LLMAnalyst(self.bot)
        self.regime_detector = RegimeDetector()
        
        # v29 logic config
        self.adx_threshold = 25
        self.rsi_buy_level = 30
        self.rsi_sell_level = 70
        self.technical_confluence_mode = "strict" # v29 favorite
    
    async def get_technical_score(self, symbol: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        [v31.0 LIGHTNING SCORE + v29 CONFLUENCE]
        Calculates a technical-only score for pre-filtering.
        Includes RSI, MACD, Bollinger, and Momentum Guards.
        """
        try:
            df = data.get('df')
            if df is None or len(df) < 50:
                return {'symbol': symbol, 'tech_score': 0.0, 'side': 'buy'}

            # ⚙️ Indicators
            close = df['close']
            df['rsi'] = self._calculate_rsi(close)
            df['macd'], df['signal'], df['hist'] = self._calculate_macd(close)
            df['bb_up'], df['bb_mid'], df['bb_low'] = self._calculate_bollinger(close)
            df['ema200'] = close.ewm(span=200, adjust=False).mean()
            
            # Latest values
            price = df['close'].iloc[-1]
            rsi = df['rsi'].iloc[-1]
            macd_hist = df['hist'].iloc[-1]
            bb_up = df['bb_up'].iloc[-1]
            bb_low = df['bb_low'].iloc[-1]
            ema200 = df['ema200'].iloc[-1]
            
            # --- V29 MOMENTUM GUARD ---
            change_5m = (price - df['close'].iloc[-4]) / df['close'].iloc[-4] # ~5 min check (15m candles, taking last 4 samples of 10s is better but here we have 15m df)
            # Adjusting for 15m TF: check last candle
            change_15m = (price - df['close'].iloc[-2]) / df['close'].iloc[-2]

            # 1. Regime Detection
            regime_type, confidence = self.regime_detector.detect_regime(data)
            
            # 2. MTF Trend (Directional)
            trend_bias = await self._check_mtf_trend(symbol) # 1 for Long, -1 for Short
            
            # 3. CONFLUENCE LOGIC (V29)
            is_rsi_buy = rsi <= self.rsi_buy_level
            is_bb_buy = price <= (bb_low * 1.01)
            is_macd_buy = macd_hist > 0
            
            is_rsi_sell = rsi >= self.rsi_sell_level
            is_bb_sell = price >= (bb_up * 0.99)
            is_macd_sell = macd_hist < 0

            tech_buy = False
            tech_sell = False
            
            if self.technical_confluence_mode == "loose":
                tech_buy = is_rsi_buy or is_bb_buy
                tech_sell = is_rsi_sell or is_bb_sell
            else: # Strict
                tech_buy = is_rsi_buy and is_bb_buy and is_macd_buy
                tech_sell = is_rsi_sell and is_bb_sell and is_macd_sell

            # 4. SIZING MODIFIER (Funding/Momentum)
            size_multiplier = 1.0
            
            # --- MOMENTUM GUARD (V29) ---
            if tech_sell and change_15m > 0.015:
                logger.info(f"🛡️ [MOMENTUM GUARD] Blocking SHORT on {symbol}: Bullish surge {change_15m:.2%}")
                tech_sell = False

            # --- FUNDING PENALTY (V29) ---
            funding_rate = data.get('funding_rate', 0)
            if tech_buy and funding_rate > 0.0005: # High funding
                size_multiplier *= 0.5
                logger.warning(f"⚠️ [FUNDING PENALTY] High funding on {symbol} ({funding_rate:.4%}). Sizing x0.5")
            elif tech_sell and funding_rate < -0.0005: # Negative funding
                size_multiplier *= 0.5
                logger.warning(f"⚠️ [FUNDING PENALTY] Negative funding on {symbol} ({funding_rate:.4%}). Sizing x0.5")

            # 5. FINAL TECH SCORE
            score = 0.0
            side = "buy" if tech_buy else ("sell" if tech_sell else "neutral")
            
            if tech_buy:
                score = 0.6 if trend_bias >= 0 else 0.4
            elif tech_sell:
                score = 0.6 if trend_bias <= 0 else 0.4
                
            return {
                'symbol': symbol,
                'tech_score': score,
                'side': side,
                'regime': regime_type,
                'trend_bias': trend_bias,
                'size_multiplier': size_multiplier
            }
        except Exception as e:
            logger.error(f"Error in technical scoring for {symbol}: {e}")
            return {'symbol': symbol, 'tech_score': 0.0, 'side': 'buy'}

    async def analyze_opportunity(self, symbol: str, data: Dict[str, Any], tech_snapshot: Dict = None) -> Dict[str, Any]:
        """
        Deep analysis of a single symbol, now AI-optimized.
        """
        try:
            # Use pre-calculated data if available
            snapshot = tech_snapshot or await self.get_technical_score(symbol, data)
            side = snapshot['side']
            tech_score = snapshot['tech_score']
            
            # --- V11.8 COOLDOWN CHECK ---
            if self.analyst.is_in_cooldown(symbol):
                logger.debug(f"⏭️ [COOLDOWN] {symbol} skipped LLM (too frequent)")
                return {'symbol': symbol, 'score': 0.0, 'reason': 'ai_cooldown'}

            # 3. AI Deep Audit (The Costly Part)
            indicators = data if data else {}
            # Pass side to AI so it evaluates the CORRECT direction
            result = await self.analyst.decide_strategy(
                symbol=symbol,
                side=side,
                signal_type="DEEP_RANKED_SCAN",
                indicators=indicators
            )
            
            # Safe unpack
            approved = result[0] if result else False
            ai_confidence = result[1] if result and len(result) > 1 else 0.0
            leverage = result[2] if result and len(result) > 2 else self.bot.leverage
            reason = result[6] if result and len(result) > 6 else "N/A"
            
            # Update cooldown on success
            self.analyst.update_cooldown(symbol)
            
            # 4. Final Consensus Score (Weight: 60% AI, 40% Technical)
            final_score = (0.6 * ai_confidence) + (0.4 * tech_score) if approved else 0.0
            
            direction = f"{side.upper()} {'🟢' if side=='buy' else '🔴'}" if final_score >= 0.7 else "SKIP ⏭️"
            logger.info(
                f"🧠 [STRATEGY] {symbol} → Tech: {tech_score:.2f} | "
                f"AI: {reason} (Conf: {ai_confidence:.2f}) | Final: {final_score:.2f} → {direction}"
            )
            
            return {
                'symbol': symbol,
                'score': final_score,
                'ai_approved': approved,
                'ai_reason': reason,
                'confidence': ai_confidence,
                'leverage': leverage,
                'side': side
            }
            
        except Exception as e:
            logger.error(f"❌ Error during strategy evaluation for {symbol}: {e}")
            return {'symbol': symbol, 'score': 0.0, 'error': str(e)}

    async def _check_mtf_trend(self, symbol: str) -> int:
        """Fetches 4h candles and returns trend direction (1 for Long, -1 for Short, 0 for Neutral)."""
        try:
            ohlcv_4h = await self.bot.gateway.exchange.fetch_ohlcv(symbol, '4h', limit=100)
            df_4h = pd.DataFrame(ohlcv_4h, columns=['t','o','h','l','c','v'])
            ema200 = df_4h['c'].ewm(span=200, adjust=False).mean().iloc[-1]
            last_close = df_4h['c'].iloc[-1]
            
            if last_close > (ema200 * 1.002): # Clear Bullish
                 return 1
            elif last_close < (ema200 * 0.998): # Clear Bearish
                 return -1
            return 0 # Sideways/Neutral
        except Exception as e:
            logger.error(f"MTF Error for {symbol}: {e}")
            return 0

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
