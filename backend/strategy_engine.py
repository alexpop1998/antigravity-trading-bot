import logging
import asyncio
import time
from typing import Dict, Any, List, Optional
import pandas as pd
from ml_predictor import MLPredictor
from llm_analyst import LLMAnalyst
from regime_detector import RegimeDetector

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
        self.mtf_cache = {} # v44.1.0 [GWEN OPTIMIZATION]
        
        # v29 logic config
        tp = getattr(bot_instance, 'config', {}).get('trading_parameters', {})
        self.rsi_buy_level = tp.get('rsi_buy_level', 30)
        self.rsi_sell_level = tp.get('rsi_sell_level', 70)
        self.technical_confluence_mode = tp.get('technical_confluence_mode', 'strict')
        
        # v38.1 Gemini Cost Optimization
        self.ai_cache = {} 
        self.ai_cache_ttl = 1800 

    async def get_technical_score(self, symbol: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculates a technical-only score for pre-filtering.
        Includes RSI, MACD, Bollinger, and Momentum Guards.
        """
        try:
            df = data.get('df')
            if df is None or len(df) < 50:
                return {'symbol': symbol, 'tech_score': 0.0, 'side': 'buy'}

            # Indicators
            close = df['close']
            df['rsi'] = self._calculate_rsi(close)
            df['macd'], df['signal'], df['hist'] = self._calculate_macd(close)
            df['bb_up'], df['bb_mid'], df['bb_low'] = self._calculate_bollinger(close)
            df['atr'] = self._calculate_atr(df)
            df['ema200'] = close.ewm(span=200, adjust=False).mean()
            
            # Latest
            price = df['close'].iloc[-1]
            rsi = df['rsi'].iloc[-1]
            macd_hist = df['hist'].iloc[-1]
            bb_up = df['bb_up'].iloc[-1]
            bb_low = df['bb_low'].iloc[-1]
            
            change_15m = (price - df['close'].iloc[-2]) / df['close'].iloc[-2]

            # 1. Regime Detection
            regime_type, confidence = self.regime_detector.detect_regime(df)
            
            # 2. MTF Trend (Directional)
            trend_bias = await self._check_mtf_trend(symbol)
            
            # 3. CONFLUENCE (v43.3 [GWEN FIX])
            is_rsi_buy = rsi <= self.rsi_buy_level
            is_bb_buy = price <= (bb_low * 1.01)
            is_macd_buy = macd_hist > 0
            
            is_rsi_sell = rsi >= self.rsi_sell_level
            is_bb_sell = price >= (bb_up * 0.99)
            is_macd_sell = macd_hist < 0

            if self.technical_confluence_mode == "loose":
                tech_buy = is_rsi_buy or is_bb_buy or is_macd_buy
                tech_sell = is_rsi_sell or is_bb_sell or is_macd_sell
            else: 
                tech_buy = is_rsi_buy and is_bb_buy and is_macd_buy
                tech_sell = is_rsi_sell and is_bb_sell and is_macd_sell

            # 4. SIZING MODIFIER
            size_multiplier = 1.0
            if tech_sell and change_15m > 0.015:
                tech_sell = False

            funding_rate = data.get('funding_rate', 0)
            if tech_buy and funding_rate > 0.0005: 
                size_multiplier *= 0.5
            elif tech_sell and funding_rate < -0.0005: 
                size_multiplier *= 0.5

            # 5. FINAL TECH SCORE
            score = 0.0
            side = "buy" if tech_buy else ("sell" if tech_sell else "neutral")
            
            if tech_buy:
                # v55.8.0 [HARD GUARD] Zero score if LONG in DOWNTREND
                if trend_bias == -1:
                    score = 0.0
                else:
                    score = 0.6 if trend_bias >= 0 else 0.4
            elif tech_sell:
                # v55.8.0 [HARD GUARD] Zero score if SHORT in UPTREND
                if trend_bias == 1:
                    score = 0.0
                else:
                    score = 0.6 if trend_bias <= 0 else 0.4
                
            return {
                'symbol': symbol,
                'tech_score': score,
                'side': side,
                'atr': df['atr'].iloc[-1],
                'price': price,
                'rsi': rsi,
                'regime': regime_type,
                'trend_bias': trend_bias,
                'size_multiplier': size_multiplier
            }
        except Exception as e:
            logger.error(f"Error in technical scoring for {symbol}: {e}")
            return {'symbol': symbol, 'tech_score': 0.0, 'side': 'buy'}

    async def analyze_opportunity(self, symbol: str, data: Dict[str, Any], tech_snapshot: Dict = None) -> Dict[str, Any]:
        """Deep analysis of a single symbol, AI-optimized."""
        side = "neutral"
        try:
            snapshot = tech_snapshot or await self.get_technical_score(symbol, data)
            side = snapshot['side']
            tech_score = snapshot['tech_score']
            
            # v43.3.1 Data-Driven Configuration for Pre-Audit and Bias
            strategic = self.bot.config.get('strategic_params', {})
            preaudit_threshold = strategic.get('preaudit_tech_threshold', 0.45)
            
            # [GWEN PURGE] force_trend_bias_bypass is now strictly FORBIDDEN for Blitz safety
            if strategic.get('force_trend_bias_bypass', False):
                logger.warning(f"⚠️ [SAFETY] Bypass detected in config but OVERRIDDEN for {symbol}. Trend Guard is Mandatory.")
                # We do NOT allow bypass anymore in this emergency fix

            if tech_score < preaudit_threshold:
                return {'symbol': symbol, 'score': 0.0, 'side': side, 'reason': 'low_tech_score_prefilter'}

            now = time.time()
            if symbol in self.ai_cache:
                cached_data, timestamp = self.ai_cache[symbol]
                if (now - timestamp) < self.ai_cache_ttl:
                    return cached_data

            # v43.3 [GWEN FIX] Persist the price at analysis time for Slippage Guard
            ref_price = snapshot.get('price', 0)
            
            result = await self.analyst.decide_strategy(
                symbol=symbol,
                side=side,
                signal_type="DEEP_RANKED_SCAN",
                indicators=data
            )
            
            if result[-1] == "RATE_LIMIT_429" and strategic.get('llm_cooldown_bypass', False):
                if tech_score >= 0.75:
                    return {'symbol': symbol, 'score': 0.90, 'side': side, 'reason': 'llm_bypassed', 'leverage': 25, 'reference_price': ref_price}

            approved = result[0] if result else False
            ai_confidence = result[1] if result and len(result) > 1 else 0.0
            leverage = result[3] if result and len(result) > 3 else self.bot.leverage
            reason = result[7] if result and len(result) > 7 else "N/A"
            ai_side = result[8] if result and len(result) > 8 else side
           
            final_side = ai_side if approved else side
            if approved:
                self.analyst.update_cooldown(symbol)
            
            aligned_tech_score = tech_score if side.lower() == final_side.lower() else 0.0
            
            # v43.3.1 Data-Driven Boost
            if approved and strategic.get('enable_confidence_boost', False):
                if ai_confidence > 0.85: 
                    logger.info(f"⚡ [CONFIG BOOST] High AI Confidence ({ai_confidence}) -> Forcing 0.99 Score.")
                    final_score = 0.99
                    leverage = 50
                else:
                    final_score = max(0.81, (0.7 * ai_confidence) + (0.3 * aligned_tech_score))
                    logger.info(f"⚡ [CONFIG MELD] Score: {final_score:.2f} (AI: {ai_confidence:.2f}, Tech: {aligned_tech_score:.2f})")
            else:
                final_score = (0.6 * ai_confidence) + (0.4 * aligned_tech_score) if approved else 0.0
            
            res = {
                'symbol': symbol,
                'score': final_score,
                'ai_approved': approved,
                'ai_reason': reason,
                'confidence': ai_confidence,
                'leverage': leverage,
                'side': final_side,
                'reference_price': ref_price
            }
            # Boat v125. Boat
            self.ai_cache[symbol] = (res, time.time())
            return res
        except Exception as e:
            logger.error(f"❌ Error during strategy evaluation for {symbol}: {e}")
            return {'symbol': symbol, 'score': 0.0, 'error': str(e), 'side': side}

    async def _check_mtf_trend(self, symbol: str) -> int:
        now = time.time()
        if symbol in self.mtf_cache:
            bias, timestamp = self.mtf_cache[symbol]
            if (now - timestamp) < 3600: # 1h TTL
                return bias

        try:
            ohlcv_4h = await self.bot.gateway.exchange.fetch_ohlcv(symbol, '4h', limit=100)
            df_4h = pd.DataFrame(ohlcv_4h, columns=['t','o','h','l','c','v'])
            ema200 = df_4h['c'].ewm(span=200, adjust=False).mean().iloc[-1]
            last_close = df_4h['c'].iloc[-1]
            
            bias = 0
            if last_close > (ema200 * 1.002): bias = 1
            elif last_close < (ema200 * 0.998): bias = -1
            
            self.mtf_cache[symbol] = (bias, now)
            return bias
        except Exception as e:
            logger.error(f"MTF Error for {symbol}: {e}")
            return 0

    def _calculate_rsi(self, series, period=14):
        delta = series.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / (loss + 1e-10)
        return 100 - (100 / (1 + rs))

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

    def _calculate_atr(self, df, period=14):
        high = df['high']
        low = df['low']
        close = df['close']
        tr1 = high - low
        tr2 = abs(high - close.shift())
        tr3 = abs(low - close.shift())
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        return tr.rolling(window=period).mean()
