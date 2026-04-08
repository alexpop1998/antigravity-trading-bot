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
        self.ai_cache_ttl = self.bot.config.get('operational_params', {}).get('ai_cache_ttl', 3600)

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
            
            # 1. Trend Baseline (4H Gravity Audit)
            trend_data = await self._check_mtf_trend(symbol)
            trend_bias = trend_data['bias']
            ema200 = trend_data['ema200']
            last_close = trend_data['last_close']
            
            # v55.9.0 [INSTITUTIONAL CORE] Parameter Injection
            strategic = self.bot.config.get('strategic_params', {})
            tg_mode = strategic.get('trend_guard_mode', 'STRICT').upper()
            allow_neutral = strategic.get('allow_neutral_trend_trading', False)        
            bb_tol_buy = strategic.get('bb_tolerance_buy', 1.01)
            bb_tol_sell = strategic.get('bb_tolerance_sell', 0.99)
            
            is_rsi_buy = rsi <= self.rsi_buy_level
            is_bb_buy = price <= (bb_low * bb_tol_buy)
            is_macd_buy = macd_hist > 0
            
            is_rsi_sell = rsi >= self.rsi_sell_level
            is_bb_sell = price >= (bb_up * bb_tol_sell)
            is_macd_sell = macd_hist < 0

            if self.technical_confluence_mode == "loose":
                tech_buy = is_rsi_buy or is_bb_buy or is_macd_buy
                tech_sell = is_rsi_sell or is_bb_sell or is_macd_sell
            else: 
                tech_buy = is_rsi_buy and is_bb_buy and is_macd_buy
                tech_sell = is_rsi_sell and is_bb_sell and is_macd_sell

            # 4. SIZING MODIFIER (v55.9.1 [TOTAL INJECTION])
            size_multiplier = 1.0
            vol_prot = strategic.get('volatility_protection_threshold', 0.015)
            fund_threshold = strategic.get('funding_penalty_threshold', 0.0005)
            
            if tech_sell and change_15m > vol_prot:
                tech_sell = False

            funding_rate = data.get('funding_rate', 0)
            if tech_buy and funding_rate > fund_threshold: 
                size_multiplier *= 0.5
            elif tech_sell and funding_rate < -fund_threshold: 
                size_multiplier *= 0.5

            # 5. FINAL TECH SCORE (v5.3.0 [ROBUST PIPELINE])
            strategic = self.bot.config.get('strategic_params', {})
            tg_mode = strategic.get('trend_guard_mode', 'STRICT').upper()
            allow_neutral = strategic.get('allow_neutral_trend_trading', False)
            
            score = 0.0
            side = "buy" if tech_buy else ("sell" if tech_sell else "neutral")
            
            # Trend Alignment Logic (Gatekeeper Phase)
            is_aligned = False
            if side == "buy":
                if trend_bias == 1: is_aligned = True
                elif trend_bias == 0 and allow_neutral: is_aligned = True
            elif side == "sell":
                if trend_bias == -1: is_aligned = True
                elif trend_bias == 0 and allow_neutral: is_aligned = True
            
            # [TREND AUDIT] Extreme visibility
            trend_str = "UP" if trend_bias == 1 else ("DOWN" if trend_bias == -1 else "NEUTRAL")
            logger.info(f"📍 [TREND AUDIT] {symbol} | Price: {last_close:.4f} | EMA200: {ema200:.4f} | Bias: {trend_str} | Target: {side.upper()} | Aligned: {is_aligned}")
            
            if side != "neutral":
                if is_aligned:
                    score = strategic.get('tech_score_aligned', 0.6)
                else:
                    # Counter-Trend Handling
                    if tg_mode == "STRICT":
                        score = 0.0
                        logger.warning(f"🛡️ [GATEKEEPER] Hard-blocking counter-trend setup on {symbol}")
                    elif tg_mode == "FLEXIBLE":
                        score = strategic.get('tech_score_mismatch', 0.1)
                    else: # OFF
                        score = strategic.get('tech_score_mismatch', 0.4)
                
            return {
                'symbol': symbol,
                'tech_score': score,
                'side': side,
                'atr': df['atr'].iloc[-1],
                'price': price,
                'rsi': rsi,
                'regime': regime_type,
                'trend_bias': trend_bias,
                'is_aligned': is_aligned,
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
            
            # [INSTITUTIONAL CORE] Data-Driven Configuration
            strategic = self.bot.config.get('strategic_params', {})
            preaudit_threshold = strategic.get('preaudit_tech_threshold', 0.45)
            
            # Trend Guard Override Check (Legacy support or emergency bypass)
            if strategic.get('force_trend_bias_bypass', False) and strategic.get('trend_guard_mode') == 'OFF':
                # Only allow bypass if Trend Guard is explicitly OFF in config
                snapshot['trend_bias'] = 1 if side == 'buy' else -1

            if tech_score < preaudit_threshold:
                return {'symbol': symbol, 'score': 0.0, 'side': side, 'reason': 'low_tech_score_prefilter'}

            now = time.time()
            if symbol in self.ai_cache:
                cached_data, timestamp = self.ai_cache[symbol]
                if (now - timestamp) < self.ai_cache_ttl:
                    return cached_data

            # v43.3 [GWEN FIX] Persist the price at analysis time for Slippage Guard
            ref_price = snapshot.get('price', 0)
            
            # --- [GATEKEEPER v5.3.0] MANDATORY HIERARCHY ---
            is_aligned = snapshot.get('is_aligned', False)
            tg_mode = strategic.get('trend_guard_mode', 'STRICT').upper()
            
            if tg_mode == "STRICT" and not is_aligned:
                logger.warning(f"🛡️ [GATEKEEPER] Aborting analyze_opportunity for {symbol} due to Trend Mismatch. AI call Bypassed.")
                return {'symbol': symbol, 'score': 0.0, 'side': side, 'reason': 'hard_trend_block'}

            trend_bias = snapshot.get('trend_bias', 0)
            mtf_context = "UPTREND (EMA200 4H)" if trend_bias == 1 else ("DOWNTREND (EMA200 4H)" if trend_bias == -1 else "NEUTRAL")

            result = await self.analyst.decide_strategy(
                symbol=symbol,
                side=side,
                signal_type="DEEP_RANKED_SCAN",
                indicators=data,
                mtf_context=mtf_context
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
            
            # v55.9.1 Data-Driven Boost (v5.2.2 [ZERO BYPASS])
            boost_trigger = strategic.get('ai_boost_trigger', 0.85)
            boost_score = strategic.get('ai_boost_score', 0.99)
            boost_lev = strategic.get('ai_boost_leverage', 50)
            
            # THE HARD GUARD (v5.3.0 [ZERO BYPASS])
            is_aligned = snapshot.get('is_aligned', False)
            tg_mode = strategic.get('trend_guard_mode', 'STRICT').upper()
            
            if not is_aligned and tg_mode == "STRICT":
                # Final safeguard, though GATEKEEPER should have already aborted.
                final_score = 0.0
            elif approved and strategic.get('enable_confidence_boost', False):
                if ai_confidence > boost_trigger: 
                    logger.info(f"⚡ [CONFIG BOOST] High AI Confidence ({ai_confidence}) -> Forcing {boost_score} Score.")
                    final_score = boost_score
                    leverage = boost_lev
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
            bias_data, timestamp = self.mtf_cache[symbol]
            if (now - timestamp) < 3600: # 1h TTL
                return bias_data

        try:
            ohlcv_4h = await self.bot.gateway.exchange.fetch_ohlcv(symbol, '4h', limit=100)
            df_4h = pd.DataFrame(ohlcv_4h, columns=['t','o','h','l','c','v'])
            ema200 = df_4h['c'].ewm(span=200, adjust=False).mean().iloc[-1]
            last_close = df_4h['c'].iloc[-1]
            
            ema_thresh = self.bot.config.get('strategic_params', {}).get('mtf_ema_threshold', 0.002)
            bias = 0
            if last_close > (ema200 * (1 + ema_thresh)): bias = 1
            elif last_close < (ema200 * (1 - ema_thresh)): bias = -1
            
            res = {'bias': bias, 'ema200': ema200, 'last_close': last_close}
            self.mtf_cache[symbol] = (res, now)
            return res
        except Exception as e:
            logger.error(f"MTF Error for {symbol}: {e}")
            return {'bias': 0, 'ema200': 0, 'last_close': 0}

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
