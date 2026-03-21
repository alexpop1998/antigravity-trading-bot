import ccxt.async_support as ccxt
import pandas as pd
import asyncio
import traceback
import logging
from dotenv import load_dotenv
import os
import json
import time
from ml_predictor import MLPredictor
from database import BotDatabase
from telegram_notifier import TelegramNotifier
from signal_manager import SignalManager

load_dotenv(override=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("TradingBot")
class CryptoBot:
    def __init__(self, config_file="client_config.json"):
        # Load SaaS Client Risk Profiles
        self.config_file = config_file
        self.risk_profile = self._load_risk_profile()
        
        params = self.risk_profile.get("trading_parameters", {})
        self.symbols = params.get("symbols", ["BTC/USDT"])
        self.timeframe = params.get("timeframe", "15m")
        self.rsi_period = params.get("rsi_period", 14)
        self.leverage = params.get("leverage", 10)
        self.usdt_per_trade = params.get("usdt_per_trade", 50)
        self.stop_loss_pct = params.get("stop_loss_pct", 0.02)
        self.take_profit_pct = params.get("take_profit_pct", 0.06)
        self.max_concurrent_positions = params.get("max_concurrent_positions", 10)
        
        api_key = os.getenv("EXCHANGE_API_KEY")
        api_secret = os.getenv("EXCHANGE_API_SECRET")
        # binance doesn't need a passphrase
        
        # Initialize Binance USDT-M Futures explicitly
        self.exchange = ccxt.binanceusdm({
            'apiKey': api_key,
            'secret': api_secret,
            'enableRateLimit': True,
            'timeout': 10000, # 10 seconds timeout
        })
        
        # Set to sandbox mode based on .env
        sandbox_mode = os.getenv("BINANCE_SANDBOX", "true").lower() == "true"
        self.exchange.set_sandbox_mode(sandbox_mode)
        
        mode_str = "BINANCE TESTNET" if sandbox_mode else "BINANCE PRODUCTION"
        logger.warning(f"🚀 INITIALIZING BOT IN {mode_str} MODE")
        
        # Debug: Check if keys are loaded
        if api_key:
            logger.info(f"Using API Key: {api_key[:4]}...{api_key[-4:]} (Length: {len(api_key)})")
        else:
            logger.error("EXCHANGE_API_KEY IS EMPTY!")
        
        # Active symbols data
        self.latest_data: dict[str, dict] = {}
        for symbol in self.symbols:
            self.latest_data[symbol] = {
                'price': 0.0,
                'change': 0.0,
                'changePercent': 0.0,
                'rsi': 0.0,
                'imbalance': 1.0
            }
            
        self.latest_account_data = {
            'positions': [],
            'trades': [],
            'balance': 0.0,
            'equity': 0.0,
            'unrealized_pnl': 0.0,
            'alerts': []
        }
        
        # Keep track of active positions (LONG/SHORT/None)
        self.active_positions: dict[str, str | None] = {symbol: None for symbol in self.symbols}
        # Keep track of soft stop-loss levels and trade info
        self.trade_levels: dict[str, dict | None] = {symbol: None for symbol in self.symbols}
        self.is_macro_paused = False
        
        # Initialize Machine Learning Prophet
        self.predictor = MLPredictor()
        self.ml_cooldowns: dict[str, float] = {symbol: 0.0 for symbol in self.symbols}
        
        # Institutional Alert History for Frontend
        self.alert_history = []
        self.max_alerts = 50
        
        # VPS Modules
        self.db = BotDatabase()
        self.notifier = TelegramNotifier()
        self.signal_manager = SignalManager(self)
        
        # Resource management
        self.ml_semaphore = asyncio.Semaphore(1) # Concurrent ML training limit
        self.api_semaphore = asyncio.Semaphore(5) # Concurrent API call limit
        
        # Risk Management & Guards
        self.circuit_breaker_active = False
        self.symbol_blacklist = set()
        self.initial_wallet_balance = None
        
        # Recupero stato persistente
        saved_levels = self.db.load_state("trade_levels")
        if saved_levels:
            self.trade_levels = saved_levels
            logger.info("📦 Stato trade_levels recuperato correttamente dal database.")
        
        # Exchange instance will be initialized with load_markets in initialize()
        self.initialized = False

    async def initialize(self):
        if self.initialized:
            return
            
        try:
            logger.info("📡 Loading markets from Binance (Async)...")
            await self.exchange.load_markets()
            logger.info("✅ Markets loaded successfully.")
            
            # Filter symbols to only include those available on the current exchange
            available_symbols = []
            for s in self.symbols:
                if s in self.exchange.markets:
                    available_symbols.append(s)
                else:
                    logger.warning(f"⚠️ Symbol {s} not available on this exchange. Skipping.")
            
            if not available_symbols:
                logger.error("❌ NO VALID SYMBOLS FOUND ON THIS EXCHANGE!")
            
            logger.info(f"🎯 Validated {len(available_symbols)} symbols.")
            self.symbols = available_symbols
            
            logger.info("⚙️ Setting leverage for all symbols...")
            await self._set_leverage_for_all()
            logger.info("✅ Leverage set successfully.")
            
            self.initialized = True
        except Exception as e:
            logger.error(f"❌ Failed during bot initialization: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise e

    def _load_risk_profile(self):
        try:
            config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), self.config_file)
            with open(config_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Could not load risk profile config: {e}. Using defaults.")
            return {}

    async def _set_leverage_for_all(self):
        for symbol in self.symbols:
            try:
                # CCXT way to set margin/leverage on Binance Futures
                market = self.exchange.market(symbol)
                await self.exchange.set_leverage(self.leverage, market['id'])
                logger.info(f"Set leverage to {self.leverage}x for {symbol}")
            except Exception as e:
                logger.error(f"Could not set leverage for {symbol}: {e}")

    async def fetch_data_and_analyze(self, symbol):
        try:
            # Using direct await instead of asyncio.to_thread with async_support
            ohlcv = await self.exchange.fetch_ohlcv(symbol, self.timeframe, limit=100)
            
            if not ohlcv:
                logger.warning(f"No data for {symbol}")
                return
                
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            
            # Calculate RSI manually to avoid pandas-ta dependency issues
            delta = df['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=self.rsi_period).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=self.rsi_period).mean()
            rs = gain / loss
            df['rsi'] = 100 - (100 / (1 + rs))
            
            # Get latest values
            current_close = df['close'].iloc[-1]
            prev_close = df['close'].iloc[-2]
            current_rsi = df['rsi'].iloc[-1]
            
            # --- MACD Calculation ---
            # MACD Line = 12-period EMA - 26-period EMA
            ema12 = df['close'].ewm(span=12, adjust=False).mean()
            ema26 = df['close'].ewm(span=26, adjust=False).mean()
            df['macd'] = ema12 - ema26
            # Signal Line = 9-period EMA of MACD
            df['macd_signal'] = df['macd'].ewm(span=9, adjust=False).mean()
            # MACD Histogram
            df['macd_hist'] = df['macd'] - df['macd_signal']
            
            current_macd = df['macd'].iloc[-1]
            current_macd_signal = df['macd_signal'].iloc[-1]
            current_macd_hist = df['macd_hist'].iloc[-1]
            
            # --- Bollinger Bands Calculation ---
            # 20-period SMA and Std Dev
            sma20 = df['close'].rolling(window=20).mean()
            std20 = df['close'].rolling(window=20).std()
            
            df['bb_upper'] = sma20 + (std20 * 2)
            df['bb_lower'] = sma20 - (std20 * 2)
            df['bb_mid'] = sma20
            
            current_bb_upper = df['bb_upper'].iloc[-1]
            current_bb_lower = df['bb_lower'].iloc[-1]
            
            # --- ATR (Average True Range) Calculation ---
            df['tr0'] = abs(df['high'] - df['low'])
            df['tr1'] = abs(df['high'] - df['close'].shift())
            df['tr2'] = abs(df['low'] - df['close'].shift())
            df['tr'] = df[['tr0', 'tr1', 'tr2']].max(axis=1)
            df['atr'] = df['tr'].rolling(window=14).mean()
            current_atr = df['atr'].iloc[-1]
            
            # --- Order Book Imbalance Calculation ---
            orderbook = await self.exchange.fetch_order_book(symbol, limit=100)
            bids = sum(float(bid[1]) for bid in orderbook['bids'])
            asks = sum(float(ask[1]) for ask in orderbook['asks'])
            imbalance = bids / asks if asks > 0 else 1.0
            
            ticker = await self.exchange.fetch_ticker(symbol)
            
            # Optimize: Only re-train/predict every 30 minutes to save CPU, and only one at a time
            import time
            current_time = time.time()
            if current_time - self.ml_cooldowns.get(symbol, 0) > 1800: # 30 minutes
                try:
                    async with self.ml_semaphore:
                        # ML predictor is still sync, keep to_thread for it
                        predicted_price = await asyncio.to_thread(self.predictor.train_and_predict, symbol, df)
                except Exception as mle:
                    logger.error(f"⚠️ ML Training failed for {symbol}: {mle}")
                    predicted_price = "ML Error"
                self.ml_cooldowns[symbol] = current_time
            else:
                # Use cached prediction if available
                predicted_price = self.latest_data[symbol].get('prediction', "Syncing ML...")
            
            self.latest_data[symbol] = {
                'price': current_close,
                'change': ticker['change'] if ticker['change'] is not None else (current_close - prev_close),
                'changePercent': ticker['percentage'] if ticker['percentage'] is not None else (((current_close - prev_close) / prev_close) * 100),
                'rsi': round(current_rsi, 2) if not pd.isna(current_rsi) else "N/A",
                'macd': round(current_macd, 4) if not pd.isna(current_macd) else "N/A",
                'macd_hist': round(current_macd_hist, 4) if not pd.isna(current_macd_hist) else "N/A",
                'bb_upper': round(current_bb_upper, 4) if not pd.isna(current_bb_upper) else "N/A",
                'bb_lower': round(current_bb_lower, 4) if not pd.isna(current_bb_lower) else "N/A",
                'imbalance': round(imbalance, 2),
                'atr': round(current_atr, 6) if not pd.isna(current_atr) else "N/A",
                'prediction': predicted_price if predicted_price else "Syncing ML..."
            }
            
            self._check_soft_stop_loss(symbol, current_close)
            self._check_trading_signals(symbol, current_close, current_rsi, current_macd_hist, current_bb_lower, current_bb_upper, imbalance)
            
        except Exception as e:
            logger.error(f"Error analyzing {symbol}: {e}")

    def _check_trading_signals(self, symbol, price, rsi, macd_hist, bb_lower, bb_upper, imbalance):
        if pd.isna(rsi) or pd.isna(macd_hist) or pd.isna(bb_lower):
            return
            
        if self.is_macro_paused:
            # We skip creating new positions due to explosive un-predictable volatility
            return

        # Check absolute account-wide position limit (including manual or old trades)
        current_open_positions = len(self.latest_account_data.get('positions', []))
        if current_open_positions >= self.max_concurrent_positions:
            # We are at capacity, no new entries allowed
            return

        # 1. Order Book Imbalance Strategy (The Invisible Wall)
        if imbalance > 3.0 and self.active_positions[symbol] != 'LONG':
            logger.info(f"🧱 WALL DETECTED: Massive Buy Wall for {symbol}. Bids {imbalance:.1f}x Asks. LONG signal.")
            self.active_positions[symbol] = 'LONG'
            asyncio.create_task(self.execute_order(symbol, 'buy', price))
            return
            
        elif imbalance < 0.33 and self.active_positions[symbol] != 'SHORT':
            logger.info(f"🧱 WALL DETECTED: Massive Sell Wall for {symbol}. Asks {1/imbalance:.1f}x Bids. SHORT signal.")
            self.active_positions[symbol] = 'SHORT'
            asyncio.create_task(self.execute_order(symbol, 'sell', price))
            return
            
        # 2. Classic Technical Strategy (RSI + MACD + Bollinger)
        is_technical_signal = False
        if rsi < 30 and macd_hist > 0 and price <= (bb_lower * 1.01) and self.active_positions[symbol] != 'LONG':
            logger.info(f"[{symbol}] OVERSOLD CONFIRMED: RSI={rsi:.2f}, MACD_Hist>0, Price near BB_Lower. LONG signal at {price}.")
            self.active_positions[symbol] = 'LONG'
            asyncio.create_task(self.execute_order(symbol, 'buy', price))
            is_technical_signal = True
            
        elif rsi > 70 and macd_hist < 0 and price >= (bb_upper * 0.99) and self.active_positions[symbol] != 'SHORT':
            logger.info(f"[{symbol}] OVERBOUGHT CONFIRMED: RSI={rsi:.2f}, MACD_Hist<0, Price near BB_Upper. SHORT signal at {price}.")
            self.active_positions[symbol] = 'SHORT'
            asyncio.create_task(self.execute_order(symbol, 'sell', price))
            is_technical_signal = True

        # 3. AI Predictive Alpha (Machine Learning Prophet)
        # If technical indicators are neutral (not OVERSOLD/OVERBOUGHT), consult the AI Prophet
        if not is_technical_signal and self.active_positions[symbol] is None:
            prediction = self.latest_data[symbol].get('prediction')
            if isinstance(prediction, (int, float)) and prediction > 0:
                price_diff_pct = (prediction - price) / price
                
                # Confidence Threshold: 0.5% expected move in the NEXT 15m candle
                if price_diff_pct > 0.005:
                    logger.critical(f"🧠 AI ALPHA: Machine Learning predicts a +{price_diff_pct*100:.2f}% move for {symbol}. LONG signal.")
                    self.active_positions[symbol] = 'LONG'
                    asyncio.create_task(self.execute_order(symbol, 'buy', price))
                elif price_diff_pct < -0.005:
                    logger.critical(f"🧠 AI ALPHA: Machine Learning predicts a {price_diff_pct*100:.2f}% move for {symbol}. SHORT signal.")
                    self.active_positions[symbol] = 'SHORT'
                    asyncio.create_task(self.execute_order(symbol, 'sell', price))

    def _check_soft_stop_loss(self, symbol, current_price):
        if not self.trade_levels.get(symbol):
            return
            
        trade = self.trade_levels[symbol]
        # Normalize side (handle both 'buy'/'long' and 'sell'/'short')
        side = str(trade.get('side', 'buy')).lower()
        is_long = side in ['buy', 'long']
        sl = trade.get('sl', 0)
        tp = trade.get('tp', 0)
        
        # --- Trailing Stop-Loss Logic ---
        if 'highest_price' not in trade and is_long:
            trade['highest_price'] = current_price
        if 'lowest_price' not in trade and not is_long:
            trade['lowest_price'] = current_price
            
        should_close = False
        reason = ""
        
        if is_long:
            # Update trailing high and move SL up
            if current_price > trade.get('highest_price', 0):
                trade['highest_price'] = current_price
                # Move SL up if price has moved significantly (trailing gap)
                new_sl = current_price * (1 - self.stop_loss_pct)
                if new_sl > sl:
                    trade['sl'] = new_sl
                    logger.info(f"📈 TRAILING STOP UPDATED for {symbol}: SL now at {new_sl:.6f}")
            
            if current_price <= trade.get('sl', 0):
                should_close = True
                reason = "STOP-LOSS (TRAILING)" if 'highest_price' in trade else "STOP-LOSS"
            elif current_price >= tp:
                # PARTIAL TAKE PROFIT (PTP)
                if not trade.get('tp1_hit', False):
                    logger.warning(f"🎯 PARTIAL TP1 HIT for {symbol} at {current_price}!")
                    asyncio.create_task(self.close_position(symbol, trade, partial_pct=0.5))
                    trade['tp1_hit'] = True
                    # Move SL to break-even to protect remaining 50%
                    trade['sl'] = trade.get('entry_price', current_price) 
                else:
                    # Final TP
                    should_close = True
                    reason = "TAKE-PROFIT (FINAL)"
                    
        else: # SHORT
            # Update trailing low and move SL down
            if current_price < trade.get('lowest_price', 999999):
                trade['lowest_price'] = current_price
                new_sl = current_price * (1 + self.stop_loss_pct)
                if new_sl < sl:
                    trade['sl'] = new_sl
                    logger.info(f"📉 TRAILING STOP UPDATED for {symbol}: SL now at {new_sl:.6f}")
                    
            if current_price >= trade.get('sl', 999999):
                should_close = True
                reason = "STOP-LOSS (TRAILING)" if 'lowest_price' in trade else "STOP-LOSS"
            elif current_price <= tp:
                if not trade.get('tp1_hit', False):
                    logger.warning(f"🎯 PARTIAL TP1 HIT for {symbol} at {current_price}!")
                    asyncio.create_task(self.close_position(symbol, trade, partial_pct=0.5))
                    trade['tp1_hit'] = True
                    trade['sl'] = trade.get('entry_price', current_price)
                else:
                    should_close = True
                    reason = "TAKE-PROFIT (FINAL)"
                
        if should_close:
            logger.warning(f"🔔 SOFT {reason} HIT for {symbol} at {current_price}!")
            asyncio.create_task(self.close_position(symbol, trade))
            self.trade_levels[symbol] = None
            self.db.save_state("trade_levels", self.trade_levels)

    async def close_position(self, symbol, trade, partial_pct=1.0):
        try:
            side = str(trade.get('side', 'buy')).lower()
            is_long = side in ['buy', 'long']
            close_side = 'sell' if is_long else 'buy'
            
            # --- BUG FIX: Ensure amount is formatted as a clean string for ccxt/decimal ---
            amount = float(trade.get('amount', 0)) * partial_pct
            # Precision adjustment using ccxt helper
            amount_str = self.exchange.amount_to_precision(symbol, amount)
            amount = float(amount_str)
            
            logger.info(f"🔄 Closing position for {symbol}: side={close_side}, amount_str='{amount_str}', amount_float={amount}")
            
            if amount <= 0:
                logger.warning(f"Skipping closure for {symbol}: amount too small.")
                return

            # --- BUG FIX: Use a clean float or string to avoid decimal issues in some ccxt versions ---
            logger.info(f"Closing position for {symbol} ({'PARTIAL' if partial_pct < 1 else 'FULL'}): {close_side} {amount}")
            # --- BUG FIX: Pass price=current_price even for Market orders to avoid CCXT 4.2.53 bug with None conversion ---
            # Binance ignores the price for MARKET orders anyway.
            current_price = self.latest_data[symbol].get('price', 0) if symbol in self.latest_data else 0
            await self.exchange.create_order(symbol, 'market', close_side, amount, current_price, {'reduceOnly': True})
            
            if partial_pct >= 1.0:
                logger.info(f"Position closed successfully for {symbol}.")
                self.active_positions[symbol] = None
                
                # Calculate PnL for logging
                exit_price = current_price
                entry_price = float(trade.get('entry_price', exit_price))
                pnl = (exit_price - entry_price) * amount if is_long else (entry_price - exit_price) * amount
                self.db.log_trade(symbol, "CLOSE", exit_price, amount, pnl, f"EXIT")
                
                self.notifier.notify_trade(symbol, "CLOSE", entry_price, amount, "EXIT")
            else:
                # Update remaining amount in state
                trade['amount'] = float(trade['amount']) - amount
                logger.info(f"Partial closure successful for {symbol}. Remaining: {trade['amount']}")
                
                # Log partial closure too
                exit_price = current_price
                entry_price = float(trade.get('entry_price', exit_price))
                pnl = (exit_price - entry_price) * amount if is_long else (entry_price - exit_price) * amount
                self.db.log_trade(symbol, "CLOSE_PARTIAL", exit_price, amount, pnl, "PARTIAL_EXIT")
                
        except Exception as e:
            logger.error(f"Failed to close position for {symbol}: {e}")
            logger.error(traceback.format_exc())

    # Calculate dynamic order amount based on risk capital and volatility
    def _calculate_order_amount(self, symbol, current_price, custom_leverage=None):
        leverage = custom_leverage if custom_leverage else self.leverage
        
        # Volatility-based capital allocation (Kelly Criterion proxy)
        current_atr = self.latest_data[symbol].get('atr', current_price * 0.01)
        if current_atr == "N/A" or not current_atr:
            current_atr = current_price * 0.01
            
        atr_pct = (current_atr / current_price) if current_price > 0 else 0.01
        baseline_atr_pct = 0.01  # Baseline expected volatility per candle
        
        # High volatility -> lower modifier -> less capital risked
        # Low volatility -> higher modifier -> more capital risked
        volatility_modifier = baseline_atr_pct / atr_pct if atr_pct > 0 else 1.0
        volatility_modifier = max(0.2, min(3.0, volatility_modifier))
        
        adjusted_usdt = self.usdt_per_trade * volatility_modifier
        purchasing_power = adjusted_usdt * leverage
        
        logger.info(f"[{symbol}] ATR Allocation: Modifier {volatility_modifier:.2f}x (Base: {self.usdt_per_trade} USDT -> Adjusted: {adjusted_usdt:.2f} USDT)")
        
        market = self.exchange.market(symbol)
        # Contract size calculation
        amount = purchasing_power / current_price
        
        # Adjust for exchange precision rules
        amount = self.exchange.amount_to_precision(symbol, amount)
        return float(amount)

    async def handle_signal(self, symbol, type, side, weight_modifier=1.0, is_black_swan=True):
        """Routing centralizzato dei segnali attraverso il SignalManager"""
        # Verifica se abbiamo già una posizione per evitare raddoppi indesiderati
        if self.active_positions.get(symbol) is not None:
             # Se il segnale è opposto alla posizione attuale e la convinzione è alta, potremmo chiudere e girarci
             # Per ora, semplicemente evitiamo sovrapposizioni
             return

        should_execute = await self.signal_manager.add_signal(symbol, type, side, weight_modifier)
        
        if should_execute:
            current_price = self.latest_data.get(symbol, {}).get('price', 0)
            if current_price > 0:
                self.active_positions[symbol] = 'LONG' if side.lower() in ['buy', 'long'] else 'SHORT'
                asyncio.create_task(self.execute_order(symbol, side, current_price, is_black_swan=is_black_swan))
                # Notifica il consenso raggiunto
                self.notifier.notify_alert("CONSENSUS", f"Segnale Confermato: {type}", f"{symbol} {side.upper()}")

    async def execute_order(self, symbol, side, current_price, is_black_swan=False):
        # --- GLOBAL RISK CHECKS ---
        if symbol in self.symbol_blacklist:
            logger.warning(f"🚫 Skipping {symbol}: In blacklist (requires manual agreement).")
            return
            
        if self.circuit_breaker_active:
            logger.error("🛑 CIRCUIT BREAKER ACTIVE: Skipping all new entries due to high daily loss.")
            return
        current_positions_count = len([p for p in self.latest_account_data.get('positions', []) if float(p.get('positionAmt', 0)) != 0])
        if current_positions_count >= self.max_concurrent_positions and not is_black_swan:
            logger.warning(f"⚠️ Max positions ({self.max_concurrent_positions}) reached. Skipping entry for {symbol}.")
            return

        try:
            active_leverage = 50 if is_black_swan else self.leverage
            if is_black_swan:
                try:
                    await self.exchange.set_leverage(active_leverage, self.exchange.market(symbol)['id'])
                    logger.warning(f"🦢 BLACK SWAN LEVERAGE: Increased to {active_leverage}x for {symbol}")
                except Exception as e:
                    logger.error(f"Failed to set aggressive leverage for {symbol}: {e}")
                    active_leverage = self.leverage
            
            amount_to_buy = self._calculate_order_amount(symbol, current_price, custom_leverage=active_leverage)
            logger.info(f"Executing {side} order for {amount_to_buy} {symbol} @ {current_price} USDT (Leverage {active_leverage}x)")
            
            # 1. Place Main Market Order
            order = await self.exchange.create_market_order(symbol, side, amount_to_buy)
            logger.info(f"Main order success: {order['id']}")
            
            # 2. Calculate Risk Levels based on ATR (Institutional Standard)
            current_atr = self.latest_data[symbol].get('atr', current_price * 0.01)
            if current_atr == "N/A" or not current_atr:
                current_atr = current_price * 0.01
                
            # Dynamic SL is usually 2x ATR
            sl_distance = current_atr * 2.0
            # Dynamic TP1 is usually 1.5x ATR, TP2 is 3x ATR
            tp1_distance = current_atr * 1.5
            
            if side.lower() == 'buy':
                sl_price = current_price - sl_distance
                tp_price = current_price + (tp1_distance * 2.0) # Aim for TP2 initially
                close_side = 'sell'
            else:
                sl_price = current_price + sl_distance
                tp_price = current_price - (tp1_distance * 2.0)
                close_side = 'buy'
                
            sl_price = float(self.exchange.price_to_precision(symbol, sl_price))
            tp_price = float(self.exchange.price_to_precision(symbol, tp_price))
            
            logger.info(f"📊 ATR Risk Levels for {symbol}: SL @ {sl_price} (2xATR), TP @ {tp_price} (3xATR)")
            
            self.trade_levels[symbol] = {
                'side': side.lower(),
                'entry_price': current_price,
                'sl': sl_price,
                'tp': tp_price,
                'amount': amount_to_buy,
                'tp1_hit': False,
                'highest_price': current_price if side.lower() == 'buy' else 0,
                'lowest_price': current_price if side.lower() != 'buy' else 0
            }
            # Salva stato nel DB
            self.db.save_state("trade_levels", self.trade_levels)
            # Log nel DB storico
            self.db.log_trade(symbol, side, current_price, amount_to_buy, 0, "INIT")
            # Notifica Telegram
            self.notifier.notify_trade(symbol, side, current_price, amount_to_buy, "SIGNAL")
        except Exception as e:
            error_msg = str(e)
            if "-4411" in error_msg or "agreement" in error_msg.lower():
                logger.error(f"❌ Blacklisting {symbol}: Agreement not signed.")
                self.symbol_blacklist.add(symbol)
            
            logger.error(f"Order sequence failed for {symbol}: {e}")

    def add_alert(self, type, title, value=None):
        alert = {
            "timestamp": int(time.time() * 1000),
            "type": type,
            "title": title,
            "value": value
        }
        self.alert_history.insert(0, alert)
        self.alert_history = self.alert_history[:self.max_alerts]
        
        # Notifica Telegram per avvisi importanti
        self.notifier.notify_alert(type, title, str(value))

    async def update_loop(self):
        logger.info("Starting bot market data update loop...")
        
        config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), self.config_file)
        last_mtime = os.path.getmtime(config_path) if os.path.exists(config_path) else 0
        
        while True:
            # 0. Hot-Reload Config if RL Tuner modified it
            try:
                if os.path.exists(config_path):
                    current_mtime = os.path.getmtime(config_path)
                    if current_mtime > last_mtime:
                        logger.warning("🔄 CONFIG CHANGE DETECTED: Hot-reloading Risk Profile from JSON...")
                        self.risk_profile = self._load_risk_profile()
                        
                        params = self.risk_profile.get("trading_parameters", {})
                        if isinstance(params, dict):
                            new_symbols: list[str] = params.get("symbols", self.symbols)
                            self.timeframe = params.get("timeframe", self.timeframe)
                            self.rsi_period = params.get("rsi_period", self.rsi_period)
                            self.leverage = params.get("leverage", self.leverage)
                            self.usdt_per_trade = params.get("usdt_per_trade", self.usdt_per_trade)
                            self.stop_loss_pct = params.get("stop_loss_pct", self.stop_loss_pct)
                            self.take_profit_pct = params.get("take_profit_pct", self.take_profit_pct)
                            self.max_concurrent_positions = params.get("max_concurrent_positions", self.max_concurrent_positions)
                            
                            # Filter new symbols too
                            validated_symbols = []
                            for symbol in new_symbols:
                                if symbol in self.exchange.markets:
                                    validated_symbols.append(symbol)
                                    
                                    if symbol not in self.latest_data:
                                        self.latest_data[symbol] = {
                                            'price': 0,
                                            'change': 0,
                                            'changePercent': 0,
                                            'rsi': 0,
                                            'imbalance': 1.0
                                        }
                                    if symbol not in self.active_positions:
                                        self.active_positions[symbol] = None
                                    if symbol not in self.trade_levels:
                                        self.trade_levels[symbol] = None
                                    if symbol not in self.ml_cooldowns:
                                        self.ml_cooldowns[symbol] = 0.0
                                else:
                                    logger.warning(f"⚠️ Hot-reload: Symbol {symbol} not available on this exchange. Skipping.")
                                    
                            self.symbols = validated_symbols
                            
                        last_mtime = current_mtime
                        await self._set_leverage_for_all()
            except Exception as e:
                logger.error(f"Error hot-reloading config: {e}")
                
            # Esecuzione a "scaglioni" e con semaforo per non farsi bannare l'IP e non saturare la CPU
            for i in range(0, len(self.symbols), 4):
                chunk = self.symbols[i:i+4]
                async with self.api_semaphore:
                    tasks = [self.fetch_data_and_analyze(symbol) for symbol in chunk]
                    await asyncio.gather(*tasks)
                await asyncio.sleep(2) # Increased sleep between chunks to be gentler
            
            # Sleep before fetching again
            await asyncio.sleep(10)

    async def account_update_loop(self):
        logger.info("Starting bot account update loop...")
        while True:
            try:
                # 1. Fetch positions FIRST
                try:
                    positions = await self.exchange.fetch_positions()
                    active_pos = [p for p in positions if float(p.get('contracts', 0) or 0) != 0]
                except Exception as pe:
                    logger.error(f"Error fetching positions: {pe}")
                    active_pos = []

                # 2. Fetch Balance and check Margin
                balance_data = await self.exchange.fetch_balance()
                
                # --- RECENT CHANGE: Use unified CCXT balance structure ---
                wallet_balance = float(balance_data.get('total', {}).get('USDT', 0))
                if wallet_balance == 0 and 'info' in balance_data:
                    # Fallback to Binance specific if unified fails
                    wallet_balance = float(balance_data['info'].get('totalWalletBalance', 0))

                margin_balance = float(balance_data.get('total', {}).get('USDT', 0)) # Approximate for futures
                if 'totalMarginBalance' in balance_data.get('info', {}):
                    margin_balance = float(balance_data['info']['totalMarginBalance'])

                equity = margin_balance
                
                # Use unrealized PnL from the unified balance if available
                unrealized_pnl = 0
                if 'info' in balance_data and 'positions' in balance_data['info']:
                    positions_info = balance_data['info']['positions']
                    unrealized_pnl = sum(float(p.get('unrealizedProfit', 0)) for p in positions_info)
                
                if self.initial_wallet_balance is None:
                    self.initial_wallet_balance = wallet_balance
                    logger.info(f"🏦 Initial Wallet Balance set: {self.initial_wallet_balance} USDT")
                
                pnl_pct = (wallet_balance - self.initial_wallet_balance) / self.initial_wallet_balance if self.initial_wallet_balance > 0 else 0
                
                if pnl_pct <= -0.05: # 5% Daily Loss
                    if not self.circuit_breaker_active:
                        logger.critical(f"🛑 CRITICAL: Daily loss limit reached ({pnl_pct:.2%}). Activating Circuit Breaker.")
                        self.circuit_breaker_active = True
                elif pnl_pct > -0.03: # Reset if we recover or restart with better balance
                    self.circuit_breaker_active = False

                logger.info(f"Account Update: Wallet={wallet_balance:.4f}, Equity={equity:.4f}, PnL={unrealized_pnl:.4f} ({pnl_pct:.2%})")
                
                # --- EMERGENCY MARGIN GUARD ---
                margin_ratio = 0.0
                if 'info' in balance_data:
                    info = balance_data['info']
                    margin_ratio = float(info.get('marginRatio', 0)) or (float(info.get('totalMaintMargin', 0)) / margin_balance if margin_balance > 0 else 0)
                
                if margin_ratio > 0.8: # 80% Margin Usage is DANGEROUS
                    logger.critical(f"⚠️ DANGER: Margin Ratio at {margin_ratio*100:.2f}%! Executing Emergency Cleanup.")
                    worst_symbol = None
                    worst_pnl = 0
                    if active_pos:
                        # Create map for emergency lookup
                        active_symbols_map = {p['symbol']: p for p in active_pos}
                        for p in active_pos:
                            pnl = float(p.get('unrealizedPnl', 0))
                            if pnl < worst_pnl:
                                worst_pnl = pnl
                                worst_symbol = p['symbol']
                        
                        if worst_symbol:
                            logger.warning(f"🛡️ EMERGENCY CLEANUP: Closing {worst_symbol} (PnL: {worst_pnl}) to save margin.")
                            # Fallback if trade levels are missing
                            trade_to_close = self.trade_levels.get(worst_symbol)
                            if not trade_to_close:
                                pos = active_symbols_map[worst_symbol]
                                trade_to_close = {
                                    'side': pos['side'],
                                    'amount': pos['contracts']
                                }
                            asyncio.create_task(self.close_position(worst_symbol, trade_to_close))
                
                self.latest_account_data['balance'] = wallet_balance
                self.latest_account_data['equity'] = margin_balance
                self.latest_account_data['unrealized_pnl'] = unrealized_pnl
                self.latest_account_data['alerts'] = self.alert_history
                self.latest_account_data['positions'] = active_pos
                
                # --- AUTO-SYNC: Update bot state to match REALITY on Binance ---
                if active_pos:
                    active_symbols_map = {p['symbol']: p for p in active_pos}
                    for symbol in self.symbols:
                        if symbol in active_symbols_map:
                            pos = active_symbols_map[symbol]
                            side = pos.get('side', '').lower()
                            self.active_positions[symbol] = side.upper()
                            
                            if self.trade_levels.get(symbol) is None:
                                entry_price = float(pos.get('entryPrice', 0))
                                amount = float(pos.get('contracts', 0))
                                if entry_price > 0:
                                    logger.info(f"🔄 Re-syncing trade levels for {symbol} from existing position @ {entry_price}")
                                    current_atr = self.latest_data[symbol].get('atr', entry_price * 0.01) if symbol in self.latest_data else entry_price * 0.01
                                    if current_atr == "N/A" or not current_atr:
                                        current_atr = entry_price * 0.01

                                    # --- TIGHTER RISK MANAGEMENT ---
                                    # Reduce ATR multiplier from 2.0 to 1.5 to cut losses faster
                                    sl_distance = current_atr * 1.5
                                    tp_distance = current_atr * 3.5 # Slightly wider TP for better R:R
                                    
                                    if side == 'long':
                                        sl = entry_price - sl_distance
                                        tp = entry_price + tp_distance
                                    else:
                                        sl = entry_price + sl_distance
                                        tp = entry_price - tp_distance
                                    
                                    self.trade_levels[symbol] = {
                                        'side': side,
                                        'entry_price': entry_price,
                                        'sl': float(self.exchange.price_to_precision(symbol, sl)),
                                        'tp': float(self.exchange.price_to_precision(symbol, tp)),
                                        'amount': amount,
                                        'tp1_hit': False,
                                        'highest_price': entry_price if side == 'long' else 0,
                                        'lowest_price': entry_price if side != 'long' else 0
                                    }
                        else:
                            self.active_positions[symbol] = None
                            self.trade_levels[symbol] = None
                else:
                    for symbol in self.symbols:
                        self.active_positions[symbol] = None
                        self.trade_levels[symbol] = None
                
                # 3. Fetch recent trades (Slow part, do it in chunks)
                # We only fetch for active positions or major symbols to be fast and resilient
                all_trades = []
                symbols_to_check = list(set([p['symbol'] for p in self.latest_account_data['positions']] + self.symbols[:20]))
                
                for i in range(0, len(symbols_to_check), 5):
                    chunk = symbols_to_check[i:i+5]
                    tasks = []
                    for symbol in chunk:
                        tasks.append(self.exchange.fetch_my_trades(symbol, limit=5))
                    
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    for res in results:
                        if isinstance(res, list):
                            all_trades.extend(res)
                    await asyncio.sleep(0.5)
                
                # Sort trades by timestamp descending and keep top 20
                all_trades.sort(key=lambda x: x.get('timestamp', 0) or 0, reverse=True)
                self.latest_account_data['trades'] = all_trades[:20]
                logger.info(f"Account Update: Found {len(all_trades)} recent trades.")
                
            except Exception as e:
                logger.error(f"Error in account update loop: {e}")
                import traceback
                logger.error(traceback.format_exc())
                
            await asyncio.sleep(10) # Update account data every 10 seconds

    async def funding_rate_loop(self):
        logger.info("Starting Funding Rate Arbitrage loop...")
        while True:
            try:
                # Fetch all funding rates for all markets currently active in bot
                rates = await self.exchange.fetch_funding_rates(self.symbols)
                
                for symbol, data in rates.items():
                    # High funding > 0.1% means longs pay shorts. Protect by shorting to collect fee.
                    rate_raw = data.get('fundingRate', 0)
                    if rate_raw is not None:
                        rate_pct = float(rate_raw) * 100
                        
                        if rate_pct > 0.1 and self.active_positions[symbol] != 'SHORT':
                            logger.warning(f"🏦 FUNDING ARB: Extreme positive funding ({rate_pct:.3f}%) on {symbol}. Executing SHORT to collect fees.")
                            current_price = self.latest_data[symbol]['price']
                            if current_price > 0:
                                self.active_positions[symbol] = 'SHORT'
                                asyncio.create_task(self.execute_order(symbol, 'sell', current_price))
                        
                        elif rate_pct < -0.1 and self.active_positions[symbol] != 'LONG':
                            logger.warning(f"🏦 FUNDING ARB: Extreme negative funding ({rate_pct:.3f}%) on {symbol}. Executing LONG to collect fees.")
                            current_price = self.latest_data[symbol]['price']
                            if current_price > 0:
                                self.active_positions[symbol] = 'LONG'
                                asyncio.create_task(self.execute_order(symbol, 'buy', current_price))
                            
            except Exception as e:
                if "does not support fetchFundingRates" not in str(e):
                    logger.error(f"Error checking funding rates: {e}")
            await asyncio.sleep(300) # Check every 5 minutes

    def get_dashboard_data(self):
        return self.latest_data
        
    def get_account_data(self):
        return self.latest_account_data
