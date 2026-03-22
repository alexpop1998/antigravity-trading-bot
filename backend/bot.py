import ccxt.async_support as ccxt
import pandas as pd
import asyncio
import traceback
import logging
from dotenv import load_dotenv
import os
import json
import time
from datetime import datetime
from collections import deque
from ml_predictor import MLPredictor
from database import BotDatabase
from telegram_notifier import TelegramNotifier
from signal_manager import SignalManager
from typing import Any, Dict, List, Optional

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
        self.usdt_per_trade = params.get("usdt_per_trade", 100)
        self.percent_per_trade = params.get("percent_per_trade", 2.5) # % of equity per trade
        self.stop_loss_pct = params.get("stop_loss_pct", 0.02)
        self.take_profit_pct = params.get("take_profit_pct", 0.06)
        self.max_concurrent_positions = params.get("max_concurrent_positions", 20)
        self.max_global_margin_ratio = params.get("max_global_margin_ratio", 0.75)
        self.current_margin_ratio = 0.0
        
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
        self.latest_data: Dict[str, Dict[str, Any]] = {symbol: {
            'price': 0.0, 'change': 0.0, 'changePercent': 0.0, 'rsi': 0.0, 
            'macd': 0.0, 'macd_hist': 0.0, 'bb_upper': 0.0, 'bb_lower': 0.0,
            'imbalance': 1.0, 'atr': 0.0, 'ema200': 0.0, 'volume24h': 0.0,
            'prediction': "Syncing..."
        } for symbol in self.symbols}
            
        self.latest_account_data: Dict[str, Any] = {
            'positions': [],
            'trades': [],
            'balance': 0.0,
            'equity': 0.0,
            'unrealized_pnl': 0.0,
            'alerts': []
        }
        
        # Track locally how many positions are being opened (before account update cycle confirms)
        self.pending_orders_count = 0
        self.order_lock = asyncio.Lock()
        
        # Keep track of active positions (LONG/SHORT/None)
        self.active_positions: Dict[str, Optional[str]] = {symbol: None for symbol in self.symbols}
        # Keep track of soft stop-loss levels and trade info
        self.trade_levels: Dict[str, Optional[Dict[str, Any]]] = {symbol: None for symbol in self.symbols}
        self.is_macro_paused = False
        
        # Initialize Machine Learning Prophet
        self.predictor = MLPredictor()
        self.ml_cooldowns: Dict[str, float] = {symbol: 0.0 for symbol in self.symbols}
        
        # Institutional Alert History for Frontend
        self.alert_history = []
        self.max_alerts = 50
        
        # VPS Modules
        self.db = BotDatabase()
        self.notifier = TelegramNotifier()
        self.signal_manager = SignalManager(self)
        
        # Resource management
        self.ml_semaphore = asyncio.Semaphore(1) 
        self.api_semaphore = asyncio.Semaphore(5) 
        
        # Risk Management & Guards
        self.circuit_breaker_active = False
        self.symbol_blacklist = set()
        self.initial_wallet_balance: Optional[float] = None
        self.previous_prices: Dict[str, float] = {symbol: 0.0 for symbol in self.symbols}
        # 5-minute rolling window (10 updates of 30s)
        self.price_windows: Dict[str, deque] = {symbol: deque(maxlen=10) for symbol in self.symbols}
        self.panic_drawdown_threshold = 0.10 # 10% Total Equity Drawdown
        self.min_leverage = 2
        self.max_leverage = 25
        self.sector_caps = {"MEME": 3, "L1": 3, "ALTS": 4} # Limit per sector
        self.sector_mapping = {
            "BTC/USDT": "MAJOR", "ETH/USDT": "MAJOR", "SOL/USDT": "L1", 
            "BNB/USDT": "L1", "SXP/USDT": "ALTS", "RIVER/USDT": "ALTS", 
            "APR/USDT": "ALTS", "WAXP/USDT": "ALTS", "DEGO/USDT": "ALTS", 
            "PHA/USDT": "ALTS", "PIPPIN/USDT": "MEME", "BAN/USDT": "MEME"
        }
        
        # Recupero stato persistente
        saved_levels = self.db.load_state("trade_levels")
        if saved_levels:
            self.trade_levels = saved_levels
            logger.info("📦 Stato trade_levels recuperato correttamente.")
        
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
            
            # --- EMA 200 Calculation (Trend Filter) ---
            df['ema200'] = df['close'].ewm(span=200, adjust=False).mean()
            current_ema200 = df['ema200'].iloc[-1]

            # --- ADX (Average Directional Index) Calculation ---
            plus_dm = df['high'].diff()
            minus_dm = df['low'].diff().multiply(-1)
            plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0)
            minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0)
            
            # Smoothing TR, +DM, -DM (Wilder's Smoothing)
            period = 14
            smoothed_tr = df['tr'].rolling(window=period).mean() # Simple mean for now
            smoothed_plus_dm = plus_dm.rolling(window=period).mean()
            smoothed_minus_dm = minus_dm.rolling(window=period).mean()
            
            plus_di = 100 * (smoothed_plus_dm / smoothed_tr)
            minus_di = 100 * (smoothed_minus_dm / smoothed_tr)
            dx = 100 * (abs(plus_di - minus_di) / (plus_di + minus_di))
            df['adx'] = dx.rolling(window=period).mean()
            current_adx = df['adx'].iloc[-1]
            
            # --- Order Book Imbalance Calculation ---
            orderbook = await self.exchange.fetch_order_book(symbol, limit=100)
            bids = sum(float(bid[1]) for bid in orderbook['bids'])
            asks = sum(float(ask[1]) for ask in orderbook['asks'])
            imbalance = bids / asks if asks > 0 else 1.0
            
            # --- FLASH & RECOVERY DETECTION (5m Rolling Window) ---
            prev_price = self.previous_prices.get(symbol, current_close)
            window = self.price_windows[symbol]
            window.append(current_close)
            
            # Compare current price to the start of the 5-minute window
            if len(window) >= 2:
                reference_price = window[0]
                change_5m_pct = (current_close - reference_price) / reference_price
                
                # --- VOL CONFIRMATION CHECK ---
                current_candle_vol = float(df['volume'].iloc[-1])
                avg_recent_vol = float(df['volume'].rolling(window=10).mean().iloc[-2]) if len(df) > 10 else 1.0
                is_high_volume = current_candle_vol > (avg_recent_vol * 1.5)
                
                # 1. Detect Flash Event (Dump or Pump) - 1.2% threshold for 5 mins
                is_event_dump = change_5m_pct < -0.012
                is_event_pump = change_5m_pct > 0.012
                
                if is_event_dump:
                    logger.info(f"⚠️ [FLASH] Potential DUMP detected for {symbol} ({change_5m_pct:.2%} in 5m)")
                    asyncio.create_task(self.handle_signal(symbol, "EVENT_DUMP", "sell", weight_modifier=1.0))
                if is_event_pump:
                    logger.info(f"⚠️ [FLASH] Potential PUMP detected for {symbol} ({change_5m_pct:.2%} in 5m)")
                    asyncio.create_task(self.handle_signal(symbol, "EVENT_PUMP", "buy", weight_modifier=1.0))
                    
                # 2. Detect V-Shape Recovery (Absorption)
                if is_event_dump and current_rsi < 28: # Slightly relaxed RSI for recovery
                    # Price is starting to tick up from the bottom of the move with Volume Confirmation
                    if current_close > prev_price and is_high_volume:
                        logger.warning(f"🛡️ [RE-ABSORPTION] RECOVERY LONG for {symbol} after {change_5m_pct:.2%} drop!")
                        asyncio.create_task(self.handle_signal(symbol, "RECOVERY", "buy", weight_modifier=1.0, is_black_swan=True, current_price=current_close, ema200=current_ema200))
                
                elif is_event_pump and current_rsi > 72:
                    # Price is starting to tick down from the top of the pump with Volume Confirmation
                    if current_close < prev_price and is_high_volume:
                        logger.warning(f"🛡️ [REJECTION] REJECTION SHORT for {symbol} after {change_5m_pct:.2%} pump!")
                        asyncio.create_task(self.handle_signal(symbol, "REJECTION", "sell", weight_modifier=1.0, is_black_swan=True, current_price=current_close, ema200=current_ema200))

            # Store current price for next cycle velocity tracking
            self.previous_prices[symbol] = current_close

            ticker = await self.exchange.fetch_ticker(symbol)
            # 24h Volume Guard (USD or Quote Currency)
            quote_volume = float(ticker.get('quoteVolume', 0))
            
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
                'ema200': round(current_ema200, 4) if not pd.isna(current_ema200) else "N/A",
                'volume24h': quote_volume,
                'prediction': predicted_price if predicted_price else "Syncing ML..."
            }
            
            self._check_soft_stop_loss(symbol, current_close)
            self._check_trading_signals(symbol, current_close, current_rsi, current_macd_hist, current_bb_lower, current_bb_upper, imbalance, current_ema200, quote_volume, current_atr, current_adx)
            
        except Exception as e:
            logger.error(f"Error analyzing {symbol}: {e}")

    def _check_trading_signals(self, symbol, price, rsi, macd_hist, bb_lower, bb_upper, imbalance, ema200, volume24h, atr, adx):
        if pd.isna(rsi) or pd.isna(macd_hist) or pd.isna(bb_lower) or pd.isna(ema200):
            return
            
        if self.is_macro_paused:
            return

        # --- TREND GUARD: BTC Correlation Check ---
        # If BTC is dumping, don't open LONGs on alts
        if "BTC/USDT" in self.latest_data and symbol != "BTC/USDT":
            btc_data = self.latest_data["BTC/USDT"]
            btc_change = float(btc_data.get('changePercent', 0))
            if btc_change < -1.5: # Market-wide dump protection
                return

        # --- SECTOR CAP CHECK ---
        sector = self.sector_mapping.get(symbol, "ALTS")
        current_sector_count = sum(1 for s, side in self.active_positions.items() if side is not None and self.sector_mapping.get(s) == sector)
        if current_sector_count >= self.sector_caps.get(sector, 3):
            return

        # STRENGTHENED LIMIT CHECK (before task creation)
        current_open_count = len(self.latest_account_data.get('positions', []))
        if current_open_count + self.pending_orders_count >= self.max_concurrent_positions:
            return

        # --- LOGIC GUARD 1: Volume ---
        # Require minimum $5M 24h volume for institutional stability (avoid shitcoin pumps)
        if volume24h < 5000000:
            return

        # --- LOGIC GUARD 2: Trend Filter (200 EMA) ---
        price_above_ema = price > ema200
        
        # 2. Classic Technical Strategy (RSI + MACD + Bollinger)
        is_technical_signal = False
        tech_side = None
        
        # --- ADX TREND FILTER ---
        # Don't take trend-following signals if we're in a sideways range (ADX < 25)
        is_trending = adx > 25 if not pd.isna(adx) else True
        
        if rsi < 30 and macd_hist > 0 and price <= (bb_lower * 1.01):
            if price_above_ema and is_trending: # Trend-following LONG
                tech_side = 'buy'
                is_technical_signal = True
            
        elif rsi > 70 and macd_hist < 0 and price >= (bb_upper * 0.99):
            if not price_above_ema and is_trending: # Trend-following SHORT
                tech_side = 'sell'
                is_technical_signal = True

        # --- FUNDING RATE FILTER (DEEPSEEK REFACTOR) ---
        # Don't open LONGs if the funding rate is extreme (>0.05%)
        current_funding = self.latest_data.get(symbol, {}).get('funding_rate', 0)
        if tech_side == 'buy' and current_funding > 0.0005: # 0.05%
            logger.warning(f"🚫 [FUNDING GUARD] Skipping LONG on {symbol} due to high funding rate: {current_funding*100:.3f}%")
            is_technical_signal = False
            tech_side = None

        # --- CONSENSUS ENGINE: Routing signals to SignalManager ---
        if is_technical_signal:
             asyncio.create_task(self.handle_signal(symbol, "TECH", tech_side, weight_modifier=1.0, current_price=price, ema200=ema200))

        # --- LOGIC GUARD 3: Volatility (ATR-based skip) ---
        # Skip if ATR is less than 0.2% of price (stagnant) or more than 5% (extreme volatility)
        atr_pct = (atr / price) if price > 0 else 0
        if atr_pct < 0.002 or atr_pct > 0.05:
            return

        # 3. AI Predictive Alpha (Machine Learning Classifier)
        prediction = self.latest_data[symbol].get('prediction')
        if isinstance(prediction, dict) and 'direction' in prediction:
            direction = prediction['direction']
            confidence = prediction['confidence']
            # Confidence Threshold: 0.55
            if direction == 1 and confidence > 0.55 and price_above_ema:
                asyncio.create_task(self.handle_signal(symbol, "AI", "buy", weight_modifier=1.2, current_price=price, ema200=ema200))
            elif direction == 0 and confidence > 0.55 and not price_above_ema:
                asyncio.create_task(self.handle_signal(symbol, "AI", "sell", weight_modifier=1.2, current_price=price, ema200=ema200))

    async def handle_signal(self, symbol, type, side, weight_modifier=1.0, is_black_swan=False, current_price=None, ema200=None):
        """
        Receives signals from various sources (TECH, AI, NEWS, etc.) and forwards them 
        to the SignalManager for consensus aggregation.
        """
        if not self.signal_manager:
            return
            
        approved, consensus_score = await self.signal_manager.add_signal(symbol, type, side, weight_modifier, current_price=current_price, ema200=ema200)
        
        if approved:
            # --- MASTER CONTROLLER: NET EXPOSURE & PRIORITY ---
            existing_pos = self.active_positions.get(symbol)
            if existing_pos:
                # 1. Prevent conflicting positions (Long vs Short)
                existing_side = 'buy' if existing_pos == 'LONG' else 'sell'
                if existing_side != side.lower():
                    logger.warning(f"🚫 [MASTER CONTROLLER] Conflicting signal for {symbol}: Already in {existing_pos}. Skipping {side.upper()} {type}.")
                    return

            # 2. Pause Standard Signals during High-Impact Events (HIE)
            # Check if current trade is HIE and we are trying a standard signal
            current_trade = self.trade_levels.get(symbol)
            if current_trade:
                is_hie_running = current_trade.get('signal_type') in ["WHALE", "LIQUIDATION", "RECOVERY", "REJECTION"]
                if is_hie_running and type in ["TECH", "AI"]:
                    logger.info(f"⏳ [MASTER CONTROLLER] Standard {type} signal paused for {symbol} due to active HIE scalp.")
                    return

            # Check global margin ratio instead of fixed position count
            if self.current_margin_ratio < self.max_global_margin_ratio or is_black_swan:
                    self.active_positions[symbol] = 'LONG' if side.lower() in ['buy', 'long'] else 'SHORT'
                    self.pending_orders_count += 1
                    
                    await self.execute_order(symbol, side.lower(), self.latest_data[symbol]['price'], is_black_swan=is_black_swan, consensus_score=consensus_score, signal_type=type)

    def _check_soft_stop_loss(self, symbol, current_price):
        trade = self.trade_levels.get(symbol)
        if not trade:
            return
            
        # Normalize side (handle both 'buy'/'long' and 'sell'/'short')
        side = str(trade.get('side', 'buy')).lower()
        is_long = side in ['buy', 'long']
        sl = float(trade.get('sl', 0))
        tp_old = float(trade.get('tp', 0))
        tp1 = float(trade.get('tp1', tp_old))
        tp2 = float(trade.get('tp2', tp_old))
        
        # Use dynamic CURRENT ATR for trailing Stop-Loss distance
        # Black Swans need more breathing room (5x ATR vs 2x ATR)
        multiplier = 5.0 if trade.get('is_black_swan') else 2.0
        latest = self.latest_data.get(symbol, {})
        current_atr = latest.get('atr')
        
        if isinstance(current_atr, (float, int)) and current_atr > 0:
            sl_distance = float(current_atr) * multiplier
        else:
            sl_distance = float(trade.get('sl_distance', current_price * self.stop_loss_pct))
            
        # Floor for Black Swans (at least 1.2% away to avoid noise)
        if trade.get('is_black_swan'):
            min_dist = current_price * 0.012
            if sl_distance < min_dist:
                sl_distance = min_dist
        
        # --- RECOVERY EARLY EXIT: Mean Reversion Complete ---
        sig_type = trade.get('signal_type')
        current_rsi = self.latest_data.get(symbol, {}).get('rsi')
        
        if sig_type in ["RECOVERY", "REJECTION"] and isinstance(current_rsi, (int, float)):
            entry_price = float(trade.get('entry_price', current_price))
            pnl_pct = (current_price - entry_price) / entry_price if is_long else (entry_price - current_price) / entry_price
            
            # Logic: Require RSI to cross 55/45 (stronger neutral) AND have at least 0.5% profit to cover fees
            if (is_long and current_rsi > 55 and pnl_pct > 0.005) or (not is_long and current_rsi < 45 and pnl_pct > 0.005):
                logger.warning(f"🏁 [{sig_type}] Closing {symbol} - RSI {current_rsi} reached exit threshold with profit {pnl_pct:.2%}")
                asyncio.create_task(self.close_position(symbol, trade))
                return
        
        # --- Trailing Stop-Loss Logic ---
        # DEEPSEEK REFACTOR: Disable trailing stop for News (GATEKEEPER) for the first 15m
        if trade.get('trailing_delayed'):
            elapsed = time.time() - float(trade.get('opened_at', 0))
            if elapsed < 900: # 15 minutes
                logger.debug(f"⏳ [NEWS TRAILING DELAY] Waiting for volatility to settle for {symbol}...")
            else:
                trade['trailing_delayed'] = False
                logger.info(f"✅ [NEWS TRAILING ACTIVE] Normal trailing stop enabled for {symbol}.")
        
        should_close = False
        reason = ""
        entry_price = float(trade.get('entry_price', current_price))
        sl = float(trade.get('sl', 0))
        
        if is_long:
            # 1. Update Trailing High
            if current_price > trade.get('highest_price', 0):
                trade['highest_price'] = current_price
                # Update Trailing SL if not delayed
                if not trade.get('trailing_delayed'):
                    new_sl = current_price - sl_distance
                    if new_sl > sl:
                        trade['sl'] = new_sl
                        sl = new_sl
                        logger.info(f"📈 TRAILING STOP UPDATED for {symbol}: SL now at {new_sl:.6f}")
            
            # 2. Check Exit Conditions
            if current_price <= sl:
                should_close = True
                reason = "STOP-LOSS (TRAILING)" if trade.get('highest_price', 0) > entry_price else "STOP-LOSS"
            elif current_price >= tp2:
                should_close = True
                reason = "TAKE-PROFIT 2 (FINAL)"
            elif current_price >= tp1:
                # Partial Take Profit (TP1)
                if not trade.get('tp1_hit', False):
                    logger.warning(f"🎯 PARTIAL TP1 HIT for {symbol} at {current_price}!")
                    asyncio.create_task(self.close_position(symbol, trade, partial_pct=0.5))
                    trade['tp1_hit'] = True
                    # Move SL to fee-covering break-even (Entry + 0.15% profit)
                    offset = entry_price * 0.0015
                    trade['sl'] = max(entry_price + offset, trade.get('sl', 0))
                    
        else: # SHORT
            # 1. Update Trailing Low
            if current_price < trade.get('lowest_price', 999999):
                trade['lowest_price'] = current_price
                # Update Trailing SL if not delayed
                if not trade.get('trailing_delayed'):
                    new_sl = current_price + sl_distance
                    if sl == 0 or new_sl < sl: # sl == 0 should not happen but for safety
                        trade['sl'] = new_sl
                        sl = new_sl
                        logger.info(f"📉 TRAILING STOP UPDATED for {symbol}: SL now at {new_sl:.6f}")
            
            # 2. Check Exit Conditions
            if current_price >= sl:
                should_close = True
                reason = "STOP-LOSS (TRAILING)" if trade.get('lowest_price', 999999) < entry_price else "STOP-LOSS"
            elif current_price <= tp2:
                should_close = True
                reason = "TAKE-PROFIT 2 (FINAL)"
            elif current_price <= tp1:
                # Partial Take Profit (TP1)
                if not trade.get('tp1_hit', False):
                    logger.warning(f"🎯 PARTIAL TP1 HIT for {symbol} at {current_price}!")
                    asyncio.create_task(self.close_position(symbol, trade, partial_pct=0.5))
                    trade['tp1_hit'] = True
                    # Move SL to fee-covering break-even (Entry - 0.15% profit)
                    offset = entry_price * 0.0015
                    trade['sl'] = min(entry_price - offset, trade.get('sl', 999999))
                
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
            total_amount = float(trade.get('amount', 0))
            amount_to_close = total_amount * partial_pct
            
            # Precision adjustment using ccxt helper
            amount_str = self.exchange.amount_to_precision(symbol, amount_to_close)
            amount_to_close = float(amount_str)
            
            if amount_to_close <= 0:
                logger.warning(f"Skipping closure for {symbol}: amount too small.")
                return

            logger.info(f"🔄 Closing position for {symbol} ({'PARTIAL' if partial_pct < 1 else 'FULL'}): {close_side} {amount_to_close}")
            
            # --- IMPROVEMENT: Capture actual execution price from order response ---
            # Binance MARKET orders return fill information in the 'fills' property of the response
            # or 'average' price if unified by CCXT.
            order_response = await self.exchange.create_order(symbol, 'market', close_side, amount_to_close, None, {'reduceOnly': True})
            
            # Extract execution details
            order_id = order_response.get('id')
            execution_price = float(order_response.get('average') or order_response.get('price', 0))
            
            # Fallback for execution price if not in response
            if execution_price <= 0:
                execution_price = self.latest_data[symbol].get('price', 0) if symbol in self.latest_data else 0
                logger.warning(f"⚠️ Could not get execution price from order response for {symbol}. Using last known price: {execution_price}")
            
            if partial_pct >= 1.0:
                logger.info(f"Position closed successfully for {symbol} @ {execution_price}.")
                self.active_positions[symbol] = None
                # Mark for deletion in trade_levels to avoid re-sync race
                self.trade_levels[symbol] = None
            else:
                # Update remaining amount in state
                trade['amount'] = total_amount - amount_to_close
                logger.info(f"Partial closure successful for {symbol}. Remaining: {trade['amount']} @ {execution_price}")
            
            # Calculate PnL for logging using ACTUAL execution price
            entry_price = float(trade.get('entry_price', execution_price))
            pnl = (execution_price - entry_price) * amount_to_close if is_long else (entry_price - execution_price) * amount_to_close
            
            # Log with exchange_trade_id to prevent duplicates in sync_binance_trades
            reason = "EXIT" if partial_pct >= 1.0 else "PARTIAL_EXIT"
            self.db.log_trade(symbol, "CLOSE" if partial_pct >= 1.0 else "CLOSE_PARTIAL", execution_price, amount_to_close, pnl, reason, exchange_trade_id=order_id)
            
            self.notifier.notify_trade(symbol, "CLOSE", execution_price, amount_to_close, reason, pnl=pnl)
            
            # Save state immediately to persist closure
            self.db.save_state("trade_levels", self.trade_levels)
                
        except Exception as e:
            logger.error(f"Failed to close position for {symbol}: {e}")
            logger.error(traceback.format_exc())
    # Calculate dynamic order amount based on risk capital, equity, and confidence
    def _calculate_order_amount(self, symbol, current_price, custom_leverage=None, confidence_modifier=1.0):
        leverage = custom_leverage if custom_leverage else self.leverage
        
        # 1. Base Capital Calculation (Equity % vs Fixed USDT)
        current_equity = self.latest_account_data.get('equity', 0)
        if current_equity > 0:
            base_usdt = current_equity * (self.percent_per_trade / 100)
            logger.info(f"[{symbol}] Equity Sizing: {self.percent_per_trade}% of {current_equity:.2f} USDT = {base_usdt:.2f} USDT")
        else:
            base_usdt = self.usdt_per_trade
            logger.warning(f"[{symbol}] Equity not available, falling back to fixed {base_usdt} USDT sizing.")
            
        # 2. Volatility-based capital allocation (ATR modifier)
        current_atr = self.latest_data[symbol].get('atr', current_price * 0.01)
        if current_atr == "N/A" or not current_atr:
            current_atr = current_price * 0.01
            
        atr_pct = (current_atr / current_price) if current_price > 0 else 0.01
        baseline_atr_pct = 0.01  # Baseline expected volatility per candle
        
        volatility_modifier = baseline_atr_pct / atr_pct if atr_pct > 0 else 1.0
        volatility_modifier = max(0.2, min(2.0, volatility_modifier))
        
        # 3. Final Allocation with Confidence Scaling
        adjusted_usdt = base_usdt * volatility_modifier * confidence_modifier
        
        # Global Cap: Never risk more than 15% of equity per trade even with modifiers
        if current_equity > 0:
            max_limit = current_equity * 0.15
            if adjusted_usdt > max_limit:
                logger.warning(f"[{symbol}] Capping order size to 15% of equity: {adjusted_usdt:.2f} -> {max_limit:.2f}")
                adjusted_usdt = max_limit

        purchasing_power = adjusted_usdt * leverage
        
        logger.info(f"[{symbol}] Sizing Summary: Base={base_usdt:.2f}, VolMod={volatility_modifier:.2f}, ConfMod={confidence_modifier:.2f} -> Adjusted={adjusted_usdt:.2f} USDT")
        
        market = self.exchange.market(symbol)
        # Contract size calculation
        amount = purchasing_power / current_price
        
        # Adjust for exchange precision rules
        amount = self.exchange.amount_to_precision(symbol, amount)
        return float(amount)

    def calculate_dynamic_leverage(self, symbol, side, consensus_score, current_price):
        """Calculates leverage based on consensus score, volatility, and trend safety."""
        latest = self.latest_data.get(symbol, {})
        atr = latest.get('atr', current_price * 0.01)
        if not atr or atr == "N/A": atr = current_price * 0.01
        
        # 1. Base leverage 10x (Aggressive Default)
        base_lev = 10
        
        # 2. Consensus Scaling (Aggressive Scaling: +2 lev per 1.0 score above 5)
        score_bonus = (consensus_score - 5.0) * 2.0 
        
        # 3. Volatility Penalty
        vol_pct = (atr / current_price) * 100 if current_price > 0 else 0
        vol_penalty = vol_pct * 1.5 # 2% vol = -3 leverage
        
        # 4. Trend Safety Bonus (+2 if opening in trend direction)
        trend_bonus = 0
        ema200 = latest.get('ema200')
        if isinstance(ema200, (int, float)):
            is_long = side.lower() in ['buy', 'long']
            if (is_long and current_price > ema200) or (not is_long and current_price < ema200):
                trend_bonus = 2
                logger.info(f"🛡️ [Leverage] Trend Safety Bonus applied for {symbol} (+2)")
        
        # --- ATR-BASED VOLATILITY CAP (DEEPSEEK REFACTOR) ---
        # Riduciamo drasticamente la leva automatica se la volatilità è estrema
        vol_limit = 25 # Default max
        if vol_pct > 4.5:
             vol_limit = 5
             logger.warning(f"⚠️ [VOLATILITY LIMIT] Extreme Volatility ({vol_pct:.2f}%) on {symbol}. Capping leverage to 5x.")
        elif vol_pct > 3.0:
             vol_limit = 10
        elif vol_pct > 1.5:
             vol_limit = 20
        
        lev = base_lev + score_bonus - vol_penalty + trend_bonus
        lev = min(lev, vol_limit)
        
        # 5. Sector Dynamic Caps
        sector = self.sector_mapping.get(symbol, "ALTS")
        # Base Caps: MAJOR=25, ALTS=20
        max_cap = 25 if sector == "MAJOR" else 20
        
        # High Conviction "Ultra" Bonus
        if consensus_score >= 8.0:
            max_cap += 5 # Push to 25x for Major, 20x for Alts
            logger.warning(f"🔥 [Leverage] High Conviction Bonus applied for {symbol} (Max Cap: {max_cap})")
        
        final_lev = max(self.min_leverage, min(max_cap, round(lev)))
        logger.info(f"📐 [Leverage] {symbol} Calculation: Base={base_lev}, ScoreBonus={score_bonus:.1f}, VolPenalty={vol_penalty:.1f}, TrendBonus={trend_bonus} -> Final={final_lev}x")
        
        return final_lev

    async def execute_order(self, symbol, side, current_price, is_black_swan=False, consensus_score=5.0, signal_type="TECH"):
        # --- GLOBAL RISK CHECKS ---
        if symbol in self.symbol_blacklist:
            logger.warning(f"🚫 Skipping {symbol}: In blacklist.")
            return
            
        if self.circuit_breaker_active:
            logger.error("🛑 CIRCUIT BREAKER ACTIVE: Skipping entries.")
            return

        try:
            # FIX #1: Acquire lock to serialize order execution and prevent position count race condition
            async with self.order_lock:
                # Re-check limit inside the lock with latest confirmed info
                confirmed_positions = [p for p in self.latest_account_data.get('positions', []) if float(p.get('contracts', 0) or 0) != 0]
                
                # --- NEW SAFETY GUARD: Total Symbol Exposure Cap ---
                current_equity = self.latest_account_data.get('equity', 0)
                if current_equity > 0:
                    matching_position = next((p for p in confirmed_positions if p.get('symbol') == symbol or p.get('symbol') == symbol.replace('/', '').split(':')[0]), None)
                    if matching_position:
                        # Estimate total value in USD
                        pos_value = abs(float(matching_position.get('notional', 0)))
                        # If already more than 25% of equity, don't add more
                        if pos_value > (current_equity * 0.25):
                            logger.warning(f"🚫 [SAFETY] Symbol {symbol} already at max allowed exposure ({pos_value:.2f} > 25% of {current_equity:.2f}). Skipping.")
                            self.active_positions[symbol] = None
                            self.pending_orders_count = max(0, self.pending_orders_count - 1)
                            return

                # Switch to margin ratio guard
                if self.current_margin_ratio >= self.max_global_margin_ratio and not is_black_swan:
                    logger.warning(f"🚫 [ORDER] Global Margin Limit Reached ({self.current_margin_ratio*100:.1f}% >= {self.max_global_margin_ratio*100:.1f}%). Skipping {symbol}.")
                    self.active_positions[symbol] = None
                    self.pending_orders_count = max(0, self.pending_orders_count - 1)
                    return

                # Dynamic leverage calculation
                if signal_type == "GATEKEEPER":
                    active_leverage = 5 # News trades use low leverage
                    logger.info(f"🛡️ [NEWS LEVERAGE] Using safe 5x for {symbol} News Trade.")
                elif is_black_swan:
                    base_leverage = self.calculate_dynamic_leverage(symbol, side, consensus_score, current_price)
                    active_leverage = min(20, base_leverage) # Hard cap at 20x for High Impact Events
                else:
                    active_leverage = self.calculate_dynamic_leverage(symbol, side, consensus_score, current_price)
                
                try:
                    market = self.exchange.market(symbol)
                    await self.exchange.set_leverage(active_leverage, market['id'])
                except Exception as e:
                    logger.error(f"Failed to set leverage for {symbol}: {e}")
                
                amount_to_buy = self._calculate_order_amount(symbol, current_price, custom_leverage=active_leverage)
                
                # --- SPREAD PROTECTOR ---
                ticker = await self.exchange.fetch_ticker(symbol)
                ask_val = ticker.get('ask')
                bid_val = ticker.get('bid')
                ask = float(ask_val) if ask_val is not None else 0.0
                bid = float(bid_val) if bid_val is not None else 0.0
                
                if bid > 0 and ask > 0:
                    spread_pct = (ask - bid) / bid
                    if spread_pct > 0.003: # 0.3%
                        logger.warning(f"🚫 [SPREAD PROTECTOR] Spread for {symbol} is too high ({spread_pct * 100:.2f}%). Skipping trade.")
                        self.active_positions[symbol] = None
                        self.pending_orders_count = max(0, self.pending_orders_count - 1)
                        return
                        
                logger.warning(f"🚀 [ORDER] {side.upper()} {amount_to_buy} {symbol} @ {current_price} (Consensus Order)")
                
                # 1. Place Main Market Order
                order = await self.exchange.create_market_order(symbol, side.lower(), amount_to_buy)
                logger.info(f"✅ Main order success for {symbol}: {order['id']}")
                
                # --- NEW: Enhanced Alert for Gatekeeper (Black Swan) ---
                if signal_type == "GATEKEEPER":
                    msg = f"☢️ *URGENT: GATEKEEPER ALERT*\n\n" \
                          f"Bot has EXECUTED an emergency `{side.upper()}` on `{symbol}` @ `{current_price}`.\n\n" \
                          f"⚠️ This is a fundamental 'Black Swan' event. Manual monitoring/intervention is highly recommended."
                    asyncio.create_task(self.notifier.send_message(msg))
            
            # --- Continue with Risk Level calculation OUTSIDE the order lock ---
            # 2. Risk Levels based on ATR
            current_atr = self.latest_data[symbol].get('atr')
            if not current_atr or current_atr == "N/A":
                current_atr = current_price * 0.01 # Fallback 1%
                
            # Optimized R:R (SL 1.5x, TP1 2.0x, TP2 4.0x)
            # --- WHALE SCALPING LOGIC ---
            if signal_type == "WHALE":
                logger.warning(f"🐋 WHALE SCALPING MODE ACTIVE for {symbol}: Using tight Take Profit.")
                sl_mult = 1.5
                tp1_mult = 0.5 # 0.5% profit target (approx)
                tp2_mult = 1.0 # 1.0% profit target (approx)
                
                sl_distance  = current_price * 0.015 # 1.5% SL
                tp1_distance = current_price * 0.008 # 0.8% TP1 (Increased from 0.5% to ensure profitability)
                tp2_distance = current_price * 0.016 # 1.6% TP2 (Increased from 1.0%)
            elif signal_type in ["RECOVERY", "REJECTION"]:
                logger.warning(f"🛡️ RECOVERY SCALPING ACTIVE for {symbol}: 1.2% Partial TP Target.")
                sl_distance = current_price * 0.015 # 1.5% SL
                tp1_distance = current_price * 0.012 # 1.2% TP1 (Take profit on bounce)
                tp2_distance = current_price * 0.030 # 3.0% TP2
            else:
                # Black Swans get wider stops (5x ATR) to avoid noise
                sl_mult = 5.0 if is_black_swan else 2.2
                tp1_mult = 6.0 if is_black_swan else 3.0
                tp2_mult = 12.0 if is_black_swan else 6.0
                
                sl_distance  = current_atr * sl_mult
                # Floor for Black Swans (1.2% as per user safety requirement)
                if is_black_swan:
                    min_sl = current_price * 0.012
                    sl_distance = max(sl_distance, min_sl)
                    
                tp1_distance = current_atr * tp1_mult
                tp2_distance = current_atr * tp2_mult
            
            is_long = side.lower() in ['buy', 'long']
            if is_long:
                sl_price  = current_price - sl_distance
                tp1_price = current_price + tp1_distance
                tp2_price = current_price + tp2_distance
            else:
                sl_price  = current_price + sl_distance
                tp1_price = current_price - tp1_distance
                tp2_price = current_price - tp2_distance
                
            sl_price  = float(self.exchange.price_to_precision(symbol, sl_price))
            tp1_price = float(self.exchange.price_to_precision(symbol, tp1_price))
            tp2_price = float(self.exchange.price_to_precision(symbol, tp2_price))
            
            logger.info(f"📊 {symbol} Risk: SL={sl_price}, TP1={tp1_price}, TP2={tp2_price}")
            
            self.trade_levels[symbol] = {
                'side': 'buy' if is_long else 'sell',
                'entry_price': current_price,
                'sl': sl_price,
                'sl_distance': sl_distance,
                'tp1': tp1_price,
                'tp2': tp2_price,
                'amount': amount_to_buy,
                'tp1_hit': False,
                'is_black_swan': is_black_swan,
                'signal_type': signal_type,
                'opened_at': time.time(),
                'trailing_delayed': True if signal_type == "GATEKEEPER" else False,
                'highest_price': current_price if is_long else 0,
                'lowest_price': current_price if not is_long else 0
            }
            # Save and Log
            self.db.save_state("trade_levels", self.trade_levels)
            self.db.log_trade(symbol, side.upper(), current_price, amount_to_buy, 0, "INIT_CONSENSUS")
            self.notifier.notify_trade(symbol, side.upper(), current_price, amount_to_buy, "CONSENSUS_HIT")
            
            # Decrement pending count
            self.pending_orders_count = max(0, self.pending_orders_count - 1)

        except Exception as e:
            # Cleanup on failure
            self.pending_orders_count = max(0, self.pending_orders_count - 1)
            self.active_positions[symbol] = None
            
            error_msg = str(e)
            if "-4411" in error_msg or "agreement" in error_msg.lower():
                logger.error(f"❌ Blacklisting {symbol}: Agreement not signed.")
                self.symbol_blacklist.add(symbol)
                
            logger.error(f"❌ Order failed for {symbol}: {e}")
            logger.error(traceback.format_exc())

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
                            new_symbols_raw = params.get("symbols", self.symbols)
                            new_symbols: List[str] = [str(s) for s in new_symbols_raw] if isinstance(new_symbols_raw, list) else self.symbols
                            
                            self.timeframe = str(params.get("timeframe", self.timeframe))
                            self.rsi_period = int(params.get("rsi_period", self.rsi_period) or self.rsi_period)
                            self.leverage = int(params.get("leverage", self.leverage) or self.leverage)
                            self.usdt_per_trade = float(params.get("usdt_per_trade", self.usdt_per_trade) or self.usdt_per_trade)
                            self.percent_per_trade = float(params.get("percent_per_trade", self.percent_per_trade) or self.percent_per_trade)
                            self.stop_loss_pct = float(params.get("stop_loss_pct", self.stop_loss_pct) or self.stop_loss_pct)
                            self.take_profit_pct = float(params.get("take_profit_pct", self.take_profit_pct) or self.take_profit_pct)
                            self.max_concurrent_positions = int(params.get("max_concurrent_positions", self.max_concurrent_positions) or self.max_concurrent_positions)
                            self.max_global_margin_ratio = float(params.get("max_global_margin_ratio", self.max_global_margin_ratio) or self.max_global_margin_ratio)
                            
                            # Filter new symbols too
                            validated_symbols: List[str] = []
                            for symbol in new_symbols:
                                if symbol in self.exchange.markets:
                                    validated_symbols.append(symbol)
                                    
                                    if symbol not in self.latest_data:
                                        self.latest_data[symbol] = {
                                            'price': 0.0, 'change': 0.0, 'changePercent': 0.0, 'rsi': 0.0, 
                                            'macd': 0.0, 'macd_hist': 0.0, 'bb_upper': 0.0, 'bb_lower': 0.0,
                                            'imbalance': 1.0, 'atr': 0.0, 'ema200': 0.0, 'volume24h': 0.0,
                                            'prediction': "Syncing..."
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
                # --- UNIFIED BALANCE: USDT + USDC ---
                balances = await self.exchange.fetch_balance()
                usdt_bal = float(balances.get('USDT', {}).get('total', 0))
                usdc_bal = float(balances.get('USDC', {}).get('total', 0))
                wallet_balance = usdt_bal + usdc_bal
                
                # Fetch actual positions for precise PnL and Margin
                pos_data = await self.exchange.fetch_positions()
                unrealized_pnl = sum(float(p.get('unrealizedPnl', 0) or 0) for p in pos_data)
                equity = wallet_balance + unrealized_pnl

                if self.initial_wallet_balance is None:
                    self.initial_wallet_balance = wallet_balance
                    logger.info(f"🏦 Unified Balance Initialized: Balance={self.initial_wallet_balance} (USDT: {usdt_bal}, USDC: {usdc_bal})")
                
                # --- OVERHAUL: Circuit Breaker now also considers EQUITY DRAWDOWN (Unrealized) ---
                # This stops the "mess" by reacting to bleeding positions before they are closed.
                wallet_pnl_pct = (wallet_balance - self.initial_wallet_balance) / self.initial_wallet_balance if self.initial_wallet_balance > 0 else 0
                equity_pnl_pct = (equity - self.initial_wallet_balance) / self.initial_wallet_balance if self.initial_wallet_balance > 0 else 0
                
                # Use the worse of the two for the circuit breaker
                effective_pnl_pct = min(wallet_pnl_pct, equity_pnl_pct)

                if effective_pnl_pct <= -0.05: # 5% Daily Equity/Wallet Loss
                    if not self.circuit_breaker_active:
                        logger.critical(f"🛑 CRITICAL: Daily loss limit reached ({effective_pnl_pct:.2%}). Activating Circuit Breaker.")
                        self.circuit_breaker_active = True
                
                # --- NEW: GLOBAL PANIC SELL ---
                if effective_pnl_pct <= -self.panic_drawdown_threshold:
                    logger.critical(f"🆘 GLOBAL PANIC: Drawdown at {effective_pnl_pct:.2%}. CLOSING ALL POSITIONS.")
                    asyncio.create_task(self.emergency_cleanup_all())
                    self.circuit_breaker_active = True
                
                elif effective_pnl_pct > -0.03: # Reset if we recover or restart with better balance
                    if self.circuit_breaker_active:
                        logger.warning(f"🟢 Circuit Breaker Reset: Current loss at {effective_pnl_pct:.2%}")
                        self.circuit_breaker_active = False

                logger.info(f"Account Update: Wallet={wallet_balance:.4f}, Equity={equity:.4f}, PnL={unrealized_pnl:.4f} (Eff: {effective_pnl_pct:.2%})")
                
                if 'info' in balances:
                    info = balances['info']
                    # Attempt to get margin ratio from info or calculate it
                    total_maint_margin = float(info.get('totalMaintMargin', 0))
                    self.current_margin_ratio = float(info.get('marginRatio', 0)) or (total_maint_margin / equity if equity > 0 else 0)
                
                if self.current_margin_ratio > 0.8: # 80% Margin Usage is DANGEROUS
                    logger.critical(f"⚠️ DANGER: Margin Ratio at {self.current_margin_ratio*100:.2f}%! Executing Emergency Cleanup.")
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
                self.latest_account_data['equity'] = equity
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
                            
                            # --- IMPROVED RE-SYNC: Only sync if not already in state ---
                            if self.trade_levels.get(symbol) is None:
                                # Double check active_positions to avoid re-syncing something currently being closed
                                if self.active_positions.get(symbol) is not None:
                                    entry_price = float(pos.get('entryPrice', 0))
                                    amount = float(pos.get('contracts', 0))
                                    if entry_price > 0:
                                        logger.info(f"🔄 Re-syncing trade levels for {symbol} from existing position @ {entry_price}")
                                        current_atr = self.latest_data[symbol].get('atr', entry_price * 0.01) if symbol in self.latest_data else entry_price * 0.01
                                        if current_atr == "N/A" or not current_atr:
                                            current_atr = entry_price * 0.01

                                        # --- TIGHTER RISK MANAGEMENT ---
                                        # Relax SL/TP constraints to avoid premature closures
                                        sl_distance = current_atr * 2.2
                                        tp1_distance = current_atr * 3.0
                                        tp2_distance = current_atr * 6.0
                                        
                                        if side == 'long':
                                            sl = entry_price - sl_distance
                                            tp1 = entry_price + tp1_distance
                                            tp2 = entry_price + tp2_distance
                                        else:
                                            sl = entry_price + sl_distance
                                            tp1 = entry_price - tp1_distance
                                            tp2 = entry_price - tp2_distance
                                        
                                        self.trade_levels[symbol] = {
                                            'side': side,
                                            'entry_price': entry_price,
                                            'sl': float(self.exchange.price_to_precision(symbol, sl)),
                                            'sl_distance': sl_distance,
                                            'tp1': float(self.exchange.price_to_precision(symbol, tp1)),
                                            'tp2': float(self.exchange.price_to_precision(symbol, tp2)),
                                            'amount': amount,
                                            'tp1_hit': False,
                                            'highest_price': entry_price if side == 'long' else 0,
                                            'lowest_price': entry_price if side != 'long' else 0
                                        }
                        else:
                            # Only clear if we not currently in a pending closure
                            if symbol not in self.active_positions or self.active_positions[symbol] is not None:
                                self.active_positions[symbol] = None
                                self.trade_levels[symbol] = None
                else:
                    for symbol in self.symbols:
                        # Only clear if we are not currently trying to open or close something for this symbol
                        # (Checking positions directly from Binance is the source of truth)
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
                
                # Sync trades to local database for history tracking
                if all_trades:
                    # Eseguiamo il sync in un thread per non bloccare il loop asincrono
                    await asyncio.to_thread(self.db.sync_binance_trades, all_trades)
                    
                # Sort trades by timestamp descending and keep top 20
                all_trades.sort(key=lambda x: x.get('timestamp', 0) or 0, reverse=True)
                self.latest_account_data['trades'] = all_trades[:20]
                logger.info(f"Account Update: Found {len(all_trades)} recent trades, synced to DB.")
                
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
                        # Store in latest_data for the entry filter to use
                        if symbol in self.latest_data:
                            self.latest_data[symbol]['funding_rate'] = float(rate_raw)
                        
                        # -- DISABLED: Entering naked directional trades just for funding is too dangerous --
                        # if rate_pct > 0.1 and self.active_positions[symbol] != 'SHORT':
                        #     logger.warning(f"🏦 FUNDING ARB: Extreme positive funding ({rate_pct:.3f}%) on {symbol}. Executing SHORT to collect fees.")
                        #     current_price = self.latest_data[symbol]['price']
                        #     if current_price > 0:
                        #         self.active_positions[symbol] = 'SHORT'
                        #         asyncio.create_task(self.execute_order(symbol, 'sell', current_price))
                        # 
                        # elif rate_pct < -0.1 and self.active_positions[symbol] != 'LONG':
                        #     logger.warning(f"🏦 FUNDING ARB: Extreme negative funding ({rate_pct:.3f}%) on {symbol}. Executing LONG to collect fees.")
                        #     current_price = self.latest_data[symbol]['price']
                        #     if current_price > 0:
                        #         self.active_positions[symbol] = 'LONG'
                        #         asyncio.create_task(self.execute_order(symbol, 'buy', current_price))
                        pass
                            
            except Exception as e:
                if "does not support fetchFundingRates" not in str(e):
                    logger.error(f"Error checking funding rates: {e}")
            await asyncio.sleep(300) # Check every 5 minutes

    def get_dashboard_data(self):
        return self.latest_data
        
    def get_account_data(self):
        return self.latest_account_data

    async def emergency_cleanup_all(self):
        """Emergency function to close every single open position immediately."""
        try:
            logger.warning("🛡️ Starting Emergency Cleanup of ALL positions...")
            positions = await self.exchange.fetch_positions()
            active_pos = [p for p in positions if float(p.get('contracts', 0) or 0) != 0]
            
            for pos in active_pos:
                symbol = pos['symbol']
                side = pos['side'].lower()
                close_side = 'sell' if side == 'long' else 'buy'
                amount = float(pos['contracts'])
                
                logger.warning(f"🆘 Emergency closing {symbol} ({amount} {side})")
                current_price = self.latest_data[symbol].get('price', 0) if symbol in self.latest_data else 0
                await self.exchange.create_order(symbol, 'market', close_side, amount, current_price, {'reduceOnly': True})
                
                self.active_positions[symbol] = None
                self.trade_levels[symbol] = None
            
            self.db.save_state("trade_levels", self.trade_levels)
            logger.info("✅ All positions closed successfully.")
            self.notifier.notify_alert("CRITICAL", "GLOBAL PANIC SELL EXECUTED", "All positions closed due to excess drawdown.")
        except Exception as e:
            logger.error(f"Failed to execute emergency cleanup: {e}")
    async def reset_circuit_breaker(self):
        """Resets the initial wallet balance to current balance, clearing the circuit breaker."""
        try:
            balances = await self.exchange.fetch_balance()
            usdt_bal = float(balances.get('USDT', {}).get('total', 0))
            usdc_bal = float(balances.get('USDC', {}).get('total', 0))
            wallet_balance = usdt_bal + usdc_bal
            
            self.initial_wallet_balance = wallet_balance
            self.circuit_breaker_active = False
            logger.warning(f"🟢 MANUAL RESET: Circuit Breaker cleared. New base balance: {wallet_balance:.2f} USDT")
            return True
        except Exception as e:
            logger.error(f"Failed to reset circuit breaker: {e}")
            return False
