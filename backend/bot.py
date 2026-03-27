import ccxt.async_support as ccxt
import pandas as pd
import asyncio
import traceback
import logging
from dotenv import load_dotenv
import os
import json
import time
from collections import deque
from ml_predictor import MLPredictor
from database import BotDatabase
from telegram_notifier import TelegramNotifier
from signal_manager import SignalManager
from llm_analyst import LLMAnalyst
from sector_manager import SectorManager
import feedparser
from typing import Any, Dict, List, Optional
from asset_scanner import AssetScanner

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
        self.daily_loss_limit = params.get("daily_loss_limit", 1.0)
        self.panic_drawdown_threshold = max(0.15, self.daily_loss_limit + 0.05)
        self.current_margin_ratio = 0.0
        
        # Select active exchange from environment
        self.active_exchange_name = os.getenv("ACTIVE_EXCHANGE", "binance").lower()
        
        # Load API Credentials for active exchange
        if self.active_exchange_name == "bitget":
            api_key = os.getenv("BITGET_API_KEY")
            api_secret = os.getenv("BITGET_API_SECRET")
            password = os.getenv("BITGET_PASSWORD")
            
            self.exchange = ccxt.bitget({
                'apiKey': api_key,
                'secret': api_secret,
                'password': password,
                'enableRateLimit': True,
                'options': {'defaultType': 'swap'},
                'timeout': 10000,
            })
            mode_str = "BITGET PRODUCTION"
        else:
            api_key = os.getenv("EXCHANGE_API_KEY")
            api_secret = os.getenv("EXCHANGE_API_SECRET")
            
            self.exchange = ccxt.binanceusdm({
                'apiKey': api_key,
                'secret': api_secret,
                'enableRateLimit': True,
                'timeout': 10000,
            })
            
            # Set to sandbox mode based on .env
            sandbox_mode = os.getenv("BINANCE_SANDBOX", "true").lower() == "true"
            self.exchange.set_sandbox_mode(sandbox_mode)
            mode_str = "BINANCE TESTNET" if sandbox_mode else "BINANCE PRODUCTION"

        logger.warning(f"🚀 INITIALIZING BOT IN {mode_str} MODE")
        
        # Debug: Check if keys are loaded
        if api_key:
            logger.info(f"Using {self.active_exchange_name} API Key: {api_key[:4]}...{api_key[-4:]}")
        else:
            logger.error(f"{self.active_exchange_name} API_KEY IS EMPTY!")
        
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
        self.analyst = LLMAnalyst(self)
        self.ml_cooldowns: Dict[str, float] = {symbol: 0.0 for symbol in self.symbols}
        self.llm_cooldowns: Dict[str, float] = {symbol: 0.0 for symbol in self.symbols}
        self.last_llm_side: Dict[str, str] = {symbol: "" for symbol in self.symbols}
        
        # Institutional Alert History for Frontend
        self.alert_history = []
        self.max_alerts = 50
        
        # VPS Modules
        self.db = BotDatabase()
        self.notifier = TelegramNotifier()
        self.signal_manager = SignalManager(self)
        self.scanner = AssetScanner(self.exchange)
        
        # Resource management
        self.ml_semaphore = asyncio.Semaphore(1) 
        self.api_semaphore = asyncio.Semaphore(5) 
        
        # Risk Management & Guards
        self.circuit_breaker_active = False
        self.global_panic_notified = False
        self.symbol_blacklist = set()
        self.initial_wallet_balance: Optional[float] = None
        self.previous_prices: Dict[str, float] = {symbol: 0.0 for symbol in self.symbols}
        
        # --- NEW: Time-Domain Lock (DeepSeek Refinement) ---
        self.hft_locks: Dict[str, float] = {symbol: 0.0 for symbol in self.symbols}
        self.start_time = time.time() # To filter trades in sync
        
        # 5-minute rolling window
        self.price_windows: Dict[str, deque] = {symbol: deque(maxlen=30) for symbol in self.symbols}
        # 2-minute rolling window for VeloCity Emergency Exit
        self.price_history_2m: Dict[str, deque] = {symbol: deque(maxlen=12) for symbol in self.symbols}
        
        self.min_leverage = 2
        self.max_leverage = 25
        
        self.sector_manager = SectorManager()
        self.current_news = "" # Real-time market sentiment
        
        # Recupero stato persistente
        saved_levels = self.db.load_state("trade_levels")
        if saved_levels:
            self.trade_levels = saved_levels
            logger.info("📦 Stato trade_levels recuperato correttamente.")
        
        # Exchange instance will be initialized with load_markets in initialize()
        self.initialized = False

    async def initialize(self, skip_leverage=False):
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
            
            # Imposta leva solo se richiesto
            if not skip_leverage:
                logger.info("⚙️ Setting leverage for all symbols...")
                await self._set_leverage_for_all()
                logger.info("✅ Leverage set successfully.")
            
            self.initialized = True
            
            # --- START DYNAMIC ASSET SCANNER ---
            logger.info("🔍 Running initial Market Scan...")
            await self.refresh_symbols()
            
            # --- NEW: ZOMBIE POSITION RECOVERY (v3.5) ---
            logger.info("🧟 Checking for Zombie Positions on Binance...")
            await self.sync_zombie_positions()

            # --- START BACKGROUND AI TASKS ---
            logger.info("🤖 Activating AI Autonomy Loops (Monitor & Refresh)...")
            asyncio.create_task(self.monitor_position_health_loop())
            asyncio.create_task(self.refresh_symbols_loop())
            asyncio.create_task(self.weekly_self_audit_loop())
            asyncio.create_task(self.news_sentiment_loop())
            
            logger.warning("🤖 [PHASE 4] Institutional Intelligence Loops Active (MTF + Sector + Audit + News)")
            
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

    async def fetch_mtf_trend(self, symbol):
        """Fetches 4h and 1d OHLCV to determine macro trend."""
        try:
            # 4h Trend
            ohlcv_4h = await self.exchange.fetch_ohlcv(symbol, '4h', limit=20)
            df_4h = pd.DataFrame(ohlcv_4h, columns=['t', 'o', 'h', 'l', 'c', 'v'])
            ema200_4h = df_4h['c'].ewm(span=200, adjust=False).mean().iloc[-1] if len(df_4h) > 5 else 0
            current_4h = df_4h['c'].iloc[-1]
            trend_4h = "BULLISH" if current_4h > ema200_4h else "BEARISH"
            
            # 1d Trend
            ohlcv_1d = await self.exchange.fetch_ohlcv(symbol, '1d', limit=10)
            df_1d = pd.DataFrame(ohlcv_1d, columns=['t', 'o', 'h', 'l', 'c', 'v'])
            current_1d = df_1d['c'].iloc[-1]
            prev_1d = df_1d['c'].iloc[-2]
            trend_1d = "UP" if current_1d > prev_1d else "DOWN"
            
            return f"4h: {trend_4h}, 1d: {trend_1d}"
        except Exception as e:
            logger.error(f"Error fetching MTF for {symbol}: {e}")
            return "4h: UNKNOWN, 1d: UNKNOWN"

    async def fetch_data_and_analyze(self, symbol):
        try:
            # Using direct await instead of asyncio.to_thread with async_support
            ohlcv = await self.exchange.fetch_ohlcv(symbol, self.timeframe, limit=100)
            mtf_context = await self.fetch_mtf_trend(symbol)
            
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
            change_5m_pct = 0
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
                    
                # --- VELOCITY EMERGENCY EXIT (2m Anti-Ruin) ---
                history_2m = self.price_history_2m[symbol]
                history_2m.append(current_close)
                if len(history_2m) >= 6: # At least 1 minute of data
                    ref_2m = history_2m[0]
                    change_2m_pct = (current_close - ref_2m) / ref_2m
                    
                    # --- DYNAMIC SOS THRESHOLD (ATR-based) ---
                    # We use ATR/Price as a proxy for 'normal' volatility.
                    # Threshold is 1x the 15m ATR, capped between 0.6% and 3.5%
                    atr_val = current_atr if isinstance(current_atr, (int, float)) else (current_close * 0.015)
                    atr_pct = (atr_val / current_close) if current_close > 0 else 0.015
                    sos_threshold = max(0.006, min(0.035, atr_pct * 1.0))
                    
                    active_pos = self.active_positions.get(symbol)
                    if active_pos:
                        # If LONG and price drops > sos_threshold in 2m
                        if active_pos == 'LONG' and change_2m_pct < -sos_threshold:
                            logger.critical(f"🆘 [VELOCITY EXIT] Closing LONG {symbol}: Volatility Spike Against Position ({change_2m_pct:.2%} < -{sos_threshold:.2%})")
                            asyncio.create_task(self.close_position(symbol, self.trade_levels[symbol]))
                        # If SHORT and price pumps > sos_threshold in 2m
                        elif active_pos == 'SHORT' and change_2m_pct > sos_threshold:
                            logger.critical(f"🆘 [VELOCITY EXIT] Closing SHORT {symbol}: Volatility Spike Against Position ({change_2m_pct:.2%} > {sos_threshold:.2%})")
                            asyncio.create_task(self.close_position(symbol, self.trade_levels[symbol]))
                    else:
                        # --- PROACTIVE VELOCITY ENTRIES ---
                        # 1. Momentum Entry: Ride the wave if move > 1.2 * SOS threshold
                        if change_2m_pct > (sos_threshold * 1.2):
                            logger.warning(f"🚀 [VELOCITY MOMENTUM] Potential breakout detected for {symbol} (+{change_2m_pct:.2%})")
                            asyncio.create_task(self.handle_signal(symbol, "VELOCITY_MOMENTUM", "buy", weight_modifier=1.2, current_price=current_close, ema200=current_ema200))
                        elif change_2m_pct < -(sos_threshold * 1.2):
                            logger.warning(f"🚀 [VELOCITY MOMENTUM] Potential breakdown detected for {symbol} (-{change_2m_pct:.2%})")
                            asyncio.create_task(self.handle_signal(symbol, "VELOCITY_MOMENTUM", "sell", weight_modifier=1.2, current_price=current_close, ema200=current_ema200))
                        
                        # 2. Reversal Entry: Catch the rubber band if extreme overextension + exhaustion
                        # Requires very high RSI AND a small price reversal in the last few periods
                        if len(history_2m) >= 4:
                            prev_move = (history_2m[-2] - history_2m[0]) / history_2m[0]
                            # Check if current close is starting to reverse from the peak of the move
                            is_reversing = (current_close < history_2m[-2]) if prev_move > sos_threshold else (current_close > history_2m[-2] if prev_move < -sos_threshold else False)
                            
                            if is_reversing and isinstance(current_rsi, (int, float)):
                                if prev_move > (sos_threshold * 1.5) and current_rsi > 82:
                                    logger.warning(f"🪃 [VELOCITY REVERSAL] Overextended PUMP detected for {symbol}. RSI={current_rsi}. Shorting rebound.")
                                    asyncio.create_task(self.handle_signal(symbol, "VELOCITY_REVERSAL", "sell", weight_modifier=1.1, current_price=current_close, ema200=current_ema200))
                                elif prev_move < -(sos_threshold * 1.5) and current_rsi < 18:
                                    logger.warning(f"🪃 [VELOCITY REVERSAL] Overextended DUMP detected for {symbol}. RSI={current_rsi}. Longing rebound.")
                                    asyncio.create_task(self.handle_signal(symbol, "VELOCITY_REVERSAL", "buy", weight_modifier=1.1, current_price=current_close, ema200=current_ema200))
                    
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
            
            # --- INSTITUTIONAL DATA FETCH (OI & L/S Ratio) ---
            current_oi = 0
            current_ls = 1.0
            try:
                # fetch_open_interest usually works in sandbox if available
                oi_data = await self.exchange.fetch_open_interest(symbol)
                current_oi = float(oi_data.get('openInterestAmount', 0))
                
                # Global Account L/S Ratio (5m) - Skip or use production fallback if in sandbox
                if not os.getenv('BINANCE_SANDBOX', 'false').lower() == 'true':
                    ls_data = await self.exchange.fapiDataGetGlobalLongShortAccountRatio({'symbol': symbol.replace('/', '').split(':')[0], 'period': '5m', 'limit': 1})
                    current_ls = float(ls_data[0]['longShortRatio']) if ls_data else 1.0
            except Exception as se:
                logger.error(f"⚠️ Sentiment fetch limited for {symbol}: {se}")

            # --- FUNDING RATE FETCH ---
            funding = await self.exchange.fetch_funding_rate(symbol)
            funding_rate = float(funding.get('fundingRate', 0))
            
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
                'funding_rate': funding_rate,
                'open_interest': current_oi,
                'ls_ratio': current_ls,
                'prediction': predicted_price if predicted_price else "Syncing ML..."
            }
            
            # --- SENTIMENT SIGNAL GENERATION ---
            # 1. OI Expansion (More participation)
            prev_oi = self.latest_data[symbol].get('open_interest', 0) if symbol in self.latest_data else 0
            oi_delta = (current_oi - prev_oi) / prev_oi if prev_oi > 0 else 0
            
            # 2. Bullish Confluence: Price up + OI up + L/S not extreme
            if change_5m_pct > 0.005 and oi_delta > 0.005 and current_ls < 1.3:
                asyncio.create_task(self.handle_signal(symbol, "SENTIMENT", "buy", weight_modifier=1.2, current_price=current_close, ema200=current_ema200))
            # 3. Bearish Confluence: Price down + OI up + L/S not extreme
            elif change_5m_pct < -0.005 and oi_delta > 0.005 and current_ls > 0.7:
                asyncio.create_task(self.handle_signal(symbol, "SENTIMENT", "sell", weight_modifier=1.2, current_price=current_close, ema200=current_ema200))

            self._check_soft_stop_loss(symbol, current_close)
            self._check_trading_signals(symbol, current_close, current_rsi, current_macd_hist, current_bb_lower, current_bb_upper, imbalance, current_ema200, quote_volume, current_atr, current_adx, mtf_context, change_5m_pct)
            
        except Exception as e:
            logger.error(f"Error analyzing {symbol}: {e}")

    def _check_trading_signals(self, symbol, price, rsi, macd_hist, bb_lower, bb_upper, imbalance, ema200, volume24h, atr, adx, mtf_context, change_5m_pct):
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

        # --- SECTOR CAP CHECK (v4.0) ---
        sector = self.sector_manager.get_sector(symbol)
        current_sector_count = sum(1 for s, side in self.active_positions.items() if side is not None and self.sector_manager.get_sector(s) == sector)
        if current_sector_count >= 5: # Increased to 5 for institutional diversification
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
            # --- SHORTING MOMENTUM GUARD ---
            # Don't short if there is strong bullish momentum in the last 5 minutes
            if change_5m_pct > 0.008: # 0.8% in 5m is too strong to short technically
                logger.info(f"🛡️ [SHORT GUARD] Skipping technical SHORT on {symbol}: Momentum too bullish ({change_5m_pct:.2%})")
            elif not price_above_ema and is_trending: # Trend-following SHORT
                tech_side = 'sell'
                is_technical_signal = True

        # --- FUNDING RATE FILTER (DEEPSEEK REFACTOR) ---
        # Don't open LONGs if the funding rate is extreme (>0.05%)
        # Don't open SHORTs if the funding rate is extreme (< -0.05%)
        current_funding = self.latest_data.get(symbol, {}).get('funding_rate', 0)
        threshold = self.risk_profile.get("trading_parameters", {}).get("funding_filter_threshold", 0.0005)
        
        if tech_side == 'buy' and current_funding > threshold:
            logger.warning(f"🚫 [FUNDING GUARD] Skipping LONG on {symbol} due to high funding rate: {current_funding*100:.3f}%")
            return
        elif tech_side == 'sell' and current_funding < -threshold:
            logger.warning(f"🚫 [FUNDING GUARD] Skipping SHORT on {symbol} due to negative funding rate (Squeeze Risk): {current_funding*100:.3f}%")
            return

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
                asyncio.create_task(self.handle_signal(symbol, "AI", "buy", weight_modifier=1.2, current_price=price, ema200=ema200, ai_confidence=confidence, mtf_context=mtf_context))
            elif direction == 0 and confidence > 0.55 and not price_above_ema:
                asyncio.create_task(self.handle_signal(symbol, "AI", "sell", weight_modifier=1.2, current_price=price, ema200=ema200, ai_confidence=confidence, mtf_context=mtf_context))

    async def handle_signal(self, symbol, type, side, weight_modifier=1.0, is_black_swan=False, current_price=None, ema200=None, ai_confidence=0.0, mtf_context="N/A"):
        if not self.signal_manager:
            return
            
        now = time.time()
        
        # --- POSITION CONFLICT RESOLVER (DeepSeek v3) ---
        # 1. Time-Domain Lock check
        if type in ["TECH", "AI"]:
            lock_time = self.hft_locks.get(symbol, 0)
            if now - lock_time < 60:
                logger.info(f"⏳ [TIME-DOMAIN LOCK] Ignoring strategic {type} signal for {symbol} (HFT priority active).")
                return

        # 2. Existing Position Logic check
        existing_pos = self.active_positions.get(symbol)
        if existing_pos and type in ["TECH", "AI"]:
            # Check if existing position is HFT
            trade = self.trade_levels.get(symbol)
            if trade and trade.get('signal_type') in ["LIQUIDATION", "DEX_ARBITRAGE"]:
                # --- Emergency Flip Logic ---
                # Allow flip only if consensus is high AND current trade is in loss
                entry_price = float(trade.get('entry_price', 0))
                is_long = trade.get('side') in ['buy', 'long']
                pnl_pct = (current_price - entry_price) / entry_price if is_long else (entry_price - current_price) / entry_price
                
                # Signal strength check is done later via SignalManager, but we pre-check here
                if pnl_pct > -0.005: # Only -0.5% threshold
                    logger.info(f"🚫 [CONFLICT RESOLVER] Active HFT on {symbol} is stable. Ignoring strategic Flip.")
                    return

        approved, consensus_score = await self.signal_manager.add_signal(symbol, type, side, weight_modifier, current_price=current_price, ema200=ema200, ai_confidence=ai_confidence)
        
        if approved:
            # If it's an HFT signal, set the lock
            if type in ["LIQUIDATION", "DEX_ARBITRAGE"]:
                self.hft_locks[symbol] = now
                logger.warning(f"🔒 [TIME-DOMAIN LOCK] Lock ACTIVE for {symbol} (60s).")

            # --- MASTER CONTROLLER: NET EXPOSURE & PRIORITY ---
            existing_pos = self.active_positions.get(symbol)
            if existing_pos:
                # 1. Prevent conflicting positions (Long vs Short)
                existing_side = 'buy' if existing_pos == 'LONG' else 'sell'
                if existing_side != side.lower():
                    # --- REVERSAL PRIORITY ---
                    # If it's a Black Swan or Gatekeeper news, we MUST flip the position
                    if is_black_swan or type in ["GATEKEEPER", "NEWS"]:
                        logger.critical(f"🆘 [REVERSAL] Emergency Flip for {symbol}: Closing {existing_pos} for {side.upper()} {type}")
                        await self.close_position(symbol, self.trade_levels[symbol])
                    else:
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
                    
                    await self.execute_order(symbol, side.lower(), self.latest_data[symbol]['price'], 
                                           is_black_swan=is_black_swan, consensus_score=consensus_score, 
                                           signal_type=type, mtf_context=mtf_context)

    def _check_soft_stop_loss(self, symbol, current_price):
        trade = self.trade_levels.get(symbol)
        if not trade:
            return
            
        # Normalize side (handle both 'buy'/'long' and 'sell'/'short')
        side = str(trade.get('side', 'buy')).lower()
        is_long = side in ['buy', 'long']
        sl = float(trade.get('sl', 0))
        tp1 = float(trade.get('tp1', 0))
        tp2 = float(trade.get('tp2', 0))
        ai_tp = trade.get('ai_tp_price')
        if ai_tp is not None:
            ai_tp = float(ai_tp)
        
        # --- DUAL STOP LOSS (DeepSeek v3.2 - Phase 4) ---
        # Technical Stop: 3x ATR (Active Exit - Relaxed from 2x)
        # Survival Stop: 5x ATR (Safety Net)
        tech_multiplier = 3.0
        survival_multiplier = 5.0
        
        latest = self.latest_data.get(symbol, {})
        current_atr = latest.get('atr')
        
        if isinstance(current_atr, (float, int)) and current_atr > 0:
            sl_distance = float(current_atr) * tech_multiplier
            survival_sl_dist = float(current_atr) * survival_multiplier
        else:
            sl_distance = float(trade.get('sl_distance', current_price * self.stop_loss_pct))
            survival_sl_dist = sl_distance * 2.5
            
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
                    # Use Technical Stop for active trailing
                    new_sl = current_price - sl_distance
                    if new_sl > sl:
                        trade['sl'] = new_sl
                        sl = new_sl
                        logger.info(f"📈 TRAILING TECHNICAL STOP UPDATED for {symbol}: SL now at {new_sl:.6f}")
            
            # 2. Check Exit Conditions
            # Ultimate Safety: Survival Stop (5x ATR)
            survival_sl = trade.get('entry_price') - survival_sl_dist
            if current_price <= survival_sl:
                should_close = True
                reason = "SURVIVAL STOP (5x ATR)"
            elif current_price <= sl:
                should_close = True
                reason = "TECHNICAL STOP (2x ATR Trailing)"
            elif current_price >= tp2:
                should_close = True
                reason = "TAKE-PROFIT 2 (FINAL)"
            elif current_price >= tp1:
                # Partial Take Profit (TP1) - Close 50%
                if not trade.get('tp1_hit', False):
                    logger.warning(f"🎯 PARTIAL TP1 HIT for {symbol} at {current_price}!")
                    asyncio.create_task(self.close_position(symbol, trade, partial_pct=0.5, reason="PARTIAL_EXIT"))
                    trade['tp1_hit'] = True
                    # Move SL to fee-covering break-even (Entry + 0.30% profit)
                    offset = entry_price * 0.003
                    trade['sl'] = max(entry_price + offset, trade.get('sl', 0))
                    
        else: # SHORT
            # 1. Update Trailing Low
            if current_price < trade.get('lowest_price', 999999):
                trade['lowest_price'] = current_price
                # Update Trailing SL if not delayed
                if not trade.get('trailing_delayed'):
                    # Use Technical Stop for active trailing
                    new_sl = current_price + sl_distance
                    if sl == 0 or new_sl < sl:
                        trade['sl'] = new_sl
                        sl = new_sl
                        logger.info(f"📉 TRAILING TECHNICAL STOP UPDATED for {symbol}: SL now at {new_sl:.6f}")
            
            # 2. Check Exit Conditions
            # Ultimate Safety: Survival Stop (5x ATR)
            survival_sl = trade.get('entry_price', current_price) + survival_sl_dist
            if current_price >= survival_sl:
                should_close = True
                reason = "SURVIVAL STOP (5x ATR)"
            elif current_price >= sl:
                should_close = True
                reason = "TECHNICAL STOP (2x ATR Trailing)"
            elif current_price <= tp2:
                should_close = True
                reason = "TAKE-PROFIT 2 (FINAL)"
            elif ai_tp and current_price <= ai_tp:
                should_close = True
                reason = "AI PRECISE TARGET HIT"
            elif current_price <= tp1:
                # Partial Take Profit (TP1) - Close 50%
                if not trade.get('tp1_hit', False):
                    logger.warning(f"🎯 PARTIAL TP1 HIT for {symbol} at {current_price}!")
                    asyncio.create_task(self.close_position(symbol, trade, partial_pct=0.5, reason="PARTIAL_EXIT"))
                    trade['tp1_hit'] = True
                    # Move SL to fee-covering break-even (Entry - 0.30% profit)
                    offset = entry_price * 0.003
                    trade['sl'] = min(entry_price - offset, trade.get('sl', 999999))
                
        if should_close:
            logger.warning(f"🔔 SOFT {reason} HIT for {symbol} at {current_price}!")
            # Aggiorna l'esito dell'IA prima di chiudere
            pnl = (current_price - entry_price) / entry_price if is_long else (entry_price - current_price) / entry_price
            self.db.update_trade_outcome(trade.get('snapshot_id'), current_price, pnl)
            
            asyncio.create_task(self.close_position(symbol, trade, reason=reason))
            self.trade_levels[symbol] = None
            self.db.save_state("trade_levels", self.trade_levels)

    async def close_position(self, symbol, trade, partial_pct=1.0, reason=None):
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
            
            # Use provided reason or fallback to default
            if not reason:
                reason = "EXIT" if partial_pct >= 1.0 else "PARTIAL_EXIT"
            
            # Log with exchange_trade_id to prevent duplicates in sync_binance_trades
            self.db.log_trade(symbol, "CLOSE" if partial_pct >= 1.0 else "CLOSE_PARTIAL", execution_price, amount_to_close, pnl, reason, exchange_trade_id=order_id)
            
            self.notifier.notify_trade(symbol, "CLOSE", execution_price, amount_to_close, reason, pnl=pnl)
            
            # Save state immediately to persist closure
            self.db.save_state("trade_levels", self.trade_levels)
                
        except Exception as e:
            logger.error(f"Failed to close position for {symbol}: {e}")
            logger.error(traceback.format_exc())
    # Calculate dynamic order amount based on risk capital, equity, and confidence
    def _calculate_order_amount(self, symbol, current_price, custom_leverage=None, consensus_score=3.5, **kwargs):
        leverage = custom_leverage if custom_leverage else self.leverage
        
        # 1. Dynamic % Sizing based on Consensus (Phase 4: Synced with Config)
        # Base: From risk_profile (User 5.0%)
        # Bonus: +0.5% per 1.0 point above threshold (3.0)
        risk_pct = self.risk_profile.get("trading_parameters", {}).get("percent_per_trade", 5.0)
        
        if self.circuit_breaker_active:
             total_pct = 0.5
             logger.warning(f"☢️ [SIZING] Circuit Breaker Exception sizing: 0.5% for {symbol}.")
        elif self.is_macro_paused:
             total_pct = 0.5
             logger.warning(f"🛡️ [SIZING] Macro Volatility Shield active: Capping size to 0.5% for {symbol}.")
        else:
            base_pct = float(risk_pct)
            # Use 3.0 as the new "low" threshold for bonus calculation (Phase 4)
            bonus_pct = max(0, (float(consensus_score) - 3.0) * 0.5)
            # --- NEW: AI STRENGTH MODIFIER ---
            ai_mod = kwargs.get('ai_strength', 1.0)
            total_pct = (base_pct + bonus_pct) * ai_mod
            if ai_mod != 1.0:
                logger.info(f"🧠 [AI SIZING] Applied AI Strength Multiplier: {ai_mod:.2f}x (Final Pct: {total_pct:.2f}%)")
        
        # 2. Base Capital Calculation from Equity
        current_equity = self.latest_account_data.get('equity', 0)
        if current_equity <= 0:
             # Fallback to wallet balance
             current_equity = self.latest_account_data.get('total_wallet_balance', 1000)
             
        base_usdt = current_equity * (total_pct / 100.0)
        
        # 3. Volatility-based capital allocation (ATR modifier)
        current_atr = self.latest_data[symbol].get('atr', current_price * 0.01)
        if current_atr == "N/A" or not current_atr:
            current_atr = current_price * 0.01
            
        atr_pct = (current_atr / current_price) if current_price > 0 else 0.01
        baseline_atr_pct = 0.01  # Baseline expected volatility per candle
        
        # Floor at 0.5 (Phase 4) - Avoid micro-trades
        volatility_modifier = baseline_atr_pct / atr_pct if atr_pct > 0 else 1.0
        volatility_modifier = max(0.5, min(2.0, volatility_modifier))
        
        # 4. Final Allocation
        adjusted_usdt = base_usdt * volatility_modifier
        
        # Global Safety Cap: 10% of equity per trade (extra cautious)
        max_limit = current_equity * 0.10
        if adjusted_usdt > max_limit:
            adjusted_usdt = max_limit

        purchasing_power = adjusted_usdt * leverage
        amount = purchasing_power / current_price
        
        logger.info(f"📐 [{symbol}] Sizing Summary: {total_pct:.1f}% Base, VolMod={volatility_modifier:.2f} -> {adjusted_usdt:.2f} USDT Margin (Score: {consensus_score})")
        
        # Adjust for exchange precision rules
        amount = self.exchange.amount_to_precision(symbol, amount)
        return float(amount)

    def calculate_dynamic_leverage(self, symbol, side, consensus_score, current_price, **kwargs):
        """Calculates leverage based on consensus score, volatility, and trend safety."""
        latest = self.latest_data.get(symbol, {})
        atr = latest.get('atr', current_price * 0.01)
        if not atr or atr == "N/A":
            atr = current_price * 0.01
        
        # 1. Base leverage (AI suggested or config fallback)
        ai_lev = kwargs.get('ai_leverage')
        base_lev = ai_lev if ai_lev is not None else self.leverage
        if ai_lev:
            logger.info(f"🧠 [AI LEVERAGE] Using AI Suggested Leverage Target: {ai_lev}x")
        
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
        
        # 5. Sector Dynamic Caps (v4.0)
        sector = self.sector_manager.get_sector(symbol)
        # Base Caps: MAJOR=25, ALTS=20
        max_cap = 25 if sector == "MAJOR" else 20
        
        # High Conviction "Ultra" Bonus
        if consensus_score >= 8.0:
            max_cap += 5 # Push to 25x for Major, 20x for Alts
            logger.warning(f"🔥 [Leverage] High Conviction Bonus applied for {symbol} (Max Cap: {max_cap})")
        
        final_lev = max(self.min_leverage, min(max_cap, round(lev)))
        logger.info(f"📐 [Leverage] {symbol} Calculation: Base={base_lev}, ScoreBonus={score_bonus:.1f}, VolPenalty={vol_penalty:.1f}, TrendBonus={trend_bonus} -> Final={final_lev}x")
        
        return final_lev

    async def execute_order(self, symbol, side, current_price, is_black_swan=False, consensus_score=5.0, signal_type="TECH", mtf_context="N/A"):
        # --- GLOBAL RISK CHECKS ---
        if symbol in self.symbol_blacklist:
            logger.warning(f"🚫 Skipping {symbol}: In blacklist.")
            return
            
        if self.circuit_breaker_active:
            # --- HIE EXCEPTION (DeepSeek v3) ---
            # Allow Gatekeeper and Liquidation signals even during circuit breaker, but with reduced size
            if signal_type in ["GATEKEEPER", "LIQUIDATION", "DEX_ARBITRAGE"]:
                logger.warning(f"☢️ [CB EXCEPTION] Allowing high-impact {signal_type} despite Circuit Breaker (Reduced Size).")
            else:
                logger.error(f"🛑 CIRCUIT BREAKER ACTIVE: Skipping {signal_type} entries.")
                return

        try:
            # FIX #1: Acquire lock to serialize order execution and prevent position count race condition
            async with self.order_lock:
                # Re-check limit inside the lock with latest confirmed info
                confirmed_positions = [p for p in self.latest_account_data.get('positions', []) if float(p.get('contracts', 0) or 0) != 0]
                
                # --- [REMOVED] Global Position Count Limit ---
                # User requested to trade purely based on margin ratio instead of position count.
                
                # --- [UPGRADED] Sector Caps & Management (v4.0) ---
                current_equity = self.latest_account_data.get('equity', 1000)
                is_safe = self.sector_manager.is_exposure_safe(symbol, self.trade_levels, current_equity)
                
                if not is_safe:
                    # --- HIGH CONSENSUS OVERRIDE (v3.1) ---
                    # If consensus is exceptional (7.5+), bypass sector cap to capture rare "Golden Opportunities"
                    if float(consensus_score) >= 7.5:
                        logger.warning(f"⚡ [OVERRIDE] Sector limit reached, but Consensus Score is HIGH ({consensus_score:.2f}). Allowing entry.")
                    else:
                        logger.warning(f"🚫 [SECTOR LIMIT] Skipping {symbol}: Sector capacity reached.")
                        self.active_positions[symbol] = None
                        return

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

                # --- AI STRATEGIC REVIEW (With Cost-Optimization Cooldown) ---
                now = time.time()
                last_review = self.llm_cooldowns.get(symbol, 0)
                last_side = self.last_llm_side.get(symbol, "")
                
                # Cooldown Bypass logic
                # News signals (GATEKEEPER) are already Gemini-approved in NewsRadar. Skip secondary review to save 50+ calls.
                is_news_signal = signal_type == "GATEKEEPER"
                is_high_priority = signal_type == "WHALE"
                is_side_flip = side.lower() != last_side
                
                # Cooldown Duration: 300s (5m) for standard, 900s (15m) for recent REJECTs
                cooldown_active = (now - last_review) < 300 
                
                if cooldown_active and not is_news_signal and not is_high_priority and not is_side_flip:
                    logger.info(f"❄️ [COST OPTIMIZATION] Skipping repetitive AI Review for {symbol}")
                    self.active_positions[symbol] = None
                    self.pending_orders_count = max(0, self.pending_orders_count - 1)
                    return

                # Initial defaults
                approved, ai_strength, ai_leverage, ai_sl_mult, ai_tp_mult, ai_tp_price, reason = True, 1.0, self.leverage, 1.0, 1.0, None, "Auto-approved"

                if is_news_signal:
                    logger.info(f"🛡️ [NEWS PASS] Bypassing secondary LLM review for {symbol} (Trusting Gatekeeper)")
                else:
                    # Retrieve current indicators for context
                    indicators = self.latest_data.get(symbol, {})
                    # Ask the LLM to analyze the setup
                    approved, ai_strength, ai_leverage, ai_sl_mult, ai_tp_mult, ai_tp_price, reason = await self.analyst.decide_strategy(
                        symbol, side, signal_type, indicators, mtf_context=mtf_context, news_context=self.current_news
                    )
                    
                    # Update Cooldown state
                    # If REJECTED, set a longer cooldown (900s) to avoid spamming the same setup
                    self.llm_cooldowns[symbol] = now if approved else now + 600 # +600 makes it 15m total (300+600)
                    self.last_llm_side[symbol] = side.lower()
                
                if not approved:
                    logger.warning(f"🧠 [LLM STRATEGIC REVIEW] REJECTED {symbol} {side.upper()}: {reason}")
                    self.active_positions[symbol] = None
                    self.pending_orders_count = max(0, self.pending_orders_count - 1)
                    return
                
                # Dynamic leverage calculation (using AI suggestion as a target)
                if signal_type == "GATEKEEPER":
                    active_leverage = 5 # News trades use low leverage
                    logger.info(f"🛡️ [NEWS LEVERAGE] Using safe 5x for {symbol} News Trade.")
                elif is_black_swan:
                    base_leverage = self.calculate_dynamic_leverage(symbol, side, consensus_score, current_price, ai_leverage=ai_leverage)
                    active_leverage = min(20, base_leverage) # Hard cap at 20x for High Impact Events
                else:
                    active_leverage = self.calculate_dynamic_leverage(symbol, side, consensus_score, current_price, ai_leverage=ai_leverage)
                
                try:
                    market = self.exchange.market(symbol)
                    await self.exchange.set_leverage(active_leverage, market['id'])
                except Exception as e:
                    logger.error(f"Failed to set leverage for {symbol}: {e}")
                
                # Calculate size with AI strength modifier
                amount_to_buy = self._calculate_order_amount(symbol, current_price, custom_leverage=active_leverage, consensus_score=consensus_score, ai_strength=ai_strength)
                
                logger.info(f"🧠 [LLM STRATEGIC REVIEW] APPROVED {symbol} {side.upper()} (Lev: {active_leverage}x, Strength: {ai_strength:.2f}): {reason}")

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
            # --- WHALE & VELOCITY SCALPING LOGIC ---
            # Now using a mix of ATR and Fixed minimums for better protection
            if signal_type in ["WHALE", "VELOCITY_MOMENTUM", "VELOCITY_REVERSAL"]:
                sl_distance  = max(current_price * 0.015, current_atr * 3.0) # Min 1.5% or 3x ATR
                tp1_distance = max(current_price * 0.020, current_atr * 5.0) # Min 2.0% or 5x ATR
                tp2_distance = max(current_price * 0.050, current_atr * 10.0)
            elif signal_type in ["RECOVERY", "REJECTION"]:
                logger.warning(f"🛡️ RECOVERY SCALPING ACTIVE for {symbol}: 1.2% Partial TP Target.")
                sl_distance = current_price * 0.015 # 1.5% SL
                tp1_distance = current_price * 0.012 # 1.2% TP1
                tp2_distance = current_price * 0.030 # 3.0% TP2
            else:
                # Optimized Strategy: SL 2.0x, TP1 3.2x, TP2 6.5x (Safe ATR multipliers)
                # --- NEW: Volatility Shield SL (DeepSeek Refinement) ---
                if self.is_macro_paused:
                    sl_mult = 5.0
                    tp1_mult = 8.0
                    tp2_mult = 15.0
                    logger.warning(f"🛡️ [RISK MGMT] Macro Volatility Shield active: Widened SL/TP for {symbol}.")
                else:
                    sl_mult = 5.0 if is_black_swan else 3.0
                    tp1_mult = 7.0 if is_black_swan else 5.0
                    tp2_mult = 15.0 if is_black_swan else 10.0
                
                # --- APPLY AI MULTIPLIERS ---
                sl_mult *= ai_sl_mult
                tp1_mult *= ai_tp_mult
                tp2_mult *= ai_tp_mult
                
                sl_distance  = current_atr * sl_mult
                # Floor for Black Swans (1.2% as per user safety requirement)
                if is_black_swan:
                    min_sl = current_price * 0.012
                    sl_distance = max(sl_distance, min_sl)
                
                # Safety Clamp for SL (Max 10%)
                max_sl = current_price * 0.10
                if sl_distance > max_sl:
                    logger.warning(f"🛡️ [SL CLAMP] Reducing SL for {symbol} from {sl_distance/current_price:.2%} to 10.00%")
                    sl_distance = max_sl
                    
                tp1_distance = current_atr * tp1_mult
                tp2_distance = current_atr * tp2_mult
                
                # Safety Clamp for TP (Min 0.3% to avoid fee traps)
                min_tp = current_price * 0.003
                tp1_distance = max(tp1_distance, min_tp)
                
                if ai_sl_mult != 1.0 or ai_tp_mult != 1.0:
                    logger.info(f"🧠 [AI TP/SL] Applied AI Multipliers: SL={ai_sl_mult:.2f}x, TP={ai_tp_mult:.2f}x")
            
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
            
            # --- AI MEMORY SNAPSHOT ---
            snapshot_id = self.db.save_trade_snapshot(symbol, signal_type, 'long' if is_long else 'short', current_price, self.latest_data[symbol])

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
                'lowest_price': current_price if not is_long else 0,
                'ai_tp_price': ai_tp_price,
                'snapshot_id': snapshot_id
            }
            # Save and Log
            self.db.save_state("trade_levels", self.trade_levels)
            self.db.log_trade(symbol, side.upper(), current_price, amount_to_buy, 0, f"{signal_type} ({consensus_score:.2f})")
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
                
            # --- DYNAMIC MONITOR LIST (v4.1) ---
            # Monitor both curated symbols AND all active positions to ensure exit triggers work
            active_trade_symbols = [s for s, level in self.trade_levels.items() if level is not None]
            monitor_list = list(set(self.symbols) | set(active_trade_symbols))
            logger.info(f"🔍 [DYNAMIC MONITOR] Tracking {len(monitor_list)} symbols. Active trades: {active_trade_symbols}")
            
            # Esecuzione a "scaglioni" e con semaforo
            for i in range(0, len(monitor_list), 4):
                chunk = monitor_list[i:i+4]
                async with self.api_semaphore:
                    tasks = [self.fetch_data_and_analyze(symbol) for symbol in chunk]
                    await asyncio.gather(*tasks)
                await asyncio.sleep(2) # Increased sleep between chunks to be gentler
            
            # Sleep before fetching again
            await asyncio.sleep(10)

    def _calculate_unified_balance(self, balances):
        """Standardizes account data across different exchange API structures."""
        # Generic fallbacks from CCXT 'total'
        wallet_balance = float(balances.get('USDT', {}).get('total', 0)) + float(balances.get('USDC', {}).get('total', 0))
        equity = wallet_balance
        unrealized_pnl = 0.0
        margin_ratio = 0.0
        
        if 'info' in balances:
            info = balances['info']
            if self.active_exchange_name == "binance":
                wallet_balance = float(info.get('totalWalletBalance') or wallet_balance)
                equity = float(info.get('totalMarginBalance') or equity)
                unrealized_pnl = float(info.get('totalUnrealizedProfit') or 0.0)
                margin_ratio = float(info.get('marginRatio') or 0.0)
            elif self.active_exchange_name == "bitget":
                # Bitget V2 Structures (USDT-M)
                wallet_balance = float(info.get('totalWalletBalance') or wallet_balance)
                equity = float(info.get('totalEquity') or info.get('equity') or wallet_balance)
                unrealized_pnl = float(info.get('totalUnrealizedPL') or info.get('unrealizedPL') or 0.0)
                # Bitget margin ratio (maintMargin / equity)
                total_maint_margin = float(info.get('totalMaintMargin', 0))
                margin_ratio = float(info.get('marginRatio') or (total_maint_margin / equity if equity > 0 else 0))

        return wallet_balance, equity, unrealized_pnl, margin_ratio

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
                # --- ACCURATE BALANCE & EQUITY: Using Binance Native Fields if available ---
                balances = await self.exchange.fetch_balance()
                
                # --- UNIFIED BALANCE PARSING (Phase 5: Dual-Exchange) ---
                wallet_balance, equity, unrealized_pnl, self.current_margin_ratio = self._calculate_unified_balance(balances)
                
                # Check for other exchanges/fallbacks if info is missing
                if not balances.get('info'):
                    # Fallback for other exchanges: sum up position PnLs manually
                    # Note: We must be careful NOT to use balances['USDT']['total'] here
                    # as it might already include PnL on some exchanges depending on CCXT implementation.
                    pos_data = await self.exchange.fetch_positions()
                    unrealized_pnl = sum(float(p.get('unrealizedPnl', 0) or 0) for p in pos_data)
                    equity = wallet_balance + unrealized_pnl

                if self.initial_wallet_balance is None:
                    self.initial_wallet_balance = wallet_balance
                    logger.info(f"🏦 Account Balance Initialized: Balance={self.initial_wallet_balance}")
                
                # --- OVERHAUL: Circuit Breaker now also considers EQUITY DRAWDOWN (Unrealized) ---
                # This stops the "mess" by reacting to bleeding positions before they are closed.
                wallet_pnl_pct = (wallet_balance - self.initial_wallet_balance) / self.initial_wallet_balance if self.initial_wallet_balance > 0 else 0
                equity_pnl_pct = (equity - self.initial_wallet_balance) / self.initial_wallet_balance if self.initial_wallet_balance > 0 else 0
                
                # Use the worse of the two for the circuit breaker
                effective_pnl_pct = min(wallet_pnl_pct, equity_pnl_pct)

                if effective_pnl_pct <= -self.daily_loss_limit:
                    if not self.circuit_breaker_active:
                        logger.critical(f"🛑 CRITICAL: Daily loss limit reached ({effective_pnl_pct:.2%}). Activating Strategic Circuit Breaker (HIE Exceptions active).")
                        self.circuit_breaker_active = True
                
                # --- NEW: GLOBAL PANIC SELL ---
                if effective_pnl_pct <= -self.panic_drawdown_threshold:
                    if not self.global_panic_notified:
                        logger.critical(f"🆘 GLOBAL PANIC: Drawdown at {effective_pnl_pct:.2%}. CLOSING ALL POSITIONS.")
                        asyncio.create_task(self.emergency_cleanup_all())
                        self.circuit_breaker_active = True
                        self.global_panic_notified = True
                
                elif effective_pnl_pct > -0.03: # Reset if we recover or restart with better balance
                    if self.circuit_breaker_active:
                        logger.warning(f"🟢 Circuit Breaker Reset: Current loss at {effective_pnl_pct:.2%}")
                        self.circuit_breaker_active = False
                        self.global_panic_notified = False # Reset flag when circuit breaker is reset

                logger.info(f"Account Update: Wallet={wallet_balance:.4f}, Equity={equity:.4f}, PnL={unrealized_pnl:.4f} (Eff: {effective_pnl_pct:.2%})")
                
                # --- DANGER CHECK ---
                
                if self.current_margin_ratio > 0.95: # 95% Margin Usage is DANGEROUS
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
                            asyncio.create_task(self.close_position(worst_symbol, trade_to_close, reason="CIRCUIT BREAKER"))
                
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
                    # Filter: Only sync trades happened after bot start to ensure clean report from scratch
                    recent_only = [t for t in all_trades if (t.get('timestamp', 0) / 1000.0) >= self.start_time]
                    if recent_only:
                        await asyncio.to_thread(self.db.sync_binance_trades, recent_only)
                        logger.info(f"Account Update: Synced {len(recent_only)} NEW trades after restart.")
                    
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
                # Log manual/emergency closure
                self.notifier.notify_trade(symbol, "CLOSE", current_price, amount, "CIRCUIT BREAKER")
            
            self.db.save_state("trade_levels", self.trade_levels)
            logger.info("✅ All positions closed successfully.")
            self.notifier.notify_alert("CRITICAL", "VENDITA DI EMERGENZA (GLOBAL PANIC)", "Tutte le posizioni sono state chiuse a causa del drawdown eccessivo.")
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
            self.global_panic_notified = False
            logger.warning(f"🟢 MANUAL RESET: Circuit Breaker cleared. New base balance: {wallet_balance:.2f} USDT")
            return True
        except Exception as e:
            logger.error(f"Failed to reset circuit breaker: {e}")
            return False

    async def refresh_symbols(self):
        """
        Scans all Binance USDT-M markets and updates the active symbol list based on volume and volatility.
        """
        try:
            # 1. Get high-potential candidates (volume/volatility filtered)
            # Reduce candidate volume (100 -> 50) for cost optimization
            scored_assets = await self.scanner.get_top_performing_assets(limit=50)
            
            # 2. AI Refinement: Let Gemini pick the Final 50
            new_symbols = await self.analyst.refine_market_selection(scored_assets, limit=50)
            
            # --- NEW: STICKY SYMBOLS PROTECTION (v3.5) ---
            # Ensure any symbol with an active position is KEPT in the list
            active_symbols_in_trade = [s for s, level in self.trade_levels.items() if level is not None]
            
            # Merge new_symbols with active_symbols_in_trade, ensuring uniqueness
            final_symbols = list(set(new_symbols) | set(active_symbols_in_trade))
            
            added = set(final_symbols) - set(self.symbols)
            removed = set(self.symbols) - set(final_symbols)
            
            # Explicitly log if we are keeping a symbol because it has an active position
            for s in active_symbols_in_trade:
                if s not in new_symbols:
                    logger.warning(f"🛡️ [STICKY SYMBOL] Preserving {s} in monitor list due to active position.")

            if not added and not removed:
                logger.info("♻️ Symbol list is already optimal. No rotation needed.")
                return
            
            logger.warning(f"♻️ ROTATING SYMBOLS: +{len(added)} added, -{len(removed)} removed")
            
            # Update internal dictionaries for NEW symbols
            for s in added:
                if s not in self.latest_data:
                    self.latest_data[s] = {
                        'price': 0.0, 'change': 0.0, 'changePercent': 0.0, 'rsi': 0.0, 
                        'macd': 0.0, 'macd_hist': 0.0, 'bb_upper': 0.0, 'bb_lower': 0.0,
                        'imbalance': 1.0, 'atr': 0.0, 'ema200': 0.0, 'volume24h': 0.0,
                        'prediction': "Syncing..."
                    }
                if s not in self.price_windows:
                    self.price_windows[s] = deque(maxlen=30)
                if s not in self.price_history_2m:
                    self.price_history_2m[s] = deque(maxlen=12)
                if s not in self.hft_locks:
                    self.hft_locks[s] = 0.0
                if s not in self.active_positions:
                    self.active_positions[s] = None
                if s not in self.trade_levels:
                    self.trade_levels[s] = None
                if s not in self.ml_cooldowns:
                     self.ml_cooldowns[s] = 0.0

            # Apply new list
            self.symbols = final_symbols
            
            # Set leverage for NEW symbols
            await self._set_leverage_for_all()
            logger.info("✅ Data structures and leverage updated for new symbols.")
            
        except Exception as e:
            logger.error(f"❌ Error during symbol refresh: {e}")

    async def sync_zombie_positions(self):
        """
        Scans all open positions on Binance and ensures they are tracked in trade_levels.
        If a position is found on exchange but NOT in trade_levels, it re-initializes it.
        """
        try:
            positions = await self.exchange.fetch_positions()
            active_pos = [p for p in positions if float(p.get('contracts', 0) or 0) != 0]
            
            for pos in active_pos:
                symbol = pos['symbol']
                if self.trade_levels.get(symbol) is None:
                    logger.warning(f"🧟 [ZOMBIE FOUND] Rediscovered untracked position for {symbol}. Restoring monitoring...")
                    
                    # Ensure symbol is in our monitor list
                    if symbol not in self.symbols:
                        self.symbols.append(symbol)
                        # Initialize structures for this symbol if missing
                        if symbol not in self.latest_data:
                            self.latest_data[symbol] = {'price': float(pos.get('markPrice', 0)), 'prediction': 'Syncing...'}
                        if symbol not in self.trade_levels:
                            self.trade_levels[symbol] = None

                    # Reconstruct a basic trade state
                    side = pos['side'].lower()
                    entry_price = float(pos['entryPrice'])
                    current_price = float(pos.get('markPrice', entry_price))
                    amount = float(pos['contracts'])
                    
                    # Initialize trade levels with a safe emergency SL (3% from current price or 5% from entry)
                    # We use a broad SL to avoid instant closure if the user wants to manage it, 
                    # but ensure SOME protection.
                    self.trade_levels[symbol] = {
                        'entry_price': entry_price,
                        'sl': current_price * (1.03 if side == 'short' else 0.97),
                        'tp1': current_price * (0.95 if side == 'short' else 1.05),
                        'tp2': current_price * (0.90 if side == 'short' else 1.10),
                        'side': 'long' if side == 'long' else 'short',
                        'amount': amount,
                        'status': 'RECOVERED_ZOMBIE'
                    }
                    logger.warning(f"🛡️ [RECOVERY] {symbol} restored with emergency SL @ {self.trade_levels[symbol]['stop_loss']}")
            
            # Save state to DB
            self.db.save_state("trade_levels", self.trade_levels)
            
        except Exception as e:
            logger.error(f"❌ Error during zombie synchronization: {e}")

    async def monitor_position_health_loop(self):
        """Background task to periodically evaluate active positions with AI."""
        await asyncio.sleep(600) # Initial 10m wait to let things settle
        while True:
            try:
                active_symbols = [s for s, level in self.trade_levels.items() if level is not None]
                if active_symbols:
                    logger.info(f"🔍 [AI AUDIT] Periodically reviewing {len(active_symbols)} active positions...")
                    for symbol in active_symbols:
                        await self._audit_single_position(symbol)
                        await asyncio.sleep(5) # Rate limit protection
            except Exception as e:
                logger.error(f"Error in position health loop: {e}")
            await asyncio.sleep(1800) # Run every 30 minutes

    async def _audit_single_position(self, symbol):
        """Perform a single AI-driven audit of an active position."""
        level = self.trade_levels.get(symbol)
        if not level: return

        side = level['side']
        entry_price = level['entry_price']
        current_price = self.latest_data[symbol].get('price', entry_price)
        
        # Calculate Current PnL %
        if side == 'long':
            pnl_pct = (current_price - entry_price) / entry_price * 100
        else:
            pnl_pct = (entry_price - current_price) / entry_price * 100

        indicators = self.latest_data.get(symbol, {})
        action, confidence, reason = await self.analyst.evaluate_active_position(symbol, side, indicators, pnl_pct)

        if action == "HOLD":
            return
        
        logger.warning(f"🧠 [AI AUDIT] Action for {symbol}: {action} ({confidence:.2f}) | Reason: {reason}")
        
        if action == "SCALE_OUT":
            await self._execute_scale_out(symbol)
        elif action == "PIVOT":
            await self._execute_pivot(symbol)
        elif action == "CLOSE":
            await self.close_position(symbol, level, reason="AI AUDIT")
            
    async def _execute_scale_out(self, symbol):
        """Close 50% of an active position to lock in profits or reduce risk."""
        level = self.trade_levels.get(symbol)
        if not level: return
        
        amount_to_close = level['amount'] / 2
        side = level['side']
        close_side = 'sell' if side == 'long' else 'buy'
        
        logger.warning(f"✂️ [SCALE OUT] Closing 50% of {symbol} ({amount_to_close} {side.upper()})")
        try:
            order = await self.exchange.create_order(symbol, 'market', close_side, amount_to_close, params={'reduceOnly': True})
            # Update local state
            level['amount'] -= amount_to_close
            self.db.save_state("trade_levels", self.trade_levels)
            self.notifier.notify_alert("AI SCALING", f"Venduta metà posizione su {symbol}", f"Motivo: {level.get('status', 'Monitoraggio standard')}")
        except Exception as e:
            logger.error(f"Failed to scale out of {symbol}: {e}")

    async def _execute_pivot(self, symbol):
        """Close current position and immediately open in opposite direction."""
        level = self.trade_levels.get(symbol)
        if not level: return
        
        side = level['side']
        opp_side = 'long' if side == 'short' else 'short'
        
        logger.warning(f"🔄 [PIVOT] Reversing position for {symbol} (from {side.upper()} to {opp_side.upper()})")
        # 1. Close current
        await self.exchange.create_order(symbol, 'market', 'sell' if side == 'long' else 'buy', level['amount'], params={'reduceOnly': True})
        # 2. Reset and allow new signal (or force new entry)
        self.active_positions[symbol] = None
        self.trade_levels[symbol] = None
        # We don't force entry here, we let the SignalManager pick it up on the next tick, 
        # but with the 'PIVOT' sentiment it will likely trigger immediately.

    async def refresh_symbols_loop(self):
        """Background task to refresh symbols periodically."""
        # Initial wait to let bot settle after start
        await asyncio.sleep(300) 
        while True:
            try:
                logger.info("♻️ [SCHEDULED] Starting periodic symbol refresh and AI curation...")
                await self.refresh_symbols()
            except Exception as e:
                logger.error(f"Error in refresh_symbols_loop: {e}")
            await asyncio.sleep(14400) # Refresh every 4 hours (Optimized for cost/efficiency)

    async def weekly_self_audit_loop(self):
        """Background task to perform AI self-audit every Sunday."""
        while True:
            try:
                # Run audit on Sundays (day 6)
                if time.localtime().tm_wday == 6:
                    logger.warning("🧠 [AI AUDIT] Initiating weekly self-correction cycle...")
                    # Get closed trades from DB
                    history = self.db.get_all_trades(limit=50)
                    await self.analyst.perform_self_audit(history)
                    
                # Check every 24 hours
                await asyncio.sleep(86400)
            except Exception as e:
                logger.error(f"Error in weekly audit loop: {e}")
                await asyncio.sleep(3600)

    async def news_sentiment_loop(self):
        """Fetches crypto news from RSS feeds to provide context to Gemini."""
        feeds = [
            "https://cryptopanic.com/news/rss/",
            "https://www.coindesk.com/arc/outboundfeeds/rss/"
        ]
        while True:
            try:
                news_items = []
                for url in feeds:
                    feed = await asyncio.to_thread(feedparser.parse, url)
                    for entry in feed.entries[:5]: # Take top 5 news
                        news_items.append(entry.title)
                
                # Format a single string for Gemini
                self.current_news = " | ".join(news_items)
                logger.info(f"📰 [NEWS] Captured {len(news_items)} top headlines.")
                
            except Exception as e:
                logger.error(f"Error fetching news: {e}")
            
            await asyncio.sleep(3600) # Update news every hour
