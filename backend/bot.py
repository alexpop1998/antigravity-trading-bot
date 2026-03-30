import ccxt.async_support as ccxt
import pandas as pd
import asyncio
import traceback
import logging
from dotenv import load_dotenv
import os
import json
import time
from collections import deque, defaultdict
from ml_predictor import MLPredictor
from database import BotDatabase
from telegram_notifier import TelegramNotifier
from signal_manager import SignalManager
from llm_analyst import LLMAnalyst
from sector_manager import SectorManager
import feedparser
import generate_report
from datetime import datetime
from typing import Any, Dict, List, Optional
from asset_scanner import AssetScanner
from regime_detector import RegimeDetector
from ai_parameter_optimizer import AIParameterOptimizer

load_dotenv(override=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("TradingBot")
class CryptoBot:
    def __init__(self, config_file=None):
        # Load SaaS Client Risk Profiles
        load_dotenv(override=True)
        if config_file is None:
            profile = os.getenv("CONFIG_PROFILE", "aggressive").lower()
            config_file = f"config_{profile}.json"
            logger.warning(f"🎯 [INIT] Loading Active Profile: {profile.upper()} ({config_file})")
            
        self.config_file = config_file
        self.risk_profile = self._load_risk_profile()
        
        params = self.risk_profile.get("trading_parameters", {})
        self.symbols = params.get("symbols", ["BTC/USDT"])
        self.timeframe = params.get("timeframe", "15m")
        self.rsi_period = params.get("rsi_period", 14)
        self.leverage = params.get("leverage", 10)
        self.usdt_per_trade = params.get("usdt_per_trade", 100)
        self.percent_per_trade = params.get("percent_per_trade", 2.5) # % of equity per trade
        self.max_margin_pct = params.get("max_margin_pct", 15.0) # Absolute cap for AI boosting
        self.stop_loss_pct = params.get("stop_loss_pct", 0.02)
        self.take_profit_pct = params.get("take_profit_pct", 0.06)
        self.max_concurrent_positions = params.get("max_concurrent_positions", 20)
        self.max_global_margin_ratio = params.get("max_global_margin_ratio", 0.75)
        self.daily_loss_limit = float(params.get("daily_loss_limit", 1.0)) / 100.0
        self.panic_drawdown_threshold = self.daily_loss_limit * 2.5 # Panic selling at 2.5x the daily limit
        if self.panic_drawdown_threshold < 0.05:
            self.panic_drawdown_threshold = 0.05 # Minimum 5% hard ceiling
        self.gemini_min_confidence = float(params.get("gemini_min_confidence", 0.90))
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
        
        # --- NEW: SHADOW MODE DATA FETCHER (Binance Mainnet for Clean Signals) ---
        try:
            self.data_fetcher = ccxt.binanceusdm({
                'enableRateLimit': True,
                'options': {'defaultType': 'swap'},
                'timeout': 10000,
            })
            # load_markets() will be called in initialize()
        except Exception as e:
            logger.error(f"❌ Failed to initialize Shadow Mode Fetcher: {e}")
            self.data_fetcher = self.exchange
        
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
        
        # --- SHADOW SCANNER (v9.6) ---
        # Ensure scanner looks at REAL Mainnet data even on Testnet
        scan_provider = self.data_fetcher if hasattr(self, 'data_fetcher') else self.exchange
        self.scanner = AssetScanner(scan_provider)
        
        # Resource management
        self.ml_semaphore = asyncio.Semaphore(1) 
        self.api_semaphore = asyncio.Semaphore(5) 
        
        # Risk Management & Guards
        self.circuit_breaker_active = False
        self.global_panic_notified = False
        self.symbol_blacklist = set()
        self.initial_wallet_balance = None
        self.previous_prices: Dict[str, float] = defaultdict(float)
        
        # --- NEW: Time-Domain Lock (DeepSeek Refinement) ---
        self.hft_locks: Dict[str, float] = defaultdict(float)
        
        # v10.4: Hard Session Lockdown Start Time
        self.session_start_time = time.time()
        saved_start = self.db.load_state("session_start_time")
        if saved_start:
            self.session_start_time = float(saved_start)
            logger.info(f"⏳ Session Lockdown attivo da: {datetime.fromtimestamp(self.session_start_time).strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            self.db.save_state("session_start_time", self.session_start_time)

        # Legacy start_time for compatibility
        self.start_time = self.session_start_time 
        
        # 5-minute rolling window
        self.price_windows: Dict[str, deque] = defaultdict(lambda: deque(maxlen=30))
        # 2-minute rolling window for VeloCity Emergency Exit
        self.price_history_2m: Dict[str, deque] = defaultdict(lambda: deque(maxlen=12))
        
        self.min_leverage = 2
        self.max_leverage = 25
        
        self.sector_manager = SectorManager()
        self.regime_detector = RegimeDetector()
        self.ai_optimizer = AIParameterOptimizer(self)
        self.current_news = "" # Real-time market sentiment
        
        # Recupero stato persistente
        saved_levels = self.db.load_state("trade_levels")
        if saved_levels:
            self.trade_levels = saved_levels
            logger.info("📦 Stato trade_levels recuperato correttamente.")
        
        saved_balance = self.db.load_state("initial_balance")
        if saved_balance:
            self.initial_wallet_balance = float(saved_balance)
            logger.info(f"🏦 Bilancio iniziale recuperato dal database: {self.initial_wallet_balance:.2f} USDT")
        
        self.initialized = False

    def _get_data_provider(self, symbol):
        """Returns the data provider (Mainnet or Testnet) for a given symbol."""
        if hasattr(self, 'data_fetcher') and self.data_fetcher is not None:
            # Normalize symbol to strip exchange-specific suffixes like :USDT for matching
            normalized_symbol = symbol.split(':')[0] if ':' in symbol else symbol
            
            try:
                # Check if symbol exists in Mainnet markets (primary or normalized)
                if symbol in self.data_fetcher.markets or normalized_symbol in self.data_fetcher.markets:
                    return self.data_fetcher
            except:
                pass
        return self.exchange

    async def initialize(self, skip_leverage=False):
        if self.initialized:
            return
            
        try:
            logger.info("📡 Loading markets from Binance (Async)...")
            await self.exchange.load_markets()
            
            if hasattr(self, 'data_fetcher'):
                logger.info("🛡️ [Shadow Mode] Loading Real Binance Markets...")
                await self.data_fetcher.load_markets()
                mainnet_symbols = list(self.data_fetcher.markets.keys())
                self.scanner.set_allowed_symbols(mainnet_symbols)
                logger.info(f"✅ Shadow Mode Active: {len(self.data_fetcher.markets)} real symbols validated and synced with Scanner.")
                
            logger.info("✅ Markets loaded successfully.")
            
            # --- DYNAMIC SYMBOL BRIDGE ---
            # Maps human-readable config symbols to exchange-specific ones (e.g. BTC/USDT -> BTCUSDT)
            self.symbols = self._apply_symbol_bridge(self.symbols)
            
            # Filter for Shadow Mode (Mainnet check)
            if hasattr(self, 'data_fetcher'):
                available = []
                for s in self.symbols:
                    if s in self.data_fetcher.markets:
                        available.append(s)
                    else:
                        logger.warning(f"🛡️ [Shadow Mode] Filtering out {s}: Not on Mainnet.")
                self.symbols = available
            
            if not self.symbols:
                logger.error("❌ NO VALID SYMBOLS FOUND ON THIS EXCHANGE!")
            
            logger.info(f"🎯 Validated {len(self.symbols)} active symbols.")
            
            # --- [FIX] SYMBOL STATE SYNCHRONIZATION (v9.8.5) ---
            # Migration: Preserve trade memory when symbols are bridged (e.g. HEMI/USDT -> HEMI/USDT:USDT)
            migrated_count = 0
            for s in self.symbols:
                # 1. Detect and Migrate Keys
                base_symbol = s.split(':')[0] if ':' in s else s
                unslashed = base_symbol.replace('/', '')
                
                # Check for various formats in existing trade_levels (loaded from DB)
                potential_old_keys = [base_symbol, unslashed, base_symbol.lower(), s.lower()]
                for old_key in potential_old_keys:
                    if old_key != s and old_key in self.trade_levels and self.trade_levels[old_key] is not None:
                        logger.warning(f"📦 [MEMORY RECOVERY] Migrating levels for {s} from old key {old_key}.")
                        self.trade_levels[s] = self.trade_levels[old_key]
                        # Don't delete the old key yet to stay robust, but prioritize 's'
                        migrated_count += 1
                        break

                # 2. Default Initialization if still missing
                if s not in self.latest_data:
                    self.latest_data[s] = {
                        'price': 0.0, 'change': 0.0, 'changePercent': 0.0, 'rsi': 0.0, 
                        'macd': 0.0, 'macd_hist': 0.0, 'bb_upper': 0.0, 'bb_lower': 0.0,
                        'imbalance': 1.0, 'atr': 0.0, 'ema200': 0.0, 'volume24h': 0.0,
                        'prediction': "Syncing..."
                    }
                if s not in self.active_positions: self.active_positions[s] = None
                if s not in self.trade_levels: self.trade_levels[s] = None
                if s not in self.ml_cooldowns: self.ml_cooldowns[s] = 0.0
                if s not in self.llm_cooldowns: self.llm_cooldowns[s] = 0.0
                if s not in self.last_llm_side: self.last_llm_side[s] = ""
                if s not in self.previous_prices: self.previous_prices[s] = 0.0
                if s not in self.hft_locks: self.hft_locks[s] = 0.0
                if s not in self.price_windows: self.price_windows[s] = deque(maxlen=30)
                if s not in self.price_history_2m: self.price_history_2m[s] = deque(maxlen=12)

            if migrated_count > 0:
                self.db.save_state("trade_levels", self.trade_levels)
                logger.info(f"✅ [MEMORY] {migrated_count} symbols successfully migrated to primary bridged keys.")
            
            # --- NEW: BLOCKING BALANCE FETCH (v9.7) ---
            # Force the bot to wait for the real account balance before starting
            logger.info("🏦 [SYNC] Waiting for real-time account balance...")
            await self._update_account_state()
            if self.latest_account_data.get('equity', 0) > 0:
                logger.info(f"✅ [SYNC] Capital Loaded: ${self.latest_account_data['equity']:.2f}")
            else:
                logger.warning("⚠️ [SYNC] Initial balance fetch returned 0. Retrying in background...")
            
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
            asyncio.create_task(self.daily_self_audit_loop())
            asyncio.create_task(self.news_sentiment_loop())
            asyncio.create_task(self.automated_report_loop())
            
            # 2. [TEMP DISABLED] Snipping/Arbitrage Loop (For Bitget future)
            # self.dex_sniper = DEXSniper(self)
            # asyncio.create_task(self.dex_sniper.monitor_arbitrage())
            
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
            provider = self._get_data_provider(symbol)
            ohlcv_4h = await provider.fetch_ohlcv(symbol, '4h', limit=20)
            df_4h = pd.DataFrame(ohlcv_4h, columns=['t', 'o', 'h', 'l', 'c', 'v'])
            ema200_4h = df_4h['c'].ewm(span=200, adjust=False).mean().iloc[-1] if len(df_4h) > 5 else 0
            current_4h = df_4h['c'].iloc[-1]
            trend_4h = "BULLISH" if current_4h > ema200_4h else "BEARISH"
            
            # 1d Trend
            provider = self._get_data_provider(symbol)
            ohlcv_1d = await provider.fetch_ohlcv(symbol, '1d', limit=10)
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
            provider = self._get_data_provider(symbol)
            ohlcv = await provider.fetch_ohlcv(symbol, self.timeframe, limit=100)
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

            # --- NEW: MARKET REGIME DETECTION (ADX & DONCHIAN) ---
            regime, multiplier = self.regime_detector.detect_regime(df)
            current_adx = df['adx'].iloc[-1] if 'adx' in df.columns else 0
            
            # Store in latest data
            if symbol not in self.latest_data:
                self.latest_data[symbol] = {}
                
            self.latest_data[symbol]['regime'] = regime
            self.latest_data[symbol]['regime_multiplier'] = multiplier
            
            # --- Order Book & Liquidity Analysis ---
            provider = self._get_data_provider(symbol)
            orderbook = await provider.fetch_order_book(symbol, limit=100)
            bids = orderbook['bids']
            asks = orderbook['asks']
            
            # 1. Volume Imbalance (Total 100 levels)
            total_bids = sum(float(bid[1]) for bid in bids)
            total_asks = sum(float(ask[1]) for ask in asks)
            imbalance = total_bids / total_asks if total_asks > 0 else 1.0
            
            # 2. Bid/Ask Spread %
            best_bid = float(bids[0][0]) if bids else current_close
            best_ask = float(asks[0][0]) if asks else current_close
            spread_pct = (best_ask - best_bid) / best_bid if best_bid > 0 else 0
            
            # 3. Depth at 1% (Sum of amount within 1% of mid-price)
            mid_price = (best_bid + best_ask) / 2
            bid_depth_1pct = sum(float(bid[1]) for bid in bids if float(bid[0]) >= mid_price * 0.99)
            ask_depth_1pct = sum(float(ask[1]) for ask in asks if float(ask[0]) <= mid_price * 1.01)
            
            # --- [PRIORITY 1] SAFETY AUDIT (v9.8.6) ---
            # Move SL/TP check to the VERY BEGINNING to protect capital even if analysis crashes
            self._check_soft_stop_loss(symbol, current_close)
            
            # --- [PRIORITY 2] VOLATILITY & VELOCITY (2m Anti-Ruin) ---
            change_5m_pct = 0
            prev_price = self.previous_prices.get(symbol, current_close)
            window = self.price_windows[symbol]
            window.append(current_close)
            
            if len(window) >= 2:
                reference_price = window[0]
                change_5m_pct = (current_close - reference_price) / reference_price
                
                # --- [SUB-MODULE] FLASH DETECTION ---
                current_candle_vol = float(df['volume'].iloc[-1])
                avg_recent_vol = float(df['volume'].rolling(window=10).mean().iloc[-2]) if len(df) > 10 else 1.0
                is_high_volume = current_candle_vol > (avg_recent_vol * 1.5)
                
                if change_5m_pct < -0.012:
                    logger.info(f"⚠️ [FLASH] Potential DUMP detected for {symbol} ({change_5m_pct:.2%} in 5m)")
                    asyncio.create_task(self.handle_signal(symbol, "EVENT_DUMP", "sell", weight_modifier=1.0))
                elif change_5m_pct > 0.012:
                    logger.info(f"⚠️ [FLASH] Potential PUMP detected for {symbol} ({change_5m_pct:.2%} in 5m)")
                    asyncio.create_task(self.handle_signal(symbol, "EVENT_PUMP", "buy", weight_modifier=1.0))

                # --- [SUB-MODULE] VELOCITY EXIT ---
                history_2m = self.price_history_2m[symbol]
                history_2m.append(current_close)
                if len(history_2m) >= 6:
                    ref_2m = history_2m[0]
                    change_2m_pct = (current_close - ref_2m) / ref_2m
                    
                    atr_val = current_atr if isinstance(current_atr, (int, float)) else (current_close * 0.015)
                    atr_pct = (atr_val / current_close) if current_close > 0 else 0.015
                    sos_threshold = max(0.012, min(0.040, atr_pct * 1.5))
                    
                    active_pos = self.active_positions.get(symbol)
                    if active_pos:
                        abs_change = abs(change_2m_pct)
                        is_against = (active_pos == 'LONG' and change_2m_pct < 0) or (active_pos == 'SHORT' and change_2m_pct > 0)
                        if is_against and abs_change > (sos_threshold * 0.7):
                            if abs_change >= (sos_threshold * 1.5):
                                logger.critical(f"🆘 [HARD VELOCITY EXIT] Closing {active_pos} {symbol}: Panic Spike ({change_2m_pct:.2%}).")
                                asyncio.create_task(self.close_position(symbol, self.trade_levels[symbol]))
                            else:
                                self.llm_cooldowns[symbol] = 0 
                    else:
                        if change_2m_pct > (sos_threshold * 1.2):
                            asyncio.create_task(self.handle_signal(symbol, "VELOCITY_MOMENTUM", "buy", weight_modifier=1.2, current_price=current_close, ema200=current_ema200))
                        elif change_2m_pct < -(sos_threshold * 1.2):
                            asyncio.create_task(self.handle_signal(symbol, "VELOCITY_MOMENTUM", "sell", weight_modifier=1.2, current_price=current_close, ema200=current_ema200))

            # Store price for next cycle
            self.previous_prices[symbol] = current_close

            # --- [PRIORITY 3] ENRICHED ANALYSIS (Wrapped to prevent stalling) ---
            try:
                # 1. Ticker Data
                provider = self._get_data_provider(symbol)
                ticker = await provider.fetch_ticker(symbol)
                quote_volume = float(ticker.get('quoteVolume', 0))
                
                # 2. ML Prediction (Cooldowned)
                current_time = time.time()
                if current_time - self.ml_cooldowns.get(symbol, 0) > 1800:
                    try:
                        async with self.ml_semaphore:
                            predicted_price = await asyncio.to_thread(self.predictor.train_and_predict, symbol, df)
                    except Exception as mle:
                        logger.error(f"⚠️ ML Training failed for {symbol}: {mle}")
                        predicted_price = "ML Error"
                    self.ml_cooldowns[symbol] = current_time
                else:
                    predicted_price = self.latest_data.get(symbol, {}).get('prediction', "Syncing...")

                # 3. Sentiment & Institutional
                current_oi, current_ls = 0, 1.0
                try:
                    sentiment_api = getattr(self, 'data_fetcher', self.exchange)
                    oi_data = await sentiment_api.fetch_open_interest(symbol)
                    current_oi = float(oi_data.get('openInterestAmount', 0))
                    
                    if not os.getenv('BINANCE_SANDBOX', 'false').lower() == 'true' or hasattr(self, 'data_fetcher'):
                        ls_data = await sentiment_api.fapiDataGetGlobalLongShortAccountRatio({'symbol': symbol.replace('/', '').split(':')[0], 'period': '5m', 'limit': 1})
                        current_ls = float(ls_data[0]['longShortRatio']) if ls_data else 1.0
                except: pass

                funding = await provider.fetch_funding_rate(symbol)
                funding_rate = float(funding.get('fundingRate', 0))

                # Update latest data record
                self.latest_data[symbol] = {
                    'price': current_close,
                    'rsi': round(current_rsi, 2) if not pd.isna(current_rsi) else "N/A",
                    'prediction': predicted_price,
                    'imbalance': round(imbalance, 2),
                    'volume24h': quote_volume,
                    'funding_rate': funding_rate,
                    'open_interest': current_oi,
                    'ls_ratio': current_ls
                }

                # 4. Final Strategy Audit
                self._check_trading_signals(symbol, current_close, current_rsi, current_macd_hist, current_bb_lower, current_bb_upper, imbalance, current_ema200, quote_volume, current_atr, current_adx, mtf_context, change_5m_pct)
            except Exception as e:
                logger.error(f"⚠️ Analysis Module failed for {symbol}: {e}")
                
        except Exception as e:
            logger.error(f"FATAL error profiling {symbol}: {e}")

    def _check_trading_signals(self, symbol, price, rsi, macd_hist, bb_lower, bb_upper, imbalance, ema200, volume24h, atr, adx, mtf_context, change_5m_pct):
        if pd.isna(rsi) or pd.isna(macd_hist) or pd.isna(bb_lower) or pd.isna(ema200):
            return
            
        if self.is_macro_paused:
            return

        # Normalize BTC lookup
        if "BTC/USDT" in self.latest_data and symbol.split(':')[0] != "BTC/USDT":
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
        
        # --- STRONGER TECHNICAL CONFLUENCE: RSI + MACD Hist + Bollinger ---
        if rsi < 30 and macd_hist > -0.0001 and macd_hist > self.latest_data[symbol].get('macd_hist_prev', -1) and price <= (bb_lower * 1.01):
            if price_above_ema and is_trending: # Trend-following LONG
                tech_side = 'buy'
                is_technical_signal = True
            
        elif rsi > 70 and macd_hist < 0.0001 and macd_hist < self.latest_data[symbol].get('macd_hist_prev', 1) and price >= (bb_upper * 0.99):
            # --- SHORTING MOMENTUM GUARD ---
            # Don't short if there is strong bullish momentum in the last 5 minutes
            if change_5m_pct > 0.008: # 0.8% in 5m is too strong to short technically
                logger.info(f"🛡️ [SHORT GUARD] Skipping technical SHORT on {symbol}: Momentum too bullish ({change_5m_pct:.2%})")
            elif not price_above_ema and is_trending: # Trend-following SHORT
                tech_side = 'sell'
                is_technical_signal = True

        # Store hist for next cycle confluence
        self.latest_data[symbol]['macd_hist_prev'] = macd_hist

        # --- FUNDING RATE FILTER (DEEPSEEK REFACTOR) ---
        # Instead of blocking, apply a reduction multiplier
        current_funding = self.latest_data.get(symbol, {}).get('funding_rate', 0)
        threshold = self.risk_profile.get("trading_parameters", {}).get("funding_filter_threshold", 0.0005)
        funding_multiplier = 1.0
        
        if tech_side == 'buy' and current_funding > threshold:
            funding_multiplier = 0.5
            logger.warning(f"⚠️ [FUNDING PENALTY] High funding on {symbol} ({current_funding*100:.3f}%). Reducing size by 50%.")
        elif tech_side == 'sell' and current_funding < -threshold:
            funding_multiplier = 0.5
            logger.warning(f"⚠️ [FUNDING PENALTY] Negative funding on {symbol} ({current_funding*100:.3f}%). Reducing size by 50%.")
            
        self.latest_data[symbol]['funding_multiplier'] = funding_multiplier

        # --- MAINNET VALIDITY FILTER (SHADOW MODE) ---
        if hasattr(self, 'data_fetcher') and self.data_fetcher.markets:
            if symbol not in self.data_fetcher.markets:
                logger.warning(f"🚫 [Shadow Filter] Ignoring {symbol}: Not present on live Binance Mainnet (Testnet phantom).")
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
            # Confidence Threshold: 0.70 (Increased from 0.55 for higher quality)
            if direction == 1 and confidence > 0.70 and price_above_ema:
                asyncio.create_task(self.handle_signal(symbol, "AI", "buy", weight_modifier=1.2, current_price=price, ema200=ema200, ai_confidence=confidence, mtf_context=mtf_context))
            elif direction == 0 and confidence > 0.70 and not price_above_ema:
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
            if type in ["LIQUIDATION", "DEX_ARBITRAGE", "NEW_LISTING"]:
                self.hft_locks[symbol] = now
                logger.warning(f"🔒 [TIME-DOMAIN LOCK] Lock ACTIVE for {symbol} (60s).")

            # --- MASTER CONTROLLER: NET EXPOSURE & PRIORITY ---
            existing_pos = self.active_positions.get(symbol)
            if existing_pos:
                # 1. Prevent conflicting positions (Long vs Short)
                existing_side = 'buy' if existing_pos == 'LONG' else 'sell'
                if existing_side != side.lower():
                    # --- REVERSAL PRIORITY ---
                    # If it's a Black Swan, Gatekeeper, high-priority event, or strong AI reversal
                    reversal_signals = ["GATEKEEPER", "NEWS", "NEW_LISTING", "EVENT_PUMP", "EVENT_DUMP", "LIQUIDATION"]
                    if is_black_swan or type in reversal_signals or (type == "AI" and ai_confidence > 0.85):
                        if not self._is_startup_shield_active():
                            logger.critical(f"🆘 [REVERSAL] Emergency Flip for {symbol}: Closing {existing_pos} for {side.upper()} {type}")
                            # Fetch current level to ensure we close the right amount
                            current_level = self.trade_levels.get(symbol)
                            await self.close_position(symbol, current_level, reason=f"REVERSAL_{type}")
                        else:
                            logger.info(f"🛡️ [Shield] Skipping Emergency Reversal for {symbol} (Startup Protection active)")
                            return
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
        
        # Technical Stop: Multiplier based on TP stage
        # USER REQUEST: Institutional SL - Wider to avoid noise
        # Normal SL (Pre-TP1): 3.5x ATR
        # TP2 Trailing (Post-TP1): 5x ATR (Wider "Largo")
        tech_multiplier = 5.0 if trade.get('tp1_hit', False) else 3.5
        survival_multiplier = 7.0 # Survival ceiling
        
        latest = self.latest_data.get(symbol, {})
        current_atr = latest.get('atr')
        
        if isinstance(current_atr, (float, int)) and current_atr > 0:
            sl_distance = float(current_atr) * tech_multiplier
            survival_sl_dist = float(current_atr) * survival_multiplier
        else:
            sl_distance = float(trade.get('sl_distance', current_price * self.stop_loss_pct))
            survival_sl_dist = sl_distance * 2.5
            
        # --- [CRITICAL] INSTITUTIONAL SL FLOOR ---
        # Never closer than 1.5% from entry to avoid paper hands / noise
        # We only apply the floor before TP1 is hit to give initial breathing room
        if not trade.get('tp1_hit', False):
            min_sl_dist = current_price * 0.015
            if sl_distance < min_sl_dist:
                 sl_distance = min_sl_dist
                 logger.debug(f"📏 [SL GUARD] Enforcing institutional floor (1.5%) for {symbol}")
            
        # Floor for Black Swans (at least 2.0% away to avoid noise)
        if trade.get('is_black_swan') or trade.get('signal_type') == "GATEKEEPER":
            min_dist = current_price * 0.02
            if sl_distance < min_dist:
                sl_distance = min_dist
        
        # --- RECOVERY EARLY EXIT: Mean Reversion Complete ---
        sig_type = trade.get('signal_type')
        current_rsi = self.latest_data.get(symbol, {}).get('rsi')
        
        if sig_type in ["RECOVERY", "REJECTION"] and isinstance(current_rsi, (int, float)):
            entry_price = float(trade.get('entry_price', current_price))
            pnl_pct = (current_price - entry_price) / entry_price if is_long else (entry_price - current_price) / entry_price
            
            # Logic: Require RSI to cross 55/45 (stronger neutral) AND have at least 1.2% profit to cover fees
            if (is_long and current_rsi > 55 and pnl_pct > 0.012) or (not is_long and current_rsi < 45 and pnl_pct > 0.012):
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
            elif current_price >= tp2 and not trade.get('tp1_hit', False):
                should_close = True
                reason = "TAKE-PROFIT 2 (FINAL - STATIC)"
            elif current_price >= tp1:
                # Partial Take Profit (TP1) - Close 50%
                if not trade.get('tp1_hit', False):
                    # ENFORCE MINIMUM TP1 profit (1.0% price move)
                    pnl_tp1 = (current_price - entry_price) / entry_price
                    if pnl_tp1 < 0.010: # 1% Minimum
                         logger.info(f"⏳ [TP1 GUARD] Skipping premature TP1 for {symbol}: {pnl_tp1:.2%} < 1.00%")
                    else:
                        # Aggressive Recovery Exit (v9.8.5)
                        # If it's a recovered zombie and ROI > 20%, close 100% immediately
                        roe_pct = pnl_tp1 * getattr(self, 'leverage', 1.0)
                        if trade.get('status') == 'RECOVERED_ZOMBIE' and roe_pct > 0.20:
                             logger.warning(f"🆘 [AGGRESSIVE ZOMBIE EXIT] {symbol} at {roe_pct:.2%} ROE (>20%). Closing 100% (TP2).")
                             asyncio.create_task(self.close_position(symbol, trade, partial_pct=1.0, reason="AGGRESSIVE_ZOMBIE_EXIT"))
                             return # Exit fully
                        
                        logger.warning(f"🎯 PARTIAL TP1 HIT for {symbol} at {current_price} (+{pnl_tp1:.2%})")
                        asyncio.create_task(self.close_position(symbol, trade, partial_pct=0.5, reason="PARTIAL_EXIT_TP1"))
                        trade['tp1_hit'] = True
                        # --- [CRITICAL] SOLID BREAK-EVEN ---
                        # Move SL to entry + 0.3% to lock in costs. 
                        # This ensures TP2 part cannot go negative.
                        offset = entry_price * 0.003
                        trade['sl'] = entry_price + offset
                        logger.warning(f"🛡️ [BREAK-EVEN] SL for {symbol} moved to {trade['sl']} (Entry+Costs)")
                    
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
            elif current_price <= tp2 and not trade.get('tp1_hit', False):
                should_close = True
                reason = "TAKE-PROFIT 2 (FINAL - STATIC)"
            elif ai_tp and current_price <= ai_tp:
                should_close = True
                reason = "AI PRECISE TARGET HIT"
            elif current_price <= tp1:
                # Partial Take Profit (TP1) - Close 50%
                if not trade.get('tp1_hit', False):
                    # ENFORCE MINIMUM TP1 profit (1.0% price move)
                    pnl_tp1 = (entry_price - current_price) / entry_price
                    if pnl_tp1 < 0.010: # 1% Minimum
                         logger.info(f"⏳ [TP1 GUARD] Skipping premature TP1 for {symbol}: {pnl_tp1:.2%} < 1.00%")
                    else:
                        # Aggressive Recovery Exit (v9.8.5)
                        # If it's a recovered zombie and ROI > 20%, close 100% immediately
                        roe_pct = pnl_tp1 * getattr(self, 'leverage', 1.0)
                        if trade.get('status') == 'RECOVERED_ZOMBIE' and roe_pct > 0.20:
                             logger.warning(f"🆘 [AGGRESSIVE ZOMBIE EXIT] {symbol} at {roe_pct:.2%} ROE (>20%). Closing 100% (TP2).")
                             asyncio.create_task(self.close_position(symbol, trade, partial_pct=1.0, reason="AGGRESSIVE_ZOMBIE_EXIT"))
                             return # Exit fully

                        logger.warning(f"🎯 PARTIAL TP1 HIT for {symbol} at {current_price} (+{pnl_tp1:.2%})")
                        asyncio.create_task(self.close_position(symbol, trade, partial_pct=0.5, reason="PARTIAL_EXIT_TP1"))
                        trade['tp1_hit'] = True
                        # --- [CRITICAL] SOLID BREAK-EVEN ---
                        # Move SL to entry - 0.3% to lock in costs.
                        offset = entry_price * 0.003
                        trade['sl'] = entry_price - offset
                        logger.warning(f"🛡️ [BREAK-EVEN] SL for {symbol} moved to {trade['sl']} (Entry+Costs)")
                
        if should_close:
            logger.warning(f"🔔 SOFT {reason} HIT for {symbol} at {current_price}!")
            # Aggiorna l'esito dell'IA prima di chiudere
            # --- NEW: POST-MORTEM FEEDBACK LOOP (Isolate errors to prevent blocking closure) ---
            try:
                # Calculate PnL for post-mortem analysis
                pnl = (current_price - entry_price) / entry_price if is_long else (entry_price - current_price) / entry_price
                asyncio.create_task(self.analyst.perform_post_mortem(symbol, trade, current_price, pnl))
            except Exception as ai_err:
                logger.error(f"⚠️ Failed to initiate post-mortem for {symbol}: {ai_err}")
            
            asyncio.create_task(self.close_position(symbol, trade, reason=reason))
            self.trade_levels[symbol] = None
            self.db.save_state("trade_levels", self.trade_levels)

    async def close_position(self, symbol, trade, partial_pct=1.0, reason=None):
        try:
            side = str(trade.get('side', 'buy')).lower()
            is_long = side in ['buy', 'long']
            close_side = 'sell' if is_long else 'buy'
            
            # --- ROBUST AMOUNT EXTRACTION (Binance/Bitget Compat) ---
            total_amount = 0.0
            if trade and 'amount' in trade:
                total_amount = float(trade['amount'])
            
            # If trade state is missing or corrupted, try to fetch from exchange directly
            if total_amount <= 0:
                logger.warning(f"⚠️ [RECOVERY] Trade state for {symbol} is missing amount. Fetching from exchange...")
                positions = await self.exchange.fetch_positions([symbol])
                if positions:
                    pos = positions[0]
                # v10.5 Fix: Binance raw position key is 'positionAmt'
                total_amount = abs(float(pos.get('positionAmt', pos.get('amount', 0)) or 0))

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
            # Bitget/CCXT might return None for price or average in market orders
            raw_avg = order_response.get('average')
            raw_price = order_response.get('price')
            execution_price = float(raw_avg if raw_avg is not None else (raw_price if raw_price is not None else 0))
            
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
            
            pnl_pct_raw = (execution_price - entry_price) / entry_price if is_long else (entry_price - execution_price) / entry_price
            # v10.3: Multiply by 100 to store as % (e.g. 5.0 instead of 0.05)
            pnl_pct_leveraged = pnl_pct_raw * getattr(self, 'leverage', 1.0) * 100
            
            # Use provided reason or fallback to default
            if not reason:
                reason = "EXIT" if partial_pct >= 1.0 else "PARTIAL_EXIT"
            
            # Log with exchange_trade_id to prevent duplicates in sync_binance_trades
            # Capture Real Exit Price for Shadow PnL
            real_exit_price = 0.0
            if hasattr(self, 'data_fetcher'):
                try:
                    rt = await self.data_fetcher.fetch_ticker(symbol)
                    real_exit_price = float(rt.get('last') or rt.get('close') or 0)
                except: pass

            self.db.log_trade(
                symbol, 
                "CLOSE" if partial_pct >= 1.0 else "CLOSE_PARTIAL", 
                execution_price, 
                amount_to_close, 
                pnl, 
                pnl_pct_leveraged, 
                reason, 
                exchange_trade_id=order_id,
                real_price=real_exit_price
            )
            
            # Log Shadow Comparison
            if real_exit_price > 0 and self.trade_levels[symbol] and self.trade_levels[symbol].get('real_entry_price'):
                r_entry = self.trade_levels[symbol]['real_entry_price']
                side = self.trade_levels[symbol]['side']
                shadow_pnl_pct = ((real_exit_price - r_entry) / r_entry) * 100 if side == 'long' else ((r_entry - real_exit_price) / r_entry) * 100
                logger.warning(f"📊 [Shadow PnL] {symbol} | Mainnet: {shadow_pnl_pct:+.2f}% | Testnet: {pnl_pct_leveraged:+.2f}%")
            
            self.notifier.notify_trade(symbol, "CLOSE", execution_price, amount_to_close, reason, pnl=pnl, pnl_pct=pnl_pct_leveraged)
            
            # Save state immediately to persist closure
            self.db.save_state("trade_levels", self.trade_levels)
        except Exception as e:
            logger.error(f"Failed to close position for {symbol}: {e}")
            logger.error(traceback.format_exc())

    def _calculate_order_amount(self, symbol, current_price, custom_leverage=None, consensus_score=3.5, **kwargs):
        """Calculates the optimal order amount based on dynamic risk parameters (v9.7.4)."""
        leverage = custom_leverage if custom_leverage else self.leverage
        
        # 1. Base Risk Sizing from risk_profile (v9.7.4 Pondered Logic)
        risk_pct = getattr(self, 'percent_per_trade', 3.0)
        
        # --- NEW: AI PONDERED SIZING (v9.7.4) ---
        raw_ai_mod = kwargs.get('ai_strength', 1.0)
        score_weight = consensus_score / 10.0
        
        is_aggressive = risk_pct > 5.0
        dampening_factor = 0.8 if is_aggressive else 0.5 
        
        ai_mod = 1.0 + (raw_ai_mod - 1.0) * dampening_factor * score_weight
        # v10.5 Conservative refinement: 3% base, up to 10% max (mod 3.33)
        ai_mod = max(0.8, min(1.8 if is_aggressive else 3.33, ai_mod))
        
        total_pct = risk_pct * ai_mod
        
        # --- CIRCUIT BREAKER PROTECTIONS ---
        if self.circuit_breaker_active:
             total_pct = 0.5
             logger.warning(f"☢️ [SIZING] Circuit Breaker active: 0.5% for {symbol}.")
        elif self.is_macro_paused:
             total_pct = 0.5
             logger.warning(f"🛡️ [SIZING] Macro Volatility Shield: 0.5% for {symbol}.")
        
        # 2. Base Capital Calculation from Equity
        current_equity = self.latest_account_data.get('equity', 0)
        if current_equity <= 0:
             logger.error(f"⚠️ [SIZING ERROR] Account Equity not synced. Aborting.")
             return 0.0
              
        base_usdt = current_equity * (total_pct / 100.0)
        
        # 3. Market Multipliers (STRONGLY DAMPERED v9.7.4)
        current_atr = self.latest_data[symbol].get('atr', current_price * 0.01)
        if current_atr == "N/A" or not current_atr:
            current_atr = current_price * 0.01
            
        atr_pct = (current_atr / current_price) if current_price > 0 else 0.01
        baseline_atr_pct = 0.01
        
        # Cap multipliers to 1.1x (Conservative) or 1.15x (Aggressive) to avoid cap-hitting
        market_cap_val = 1.15 if is_aggressive else 1.10
        volatility_modifier = max(0.9, min(market_cap_val, baseline_atr_pct / atr_pct if atr_pct > 0 else 1.0))
        regime_mult = min(market_cap_val, self.latest_data.get(symbol, {}).get('regime_multiplier', 1.0))
        
        adjusted_usdt = base_usdt * volatility_modifier * regime_mult
        
        # --- GLOBAL SAFETY CAP (DYNAMIC v9.7.4) ---
        max_limit_val = getattr(self, 'max_margin_pct', 15.0)
        max_limit_usdt = current_equity * (max_limit_val / 100.0)
        
        if adjusted_usdt > max_limit_usdt:
             logger.warning(f"🛡️ [SIZING CAP] Capping margin to {max_limit_val}%: {max_limit_usdt:.2f} USDT.")
             adjusted_usdt = max_limit_usdt

        # --- DYNAMIC FLOOR (1% Equity) ---
        min_margin = max(5.0, current_equity * 0.01) 
        if adjusted_usdt < min_margin and not self.circuit_breaker_active:
             adjusted_usdt = min_margin

        # --- EMERGENCY GLOBAL MARGIN CAP (v10.5) ---
        # Hard stop if we are already using > 85% of the account as margin
        current_used_margin = self.latest_account_data.get('used_margin', 0)
        if current_used_margin > (current_equity * 0.85):
             logger.error(f"☢️ [EMERGENCY] Global Margin Usage is too high ({current_used_margin:.2f} / {current_equity:.2f}). Blocking NEW entry.")
             return 0.0

        # --- DYNAMIC LEVERAGE CLAMP ---
        # Standard range 5x - 20x as requested
        clamped_leverage = max(5, min(25 if is_aggressive else 20, leverage))
        
        if raw_ai_mod >= 1.5 and leverage > 20:
             clamped_leverage = min(25, leverage)

        # RE-CALCULATE amount with final leverage (v9.8.9 Fix)
        amount_to_buy = (adjusted_usdt * clamped_leverage) / current_price
             logger.info(f"🔥 [AI LEVERAGE BOOST] Conviction High: Allowing {clamped_leverage}x leverage for {symbol}.")
        
        purchasing_power = adjusted_usdt * clamped_leverage
        
        if purchasing_power < 5.0:
            purchasing_power = 5.0

        if current_price <= 0:
            logger.error(f"❌ Impossibile calcolare l'ordine per {symbol}: prezzo zero.")
            return 0.0
            
        amount = purchasing_power / current_price
        
        try:
            market = self.exchange.markets.get(symbol, {})
            max_qty = market.get("limits", {}).get("amount", {}).get("max")
            if max_qty and amount > max_qty:
                logger.warning(f"⚠️ [Safety] Capping order size for {symbol}: {amount} -> {max_qty} (Exchange Max)")
                amount = max_qty
        except:
            pass
        
        logger.info(f"📐 [{symbol}] Sizing Summary: {total_pct:.2f}% of ${current_equity:.2f} -> {adjusted_usdt:.2f} USDT Margin (Purchasing Power: {purchasing_power:.2f} USDT at {clamped_leverage}x)")
        
        # Adjust for exchange precision rules
        amount = self.exchange.amount_to_precision(symbol, amount)
        return float(amount)

    def _apply_symbol_bridge(self, symbols: List[str]) -> List[str]:
        """Maps human-readable symbols to exchange-specific ones (e.g. BTC/USDT -> BTCUSDT)."""
        if not hasattr(self.exchange, 'markets') or not self.exchange.markets:
            try:
                # Synchronous load markets for bridge if needed (rare)
                import ccxt
                self.exchange.load_markets()
            except:
                return symbols
                
        validated: List[str] = []
        for s in symbols:
            if s in self.exchange.markets:
                validated.append(s)
            else:
                # Try variations
                alts = [s.replace('/', ''), f"{s}:USDT", s.split('/')[0] + "USDT"]
                found = False
                for alt in alts:
                    if alt in self.exchange.markets:
                        logger.info(f"Bridgeing {s} -> {alt}")
                        validated.append(alt)
                        found = True
                        break
                if not found:
                    logger.warning(f"Bridge: Symbol {s} not found.")
        return validated

    def calculate_dynamic_leverage(self, symbol, side, consensus_score, current_price, **kwargs):
        """Calculates dynamic leverage weighted by AI conviction (v9.8.7)."""
        latest = self.latest_data.get(symbol, {})
        if not latest or current_price <= 0:
            return self.leverage
            
        # Adaptive Profile Check
        risk_pct = getattr(self, 'percent_per_trade', 3.0)
        is_aggressive = risk_pct > 5.0

        # 1. AI Suggested vs Base (Pondered v9.8.7)
        score_trust = consensus_score / 10.0
        ai_lev = kwargs.get('ai_leverage', 0)
        
        base_lev = self.leverage
        if ai_lev > 0:
            # AI Suggestion is weighted by its own confidence score
            base_lev = base_lev + (ai_lev - base_lev) * score_trust
        
        # 2. Volatility Penalty (Mitigated by AI Confidence v9.8.7)
        atr = latest.get('atr', current_price * 0.01)
        vol_pct = (atr / current_price) * 100 if current_price > 0 else 0
        vol_penalty = vol_pct * (0.5 if is_aggressive else 1.2)
        
        # --- NEW: CONFIDENCE AUTO-BOOST ---
        # If AI is very sure, it "protects" leverage from volatility noise
        if consensus_score >= 9.0:
            vol_penalty *= 0.4 # 60% reduction in penalty
            logger.info(f"💎 [Leverage] {symbol} Extreme Confidence (9+): Volatility penalty heavily reduced.")
        elif consensus_score >= 8.0:
            vol_penalty *= 0.7 # 30% reduction in penalty
            logger.info(f"💎 [Leverage] {symbol} High Confidence (8+): Volatility penalty reduced.")

        # 3. Dynamic Trend Bonus
        trend_bonus = 0
        if consensus_score >= 7.0: 
            ema200 = latest.get('ema200')
            if isinstance(ema200, (int, float)):
                is_long = side.lower() in ['buy', 'long']
                if (is_long and current_price > ema200) or (not is_long and current_price < ema200):
                    trend_bonus = 4 if is_aggressive else 2
        
        lev_calc = base_lev - vol_penalty + trend_bonus
        
        # 4. Dynamic Floor Based on Confidence (v9.8.7)
        # Standard min is 5x. High confidence raises floor to 8x or 10x.
        floor_lev = 5
        if consensus_score >= 9.2:
            floor_lev = 10
        elif consensus_score >= 8.5:
            floor_lev = 8
            
        final_max = 25 if is_aggressive else 20
        final_lev = max(floor_lev, min(final_max, round(lev_calc)))
        
        logger.info(f"📐 [Leverage] {symbol} (v9.8.7): AI={ai_lev}, Trust={score_trust:.2f}, Floor={floor_lev}x -> Final={final_lev}x")
        return final_lev

    async def execute_order(self, symbol, side, current_price, is_black_swan=False, consensus_score=5.0, signal_type="TECH", mtf_context="N/A"):
        # --- EMERGENCY PRICE RECOVERY ---
        # If price is 0 (can happen with news signals before scanner sync), fetch it now
        if current_price <= 0:
            logger.warning(f"🔍 [Price Recovery] Current price for {symbol} is 0. Fetching fresh price from Mainnet...")
            try:
                provider = self._get_data_provider(symbol)
                ticker = await provider.fetch_ticker(symbol)
                current_price = float(ticker.get('last') or ticker.get('close') or 0)
                if current_price <= 0:
                    logger.error(f"❌ [Price Recovery] Failed to fetch price for {symbol}. Aborting order.")
                    return
                logger.info(f"✅ [Price Recovery] Successfully fetched price for {symbol}: {current_price}")
            except Exception as e:
                logger.error(f"❌ [Price Recovery] Error fetching price for {symbol}: {e}")
                return

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
                # v10.5 Fix: Using 'positionAmt' for Binance raw positions
                confirmed_positions = [p for p in self.latest_account_data.get('positions', []) if float(p.get('positionAmt', p.get('amount', 0)) or 0) != 0]
                
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
                    # --- [UPGRADED] ANTI-PYRAMIDING & DYNAMIC CAP (v9.7.6) ---
                    # --- [UPGRADED] ADAPTIVE ANTI-PYRAMIDING (v10.5) ---
                    matching_position = next((p for p in self.latest_account_data.get('positions', []) if p['symbol'] == symbol), None)
                    
                    # Self-Healing: Trust Binance over local state if they disagree
                    # v10.5 Fix: Using 'positionAmt' for Binance raw positions
                    is_p_active = matching_position and abs(float(matching_position.get('positionAmt', matching_position.get('amount', 0)))) > 0
                    if not is_p_active and self.trade_levels.get(symbol):
                         # Internal state says active but Binance says empty -> Manual closure recovery
                         logger.warning(f"🔓 [RECOVERY] {symbol} found empty on Binance. Clearing internal state for entry.")
                         self.trade_levels[symbol] = None
                         self.active_positions[symbol] = None
                    
                    is_already_trading = is_p_active or (self.trade_levels.get(symbol) is not None)
                    
                    # Profile Context
                    risk_pct = getattr(self, 'percent_per_trade', 3.0)
                    is_aggressive = risk_pct > 5.0
                    
                    # Standard Caps vs Extended Caps (v9.7.6)
                    # Conservative: strictly 10.0% max exposure per symbol
                    std_cap = 25.0 if is_aggressive else 10.0
                    ext_cap = 35.0 if is_aggressive else 10.0
                    
                    if is_already_trading:
                        # 1. Pyramiding Guard: Skip unless high conviction
                        if consensus_score < 8.5 and not is_black_swan:
                             logger.warning(f"🚫 [ANTI-PYRAMID] Symbol {symbol} already trading. Score {consensus_score:.1f} too low for second entry. Skipping.")
                             self.active_positions[symbol] = None
                             self.pending_orders_count = max(0, self.pending_orders_count - 1)
                             return
                        
                        # 2. Cumulative Margin Check
                        current_margin_usdt = 0
                        if matching_position:
                             # Calc current margin (Notional / Leverage)
                             notional = abs(float(matching_position.get('notional', 0)))
                             pos_lev = abs(float(matching_position.get('leverage', 10)))
                             current_margin_usdt = notional / pos_lev if pos_lev > 0 else 0
                        
                        current_equity = self.latest_account_data.get('equity', 10000)
                        total_margin_pct = (current_margin_usdt / current_equity) * 100 if current_equity > 0 else 0
                        
                        # Determine current allowed cap for this symbol
                        # If AI is extremely sure (9.2+) or Black Swan, use extended cap
                        effective_cap = ext_cap if (consensus_score >= 9.2 or is_black_swan) else std_cap
                        
                        if total_margin_pct >= effective_cap:
                             logger.warning(f"🚫 [CUMULATIVE CAP] {symbol} already at {total_margin_pct:.1f}% margin. Cap is {effective_cap}%. Selection rejected.")
                             self.active_positions[symbol] = None
                             self.pending_orders_count = max(0, self.pending_orders_count - 1)
                             return
                        
                        if consensus_score >= 8.5:
                             logger.info(f"🔥 [PYRAMID SCAN] High Conviction ({consensus_score:.1f}) detected for {symbol}. Allowing secondary entry within {effective_cap}% cap.")

                # Switch to global margin ratio guard
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
                cooldown_active = (now - last_review) < 300 if last_review > 0 else False
                
                if cooldown_active and not is_news_signal and not is_high_priority and not is_side_flip:
                    logger.info(f"❄️ [COST OPTIMIZATION] Skipping repetitive AI Review for {symbol}")
                    self.active_positions[symbol] = None
                    self.pending_orders_count = max(0, self.pending_orders_count - 1)
                    return

                # Initial defaults
                approved, ai_strength, ai_leverage, ai_sl_mult, ai_tp_mult, ai_tp_price, reason = False, 1.0, self.leverage, 1.0, 1.0, None, "Pending Review"

                # --- NEW: STARTUP NEWS SHIELD (10m) ---
                if is_news_signal and (time.time() - self.start_time) < 600:
                    logger.warning(f"🛡️ [STARTUP SHIELD] News Signal ignored for {symbol}: Waiting for fresh data (elapsed: {int(time.time() - self.start_time)}s)")
                    self.active_positions[symbol] = None
                    self.pending_orders_count = max(0, self.pending_orders_count - 1)
                    return

                # --- REFINED: Force LLM Review even for News (Bypass Removed) ---
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
                
                # REINFORCE: set_leverage is CRITICAL before order
                try:
                    market = self.exchange.market(symbol)
                    # Force update the leverage for this specific trade
                    await self.exchange.set_leverage(int(active_leverage), market['id'])
                    logger.info(f"⚙️ [LEVERAGE SET] {symbol} adjusted to {active_leverage}x for this trade.")
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
                # Optimized Strategy: SL 2.0x, TP1 4.0x, TP2 10.0x (Wider for big runs)
                # --- NEW: Volatility Shield SL (DeepSeek Refinement) ---
                if self.is_macro_paused:
                    sl_mult = 6.0
                    tp1_mult = 10.0
                    # TP2 very large to allow Trailing Stop to do the work
                    tp2_mult = 25.0
                    logger.warning(f"🛡️ [RISK MGMT] Macro Volatility Shield active: Widened SL/TP for {symbol}.")
                else:
                    sl_mult = 2.0
                    tp1_mult = 4.0
                    # Higher TP2 to favor Trailing Stop Largo
                    tp2_mult = 15.0
                
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

            # Capture Real Entry Price for Shadow PnL
            real_entry_price = 0.0
            if hasattr(self, 'data_fetcher'):
                try:
                    rt = await self.data_fetcher.fetch_ticker(symbol)
                    real_entry_price = float(rt.get('last') or rt.get('close') or 0)
                    logger.info(f"🛡️ [Shadow Entry] {symbol} Mainnet Price: {real_entry_price}")
                except Exception as e:
                    logger.error(f"⚠️ Failed to catch Shadow Entry price for {symbol}: {e}")

            # Log to DB with Real Price and Exchange ID (to prevent sync duplicates)
            self.db.log_trade(
                symbol, 
                side.upper(), 
                current_price, 
                amount_to_buy, 
                0, 
                0, 
                f"{signal_type} ({consensus_score:.2f})",
                exchange_trade_id=order['id'],
                real_price=real_entry_price
            )

            self.trade_levels[symbol] = {
                'side': 'buy' if is_long else 'sell',
                'entry_price': current_price,
                'real_entry_price': real_entry_price, # STORE REAL ENTRY
                'open_time': time.time(), # v10.0: Stagnation Shield support
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
                            
                            # --- DYNAMIC SYMBOL BRIDGE ---
                            self.symbols = self._apply_symbol_bridge(new_symbols)
                            
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
            
            # --- AI STRATEGIC OPTIMIZATION (v5.0) ---
            # Run hourly global risk optimization
            asyncio.create_task(self.ai_optimizer.run_optimization_cycle())

            # Sleep before fetching again
            await asyncio.sleep(10)

    def _calculate_unified_balance(self, balances):
        """Standardizes account data across different exchange API structures."""
        # Generic fallbacks from CCXT 'total' structure
        # Sum up values of all major assets to get a true equity baseline
        wallet_balance = 0.0
        if 'total' in balances:
            # Add up USDT and USDC directly
            wallet_balance += float(balances['total'].get('USDT', 0))
            wallet_balance += float(balances['total'].get('USDC', 0))
            # Add other assets (approximate value by latest price if possible, or just log them)
            # For simplicity in Testnet, we prioritize USDT/USDC but sum others if totals exist
            for asset, total in balances['total'].items():
                if asset not in ['USDT', 'USDC'] and total > 0:
                     # For now we sum them as 1:1 if we don't have price, but 
                     # usually Testnet equity is dominated by USDT/USDC.
                     # This fulfills the 'dynamic' requirement.
                     wallet_balance += float(total)

        equity = wallet_balance
        unrealized_pnl = 0.0
        margin_ratio = 0.0
        
        if 'info' in balances:
            info = balances['info']
            if self.active_exchange_name == "binance":
                # Binance USDS-M (Info is a dict)
                if isinstance(info, dict):
                    wallet_balance = float(info.get('totalWalletBalance') or wallet_balance)
                    equity = float(info.get('totalMarginBalance') or equity)
                    unrealized_pnl = float(info.get('totalUnrealizedProfit') or 0.0)
                    
                    # Calculate Margin Ratio if missing
                    total_maint = float(info.get('totalMaintMargin', 0))
                    margin_ratio = float(info.get('marginRatio') or (total_maint / equity if equity > 0 else 0))
                    
            elif self.active_exchange_name == "bitget":
                # Bitget V2 Structures (USDT-M) - info is a list of asset dicts or a single record
                usdt_info = {}
                if isinstance(info, list):
                    for item in info:
                        if item.get('marginCoin') == 'USDT' or item.get('coin') == 'USDT':
                            usdt_info = item
                            break
                elif isinstance(info, dict):
                    usdt_info = info
                
                # Robust extraction for Bitget V2: totalWalletBalance, totalEquity, totalUnrealizedPL
                wallet_balance = float(usdt_info.get('totalWalletBalance') or usdt_info.get('available') or wallet_balance)
                equity = float(usdt_info.get('totalEquity') or usdt_info.get('equity') or usdt_info.get('marginBalance') or wallet_balance)
                unrealized_pnl = float(usdt_info.get('totalUnrealizedPL') or usdt_info.get('unrealizedPL') or 0.0)
                
                # Margin ratio parsing for Bitget
                total_maint_margin = float(usdt_info.get('totalMaintMargin', 0))
                margin_ratio = float(usdt_info.get('marginRatio') or (total_maint_margin / equity if equity > 0 else 0))
        
        return wallet_balance, equity, unrealized_pnl, margin_ratio

    async def _update_account_state(self):
        """Standardizes and updates the current account state (equity, positions, PnL)."""
        try:
            # 1. Fetch positions
            positions = await self.exchange.fetch_positions()
            active_pos = [p for p in positions if float(p.get('amount', 0) or p.get('contracts', 0) or 0) != 0]

            # 2. Fetch Balance and check Margin
            balances = await self.exchange.fetch_balance()
            wallet_balance, equity, unrealized_pnl, margin_ratio = self._calculate_unified_balance(balances)
            
            # --- PERSISTENCE & SYNC ---
            self.current_margin_ratio = margin_ratio
            self.latest_account_data['balance'] = wallet_balance
            self.latest_account_data['equity'] = equity
            self.latest_account_data['unrealized_pnl'] = unrealized_pnl
            self.latest_account_data['alerts'] = self.alert_history
            
            if self.initial_wallet_balance is None:
                self.initial_wallet_balance = equity
                self.db.save_state("initial_balance", self.initial_wallet_balance)
            
            return active_pos, wallet_balance, equity, unrealized_pnl
        except Exception as e:
            logger.error(f"Error in account state update: {e}")
            return [], 0, 0, 0

    async def account_update_loop(self):
        logger.info("Starting bot account update loop...")
        
        # --- INITIAL STARTUP SYNC (v9.7.5) ---
        # Force a deep synchronization of all known symbols to align history immediately.
        try:
            logger.info("⚡ [DEEP SYNC] Initializing startup trade synchronization...")
            await asyncio.sleep(5) # Wait for initial account fetch to settle
        except: pass
        
        while True:
            try:
                # 1. Unified state update (v9.7)
                # This ensures consistent equity/margin data across all bot modules.
                active_pos, wallet_balance, equity, unrealized_pnl = await self._update_account_state()

                # --- BENCHMARK & PERFORMANCE ---
                # initial_wallet_balance and persistence are handled inside _update_account_state()
                
                # --- OVERHAUL: Circuit Breaker now also considers EQUITY DRAWDOWN (Unrealized) ---
                # This stops the "mess" by reacting to bleeding positions before they are closed.
                wallet_pnl_pct = (wallet_balance - self.initial_wallet_balance) / self.initial_wallet_balance if self.initial_wallet_balance > 0 else 0
                equity_pnl_pct = (equity - self.initial_wallet_balance) / self.initial_wallet_balance if self.initial_wallet_balance > 0 else 0
                
                # Use the worse of the two for the circuit breaker
                effective_pnl_pct = min(wallet_pnl_pct, equity_pnl_pct)

                # --- NEW: STARTUP PROTECTION SHIELD (v4.2) ---
                # v9.8.5.2: 15s shield to allow Binance API to return full active_pos list
                # before we assume manual closures (Anti-Pyramid).
                is_startup_protected = (time.time() - self.start_time) < 15

                if effective_pnl_pct <= -self.daily_loss_limit:
                    if not self.circuit_breaker_active:
                        if not is_startup_protected:
                            logger.critical(f"🛑 [CB LOCK]: Daily loss limit reached ({effective_pnl_pct:.2%}). Activating Strategic Circuit Breaker.")
                            self.circuit_breaker_active = True
                        else:
                            logger.warning(f"🛡️ [STARTUP SHIELD] Circuit Breaker suppressed during sync ({effective_pnl_pct:.2%}).")
                
                # --- NEW: GLOBAL PANIC SELL ---
                if effective_pnl_pct <= -self.panic_drawdown_threshold:
                    if not self.global_panic_notified:
                        if not is_startup_protected:
                            logger.critical(f"🆘 [PNL PANIC]: Drawdown at {effective_pnl_pct:.2%}. CLOSING ALL POSITIONS.")
                            asyncio.create_task(self.emergency_cleanup_all())
                            self.circuit_breaker_active = True
                            self.global_panic_notified = True
                        else:
                            # CRITICAL: Suppress action during startup shield
                            logger.warning(f"🛡️ [STARTUP SHIELD] Global Panic suppressed during sync ({effective_pnl_pct:.2%}).")
                            return # Avoid proceeding to margin dangerous check if we are in protection
                
                elif effective_pnl_pct > -0.06: # Reset if we recover or restart with better balance (v9.9.11: relaxed for volatility)
                    if self.circuit_breaker_active:
                        logger.warning(f"🟢 Circuit Breaker Reset: Current loss at {effective_pnl_pct:.2%}")
                        self.circuit_breaker_active = False
                        self.global_panic_notified = False # Reset flag when circuit breaker is reset

                logger.info(f"Account Update (Real Balance): Wallet={wallet_balance:.2f}, Equity={equity:.2f}, PnL={unrealized_pnl:.2f} (Eff: {effective_pnl_pct:.2%})")
                
                # --- DANGER CHECK ---
                
                if self.current_margin_ratio > 0.95: # 95% Margin Usage is DANGEROUS
                    if is_startup_protected:
                        logger.warning(f"🛡️ [STARTUP SHIELD] Margin danger suppressed during sync ({self.current_margin_ratio*100:.2f}%).")
                        return # Protection Shield ACTIVE

                    logger.critical(f"⚠️ [MARGIN DANGER]: Margin Ratio at {self.current_margin_ratio*100:.2f}%! Executing Emergency Cleanup.")
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
                                    'amount': pos.get('amount', pos['contracts'])
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
                            # --- DEPRECATED SYNC (v9.8.5) ---
                            # Logic moved to initial sync_zombie_positions to avoid race conditions and redundant param calc.
                            # Sync is now handled early in sync_zombie_positions.
                            pass
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
                # We check ALL symbols in monitor list to ensure manual trades are captured
                all_trades = []
                # --- [UPGRADED] GLOBAL SCAN (v9.7.5 Deep Sync) ---
                # Symbols to monitor: Default + Active Positions + Wallet Balances + Bot Internal State
                # v10.5 Fix: Using 'positionAmt' for Binance raw positions
                pos_symbols = [p['symbol'] for p in self.latest_account_data.get('positions', []) if abs(float(p.get('positionAmt', p.get('amount', 0)) or 0)) > 0]
                
                # Internal bot state (symbols that the bot 'thinks' are open)
                state_symbols = list(self.trade_levels.keys())
                
                bal_symbols = []
                try:
                    balance = self.latest_account_data.get('full_balance', {})
                    if 'info' in balance and 'assets' in balance['info']: # Binance
                        bal_symbols = [f"{a['asset']}/USDT:USDT" for a in balance['info']['assets'] if float(a.get('walletBalance', 0)) > 0]
                except: pass
                
                # Combine all sources of signal activity
                symbols_to_check = list(set(self.symbols + pos_symbols + state_symbols + bal_symbols))
                
                # v10.5 [ADAPTIVE ANTI-PYRAMID]
                # Stricter: Always check symbols that are in trade_levels but NOT in pos_symbols (POTENTIAL MANUAL CLOSE)
                # v9.8.5.2: Safeguard with is_startup_protected to avoid clearing recovered zombies during initial sync
                manual_check_targeted = [s for s in state_symbols if s not in pos_symbols and self.trade_levels.get(s)]
                if manual_check_targeted and not is_startup_protected:
                     logger.warning(f"🔓 [PYRAMID UNLOCKED] Manual closure detected on Binance for {manual_check_targeted}. Freeing internal state.")
                     for s in manual_check_targeted:
                         self.trade_levels[s] = None
                         self.active_positions[s] = None
                     
                     # Force state save
                     self.db.save_state("trade_levels", self.trade_levels)

                # Increase chunked scanning to ensure no trade is missed
                for i in range(0, len(symbols_to_check), 15): 
                    chunk = symbols_to_check[i:i+15]
                    tasks = [self.exchange.fetch_my_trades(symbol, limit=20) for symbol in chunk if symbol in self.exchange.markets]
                    
                    if not tasks: continue
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    for res in results:
                        if isinstance(res, list):
                            all_trades.extend(res)
                    await asyncio.sleep(0.5) 
                
                # Sync trades to local database for history tracking
                if all_trades:
                    # v10.4: Session Lockdown - Only sync trades happened in the CURRENT session
                    # This prevents 'ghost' trades from blocking new entries (Anti-Pyramid)
                    session_start = getattr(self, 'session_start_time', time.time())
                    recent_only = [t for t in all_trades if (t.get('timestamp', 0) / 1000.0) >= session_start]
                    
                    if recent_only:
                        await asyncio.to_thread(self.db.sync_binance_trades, recent_only, min_timestamp_ms=int(session_start * 1000))
                        logger.info(f"Account Update: Synced {len(recent_only)} NEW session trades found on Binance.")
                    
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
            active_pos = [p for p in positions if float(p.get('amount', 0) or p.get('contracts', 0) or 0) != 0]
            
            for pos in active_pos:
                symbol = pos['symbol']
                # Support both CCXT normalized components and Binance native names
                amount = abs(float(pos.get('amount', pos.get('positionAmt', 0)) or 0))
                # Side detection
                raw_side = pos.get('side', '') # 'long' or 'short'
                if not raw_side:
                    raw_side = 'long' if float(pos.get('amount', pos.get('positionAmt', 0)) or 0) > 0 else 'short'
                
                side = raw_side.lower()
                close_side = 'sell' if side == 'long' else 'buy'
                
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
            # Fetch 100 candidates to let Gemini pick the Final 60 (v9.7.7 Wide Radar)
            scored_assets = await self.scanner.get_top_performing_assets(limit=100)
            
            # 2. AI Refinement: Let Gemini pick the Final 60
            new_symbols = await self.analyst.refine_market_selection(scored_assets, limit=60)
            
            # --- NEW: STICKY SYMBOLS PROTECTION (v3.5) ---
            # Ensure any symbol with an active position is KEPT in the list
            active_symbols_in_trade = [s for s, level in self.trade_levels.items() if level is not None]
            
            # Merge new_symbols with active_symbols_in_trade, ensuring uniqueness
            final_symbols = list(set(new_symbols) | set(active_symbols_in_trade))
            
            # [FIX v9.8.3] Apply symbol bridge BEFORE initializing dictionaries
            final_symbols_bridged = self._apply_symbol_bridge(final_symbols)
            
            added = set(final_symbols_bridged) - set(self.symbols)
            removed = set(self.symbols) - set(final_symbols_bridged)
            
            # Explicitly log if we are keeping a symbol because it has an active position
            for s in active_symbols_in_trade:
                unbridged = s.split(':')[0]
                if unbridged not in new_symbols and s not in new_symbols:
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

            # Apply new list to active state
            self.symbols = final_symbols_bridged
            
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
            active_pos = [p for p in positions if float(p.get('amount', 0) or p.get('contracts', 0) or 0) != 0]
            
            for pos in active_pos:
                symbol = pos['symbol']
                
                # Check if already tracked (possibly migrated)
                if self.trade_levels.get(symbol) is not None:
                    continue

                logger.warning(f"🧟 [ZOMBIE FOUND] Rediscovered untracked position for {symbol}. Restoring monitoring...")
                
                # Ensure symbol is in our monitor list
                if symbol not in self.symbols:
                    self.symbols.append(symbol)
                    # Initialize structures for this symbol if missing
                    if symbol not in self.latest_data:
                        self.latest_data[symbol] = {
                            'price': float(pos.get('markPrice', 0)), 
                            'prediction': 'Syncing...',
                            'change': 0.0, 'changePercent': 0.0, 'rsi': 0.0, 'macd': 0.0, 'macd_hist': 0.0, 'bb_upper': 0.0, 'bb_lower': 0.0, 'imbalance': 1.0, 'atr': 0.0, 'ema200': 0.0, 'volume24h': 0.0
                        }
                    if symbol not in self.trade_levels:
                        self.trade_levels[symbol] = None

                # Reconstruct a basic trade state
                side = pos['side'].lower()
                entry_price = float(pos['entryPrice'])
                current_price = float(pos.get('markPrice', entry_price))
                amount = abs(float(pos.get('amount', pos.get('positionAmt', 0))))
                
                # Fallback to Risk Profile defaults for recovered positions
                # v9.8.5: Uses actual config instead of arbitrary 25% TP
                def_sl = self.stop_loss_pct or 0.02
                def_tp = self.take_profit_pct or 0.05
                
                sl_price = entry_price * (1 + def_sl if side == 'short' else 1 - def_sl)
                tp1_price = entry_price * (1 - def_tp/2 if side == 'short' else 1 + def_tp/2) # Partial TP at half final TP
                tp2_price = entry_price * (1 - def_tp if side == 'short' else 1 + def_tp)
                
                self.trade_levels[symbol] = {
                    'side': 'long' if side == 'long' else 'short',
                    'entry_price': entry_price,
                    'sl': sl_price,
                    'tp1': tp1_price,
                    'tp2': tp2_price,
                    'amount': amount,
                    'open_time': time.time() - 3600, # Fake an hour age to allow stagnation check if needed
                    'status': 'RECOVERED_ZOMBIE',
                    'tp1_hit': False,
                    'highest_price': current_price if side == 'long' else 0,
                    'lowest_price': current_price if side == 'short' else 999999
                }
                logger.warning(f"🛡️ [RECOVERY] {symbol} restored with Profile Defaults: SL @ {sl_price:.4f}, TP1 @ {tp1_price:.4f}")
                
                # --- IMMEDIATE AUDIT (v9.8.5) ---
                # Check if we should close this right now (e.g. if already deep in profit)
                try:
                    self._check_soft_stop_loss(symbol, current_price)
                except Exception as audit_err:
                    logger.error(f"⚠️ Initial audit failed for recovered {symbol}: {audit_err}")
            
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
            await asyncio.sleep(120) # Run every 2 minutes

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
        
        # --- STAGNATION SHIELD (v10.0 Time Stop) ---
        open_time = level.get('open_time')
        if open_time:
            age_hours = (time.time() - open_time) / 3600
            stagnation_limit = self.risk_profile.get('trading_parameters', {}).get('stagnation_max_hours', 3)
            stagnation_pnl = self.risk_profile.get('trading_parameters', {}).get('stagnation_pnl_threshold', 0.005) * 100 # Convert to %
            
            if age_hours >= stagnation_limit and abs(pnl_pct) < stagnation_pnl:
                logger.warning(f"🛡️ [STAGNATION SHIELD] Closing {symbol} (Open for {age_hours:.1f}h, PnL: {pnl_pct:+.2f}%) | Freeing equity for new signals.")
                await self.close_position(symbol, level, reason="STAGNATION EXIT")
                return

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
        """Close 50% of an active position to lock in profits or reduce risk (AI Triggered)."""
        level = self.trade_levels.get(symbol)
        if not level or level.get('tp1_hit'): return
        
        # Calculate current PnL to verify if we are in the dynamic range (0.75%+)
        entry_price = float(level['entry_price'])
        current_price = self.latest_data[symbol].get('price', entry_price)
        side = level['side']
        is_long = side == 'long'
        pnl_pct = (current_price - entry_price) / entry_price if is_long else (entry_price - current_price) / entry_price

        # Floor di sicurezza per lo SCALE_OUT dinamico (0.75%)
        # Se il profitto è inferiore, l'AI SCALE_OUT viene ignorato per evitare perdite sulle fee
        if pnl_pct < 0.0075:
            logger.info(f"⏳ [AI TP1 GUARD] Ignoring AI SCALE_OUT for {symbol}: {pnl_pct:.2%} < 0.75% minimum.")
            return

        amount_to_close = level['amount'] / 2
        close_side = 'sell' if is_long else 'buy'
        
        logger.warning(f"🎯 [AI DYNAMIC TP1] Gemini triggered early TP1 for {symbol} at {current_price} (+{pnl_pct:.2%})")
        try:
            # Esecuzione ordine
            order = await self.exchange.create_order(symbol, 'market', close_side, amount_to_close, params={'reduceOnly': True})
            
            # 1. Aggiorna quantità rimanente
            level['amount'] -= amount_to_close
            level['tp1_hit'] = True
            
            # 2. [BREAK-EVEN] Sposta SL in zona sicura (Entry + 0.3%)
            offset = entry_price * 0.003
            level['sl'] = entry_price + offset if is_long else entry_price - offset
            
            # 3. Salva stato e notifica
            self.db.save_state("trade_levels", self.trade_levels)
            
            # --- NEW: TP1 NOTIFICATION ---
            msg = f"🎯 *[AI DYNAMIC TP1] TARGET HIT*\n\n" \
                  f"Symbol: `{symbol}`\n" \
                  f"Action: `CLOSE 50%`\n" \
                  f"Price: `${current_price}`\n" \
                  f"Profit: `+{pnl_pct:.2%}`\n" \
                  f"🛡️ *Break-Even Protective SL activated.*"
            asyncio.create_task(self.notifier.send_message(msg))
            self.notifier.notify_trade(symbol, "CLOSE_PARTIAL", current_price, amount_to_close, "AI_TP1")
            logger.warning(f"🛡️ [BREAK-EVEN] AI-Triggered: SL for {symbol} moved to {level['sl']} (Entry+Costs)")
            self.notifier.notify_alert("AI DYNAMIC TP1", f"Gemini ha anticipato il TP1 su {symbol} (+{pnl_pct:.2%})", f"SL spostato in Break-Even.")
            
        except Exception as e:
            logger.error(f"Failed to execute AI scale out for {symbol}: {e}")

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

    async def daily_self_audit_loop(self):
        """Background task to perform AI self-audit every night at 00:00."""
        while True:
            try:
                # Run daily audit
                logger.warning("📊 [DAILY AUDIT] Initiating 24h performance review...")
                # Get last 50 closed trades (within the day)
                history = self.db.get_trades(limit=50)
                await self.analyst.perform_self_audit(history)
                
                # Check every 24 hours
                await asyncio.sleep(86400)
            except Exception as e:
                logger.error(f"Error in daily audit loop: {e}")
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

    async def automated_report_loop(self):
        """Periodically generates the HTML Investor Report."""
        # Initial wait to let bot sync data after restart
        await asyncio.sleep(10)
        while True:
            try:
                logger.info("📊 [REPORTING] Regenerating Investor Track Record...")
                await generate_report.async_generate()
                logger.info("✅ [REPORTING] Report updated successfully.")
            except Exception as e:
                logger.error(f"Error in automated_report_loop: {e}")
            
            # Update every hour
            await asyncio.sleep(3600)
