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
    def __init__(self, config_file=None, profile=None):
        # Load SaaS Client Risk Profiles
        load_dotenv(override=True)
        if config_file is None:
            if profile is None:
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
        self.take_profit_pct = float(params.get("take_profit_pct", 0.04))
        
        # --- NEW: GRANULAR STRATEGIC PARAMETERS ---
        self.breakeven_pct = float(params.get("breakeven_pct", 0.012))
        self.be_trigger_pct = float(params.get("be_trigger_pct", self.breakeven_pct))
        self.max_concurrent_positions = params.get("max_concurrent_positions", 20)
        self.max_global_margin_ratio = params.get("max_global_margin_ratio", 0.75)
        self.daily_loss_limit = float(params.get("daily_loss_limit", 1.0)) / 100.0
        self.panic_drawdown_threshold = float(params.get("panic_drawdown_threshold", self.daily_loss_limit * 2.5))
        
        if self.panic_drawdown_threshold < 0.05:
            self.panic_drawdown_threshold = 0.05 # Minimum 5% hard ceiling
            
        self.gemini_min_confidence = float(params.get("gemini_min_confidence", 0.90))
        self.current_margin_ratio = 0.0
        self.profile_type = self.risk_profile.get("profile_type", "aggressive").lower()
        
        # --- NEW: DYNAMIC TECHNICAL GUARDS (v16.6) ---
        self.adx_threshold = int(params.get("adx_threshold", 25))
        self.ema_trend_filter = bool(params.get("ema_trend_filter", True))
        self.min_volume_24h = float(params.get("min_volume_24h", 5000000))
        self.rsi_buy_level = int(params.get("rsi_buy_level", 30))
        self.rsi_sell_level = int(params.get("rsi_sell_level", 70))
        
        # --- NEW: ADVANCED STRATEGIC MODES (v16.7) ---
        self.trend_penalty_enabled = bool(params.get("trend_penalty_enabled", True))
        self.technical_confluence_mode = str(params.get("technical_confluence_mode", "strict")).lower()
        
        # --- NEW: DYNAMIC STRATEGIC PARAMETERS (v17.0) ---
        strat_params = self.risk_profile.get("strategic_params", {})
        self.llm_cooldown_seconds = int(strat_params.get("llm_cooldown_seconds", 300))
        self.llm_cooldown_bypass = bool(strat_params.get("llm_cooldown_bypass", False))
        self.startup_shield_seconds = int(strat_params.get("startup_shield_seconds", 300))
        self.news_shield_seconds = int(strat_params.get("news_shield_seconds", 600))
        self.min_notional_usdt = float(strat_params.get("min_notional_usdt", 5.0))
        self.leverage_range = strat_params.get("leverage_range", [5, 20])
        
        logger.info(f"🏗️ [PROFILE] Operation Mode: {self.profile_type.upper()} (Penalty: {self.trend_penalty_enabled}, Confluence: {self.technical_confluence_mode.upper()})")
        logger.info(f"⚙️ [STRATEGY] Cooldown: {self.llm_cooldown_seconds}s, Bypass: {self.llm_cooldown_bypass}, Shield: {self.startup_shield_seconds}s, Min Notional: ${self.min_notional_usdt}")
        
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
        
        # State management flags
        self.pending_orders_count = 0
        self.order_lock = asyncio.Lock()
        self.missing_pos_counter: Dict[str, int] = defaultdict(int)
        
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
        
        # --- SCANNER (v11.0 Live) ---
        self.scanner = AssetScanner(self.exchange)
        
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
        
        self.sector_manager = SectorManager()
        self.regime_detector = RegimeDetector()
        self.ai_optimizer = AIParameterOptimizer(self)
        self.current_news = "" # Real-time market sentiment
        
        # Recupero stato persistente
        saved_levels = self.db.load_state("trade_levels")
        if saved_levels:
            # v19.0: Atomic merge to avoid losing state
            for sym, data in saved_levels.items():
                if data: self.trade_levels[sym] = data
            logger.info("📦 Stato trade_levels recuperato correttamente dal database.")
        
        saved_balance = self.db.load_state("initial_balance")
        if saved_balance:
            self.initial_wallet_balance = float(saved_balance)
            logger.info(f"🏦 Bilancio iniziale recuperato: {self.initial_wallet_balance:.2f} USDT")
        
        self.initialized = False

    async def _atomic_position_check(self, symbol):
        """
        [ATOMIC GUARD - v19.0] 
        Performs a real-time exchange check to prevent hedging and double entry.
        Must be called INSIDE the order lock.
        """
        try:
            logger.info(f"🔍 [ATOMIC GUARD] Verifying exchange state for {symbol}...")
            # Use fetch_positions for highest reliability on derivatives
            raw_pos = await self.exchange.fetch_positions([symbol])
            
            # Defensive drilling: Support multiple Bitget/Binance return formats
            matching = next((p for p in raw_pos if p.get('symbol') == symbol and float(p.get('contracts', 0) or p.get('positionAmt', 0)) != 0), None)
            
            if matching:
                side = 'LONG' if float(matching.get('contracts', matching.get('positionAmt', 0))) > 0 else 'SHORT'
                logger.warning(f"🚫 [ATOMIC GUARD] {symbol} already has an active {side} position. Blocking NEW entry.")
                # Update local state immediately to prevent future race conditions
                self.active_positions[symbol] = side
                return True, side
            
            return False, None
        except Exception as e:
            logger.error(f"⚠️ [ATOMIC GUARD ERROR] Failed to fetch positions for {symbol}: {e}")
            # Fallback to local memory if exchange API is down, safety first
            if self.active_positions.get(symbol):
                return True, self.active_positions.get(symbol)
            return False, None

    async def _reconcile_orphan_positions(self):
        """
        [RECOVERY ENGINE - v20.0] 
        Identifies positions on the exchange missing from trade_levels and adopts them.
        Ensures Stop Loss (SL) is applied to all active trades, including Bitget XXXX/USDT:USDT format.
        """
        try:
            logger.info("🔄 [RECOVERY] Scanning for orphaned positions...")
            # CCXT Unified: fetch_positions with symbols is more stable on Bitget
            my_symbols = [s for s in self.exchange.symbols if '/USDT:USDT' in s or '/USDT' in s]
            
            try:
                all_raw = await self.exchange.fetch_positions(my_symbols)
            except Exception as e:
                logger.warning(f"⚠️ fetch_positions with symbols failed: {e}. Falling back to fetch_balance info.")
                bal = await self.exchange.fetch_balance()
                info = bal.get('info', [])
                all_raw = info if isinstance(info, list) else info.get('positions', [])
            
            adopted_count = 0
            for p in all_raw:
                # 1. Normalize Symbol (Bitget XXXX/USDT:USDT -> XXXX/USDT)
                raw_sym = p.get('symbol', p.get('instId'))
                if not raw_sym: continue
                
                # Strip Bitget's complex suffix for internal tracking if needed
                symbol = raw_sym.replace(':USDT', '') if ':USDT' in raw_sym else raw_sym
                p_amt = float(p.get('total', p.get('positionAmt', p.get('contracts', 0))))
                
                if p_amt != 0:
                    # Check memory and DB
                    if symbol not in self.trade_levels or self.trade_levels[symbol] is None:
                        logger.warning(f"🧟 [RECOVERY] Orphan found: {symbol} ({p_amt}). Adopting now (v20.0)...")
                        
                        entry_price = float(p.get('entryPrice', p.get('avgPrice', 0)))
                        if entry_price <= 0:
                            ticker = await self.exchange.fetch_ticker(symbol)
                            entry_price = ticker.get('last', 0)

                        side = 'buy' if p_amt > 0 else 'sell'
                        
                        is_long = side == 'buy'
                        sl_price = entry_price * (1 - self.stop_loss_pct if is_long else 1 + self.stop_loss_pct)
                        tp1_price = entry_price * (1 + self.take_profit_pct/2 if is_long else 1 - self.take_profit_pct/2)
                        tp2_price = entry_price * (1 + self.take_profit_pct if is_long else 1 - self.take_profit_pct)
                        
                        # Apply current profile's risk parameters
                        self.trade_levels[symbol] = {
                            "symbol": symbol,
                            "side": side,
                            "entry_price": entry_price,
                            "amount": abs(p_amt),
                            "sl": sl_price,
                            "tp1": tp1_price,
                            "tp2": tp2_price,
                            "status": "RECOVERED_ZOMBIE",
                            "opened_at": time.time(),
                            "profile_type": self.profile_type
                        }
                        self.active_positions[symbol] = 'LONG' if is_long else 'SHORT'
                        adopted_count += 1
                        logger.info(f"✅ [RECOVERY] {symbol} adopted. SL: {sl_price}")

                        # [FIX TP/SL] Push Hard TP/SL to exchange for orphans
                        try:
                            # Use proper precision
                            sl_price_fmt = float(self.exchange.price_to_precision(symbol, sl_price))
                            tp_price_fmt = float(self.exchange.price_to_precision(symbol, tp2_price))
                            
                            close_side = 'sell' if is_long else 'buy'
                            hold_side = 'long' if is_long else 'short'
                            
                            # 1. Place SL
                            sl_params = {'stopPrice': sl_price_fmt, 'triggerPrice': sl_price_fmt, 'reduceOnly': True, 'triggerType': 'mark_price'}
                            if self.active_exchange_name == "bitget":
                                sl_params.update({'holdSide': hold_side, 'marginCoin': 'USDT', 'planType': 'loss_plan'})
                            
                            logger.info(f"⚙️ [ORPHAN RECOVERY] Placing Hard SL for {symbol} at {sl_price_fmt}")
                            await self.exchange.create_order(symbol, 'market', close_side, abs(p_amt), params=sl_params)
                            
                            # 2. Place TP
                            tp_params = {'stopPrice': tp_price_fmt, 'triggerPrice': tp_price_fmt, 'reduceOnly': True, 'triggerType': 'mark_price'}
                            if self.active_exchange_name == "bitget":
                                tp_params.update({'holdSide': hold_side, 'marginCoin': 'USDT', 'planType': 'profit_plan'})
                            
                            logger.info(f"⚙️ [ORPHAN RECOVERY] Placing Hard TP for {symbol} at {tp_price_fmt}")
                            await self.exchange.create_order(symbol, 'market', close_side, abs(p_amt), params=tp_params)
                        except Exception as e:
                            logger.error(f"⚠️ [ORPHAN RECOVERY] Failed to place Hard TP/SL for {symbol}: {e}")

            if adopted_count > 0:
                self.db.save_state("trade_levels", self.trade_levels)
                # v20.1: Immediately notify and sync
                logger.warning(f"✅ [RECOVERY] Successfully adopted {adopted_count} active positions into trade_levels.")

        except Exception as e:
            logger.error(f"❌ [RECOVERY ERROR] Failed to reconcile orphans: {e}")
            traceback.print_exc()

    def _get_data_provider(self, symbol):
        """Returns the data provider for a given symbol."""
        return self.exchange

    async def initialize(self, skip_leverage=False):
        if self.initialized:
            return
            
        try:
            logger.info(f"📡 Loading markets from {self.active_exchange_name} (Async)...")
            await self.exchange.load_markets()
            logger.info("✅ Markets loaded successfully.")
            
            # --- DYNAMIC SYMBOL BRIDGE ---
            # Maps human-readable config symbols to exchange-specific ones
            self.symbols = self._apply_symbol_bridge(self.symbols)
            logger.info(f"🎯 Validated {len(self.symbols)} active symbols.")
            
            # --- PORTFOLIO ADOPTION (v11.0 Live) ---
            try:
                logger.info(f"🏦 [SYNC] Recovering active positions from {self.active_exchange_name} account...")
                balance_data = await self.exchange.fetch_balance()
                
                # Use ccxt unified 'positions' if available, fallback for Binance/Bitget info
                positions = balance_data.get('positions', [])
                if not positions and 'info' in balance_data:
                    info = balance_data['info']
                    if isinstance(info, list): # Bitget V2 fallback
                        positions = info
                    else:
                        positions = info.get('positions', []) # Binance fallback
                
                recovered_count = 0
                for pos in positions:
                    # Logic here targets Bitget/Binance unified balance data
                    p_amt = float(pos.get('positionAmt', pos.get('total', 0)))
                    if p_amt != 0:
                        p_id = pos.get('symbol', pos.get('instId'))
                        # Find the corresponding ccxt symbol
                        full_symbol = next((s for s, m in self.exchange.markets.items() if m['id'] == p_id), None)
                        if full_symbol:
                            side = 'LONG' if p_amt > 0 else 'SHORT'
                            if full_symbol not in self.symbols:
                                self.symbols.append(full_symbol)
                                # Initialize structures
                                if full_symbol not in self.latest_data:
                                    self.latest_data[full_symbol] = {'price': abs(float(pos.get('avgPrice', 0))), 'prediction': 'Initial Sync...'}
                            recovered_count += 1
                if recovered_count > 0:
                    logger.info(f"✅ Recovered {recovered_count} active positions. Adapting to {self.profile_type.upper()}...")
                
                # v20.1: [CRITICAL] Orphan recovery MUST run regardless of fetch_balance results
                # This is the primary engine for Bitget position adoption
                await self._reconcile_orphan_positions()
                
                if recovered_count > 0 or any(self.trade_levels.values()):
                    # v15.2: Async task for background reconciliation and SL checks
                    asyncio.create_task(self._reconcile_active_trades())
                    logger.warning("✅ [RECOVERY] Recovery cycle triggered for active positions.")
            except Exception as e:
                logger.error(f"⚠️ [RECOVERY ERROR] Position sync skipped: {e}")

            # --- NEW: Restart Notification (v10.1) ---
            asyncio.create_task(self.notifier.send_message("🚀 *SISTEMA RIAVVIATO*\nIl bot è ora operativo con la configurazione v10.0 Audit."))

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
                # v17.3: Dynamic floor protection (max of target or config floor)
                target_lev = max(self.leverage, self.leverage_range[0])
                await self.exchange.set_leverage(target_lev, market['id'])
                logger.info(f"Set leverage to {target_lev}x for {symbol}")
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
                    sentiment_api = self.exchange
                    oi_data = await sentiment_api.fetch_open_interest(symbol)
                    current_oi = float(oi_data.get('openInterestAmount', 0))
                    
                    ls_data = await sentiment_api.fapiDataGetGlobalLongShortAccountRatio({'symbol': symbol.replace('/', '').split(':')[0], 'period': '5m', 'limit': 1})
                    current_ls = float(ls_data[0]['longShortRatio']) if ls_data else 1.0
                except: pass

                funding = await provider.fetch_funding_rate(symbol)
                funding_rate = float(funding.get('fundingRate', 0))

                # Update latest data record
                self.latest_data[symbol] = {
                    'price': current_close,
                    'prev_price': prev_close,
                    'rsi': round(current_rsi, 2) if not pd.isna(current_rsi) else "N/A",
                    'macd_hist': round(current_macd_hist, 6) if not pd.isna(current_macd_hist) else 0,
                    'atr': round(current_atr, 6) if not pd.isna(current_atr) else 0,
                    'ema200': round(current_ema200, 6) if not pd.isna(current_ema200) else 0,
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
        # 1. Volume Filter (Profile-based)
        if volume24h < self.min_volume_24h:
            return

        # 2. Classic Technical Strategy (RSI + MACD + Bollinger)
        is_technical_signal = False
        tech_side = None
        
        # --- ADX TREND FILTER (Profile-based) ---
        is_trending = adx > self.adx_threshold if not pd.isna(adx) else True
        
        # --- STRONGER TECHNICAL CONFLUENCE: RSI + MACD Hist + Bollinger ---
        # Long Confluence
        is_rsi_buy = rsi <= self.rsi_buy_level
        is_macd_buy = macd_hist > -0.0001 and macd_hist > self.latest_data[symbol].get('macd_hist_prev', -1)
        is_bb_buy = price <= (bb_lower * 1.01)
        
        # Decide based on confluence mode
        if self.technical_confluence_mode == "loose":
            # Loose: RSI OR Bollinger is enough
            tech_buy_triggered = is_rsi_buy or is_bb_buy
        else:
            # Strict: RSI AND MACD AND Bollinger
            tech_buy_triggered = is_rsi_buy and is_macd_buy and is_bb_buy

        if tech_buy_triggered:
            # Check EMA Filter (if enabled for profile)
            if (not self.ema_trend_filter or price > ema200) and is_trending: # Trend-following LONG
                tech_side = 'buy'
                is_technical_signal = True
            
        # Short Confluence
        is_rsi_sell = rsi >= self.rsi_sell_level
        is_macd_sell = macd_hist < 0.0001 and macd_hist < self.latest_data[symbol].get('macd_hist_prev', 1)
        is_bb_sell = price >= (bb_upper * 0.99)

        if self.technical_confluence_mode == "loose":
            tech_sell_triggered = is_rsi_sell or is_bb_sell
        else:
            tech_sell_triggered = is_rsi_sell and is_macd_sell and is_bb_sell

        if tech_sell_triggered:
            # --- SHORTING MOMENTUM GUARD ---
            if change_5m_pct > 0.008: 
                logger.info(f"🛡️ [SHORT GUARD] Skipping technical SHORT on {symbol}: Momentum too bullish ({change_5m_pct:.2%})")
            # Check EMA Filter (if enabled for profile)
            elif (not self.ema_trend_filter or price < ema200) and is_trending: # Trend-following SHORT
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
        if symbol not in self.exchange.markets:
            logger.error(f"❌ {symbol} not found on {self.active_exchange_name}!")
            return
                
        # --- CONSENSUS ENGINE: Routing signals to SignalManager ---
        if is_technical_signal:
             asyncio.create_task(self.handle_signal(symbol, "TECH", tech_side, weight_modifier=1.0, current_price=price, ema200=ema200))

        # --- LOGIC GUARD 3: Volatility (ATR-based skip) ---
        # Skip if ATR is less than 0.2% of price (stagnant) or more than 5% (extreme volatility)
        # v17.6: Bypass for Institutional profile (allows long-stagnant strategic holds)
        atr_pct = (atr / price) if price > 0 else 0
        if self.profile_type != 'institutional':
            if atr_pct < 0.002 or atr_pct > 0.05:
                # logger.debug(f"📐 [STAGNATION SHIELD] Filtering {symbol} (ATR: {atr_pct:.4%})")
                return
        else:
            if atr_pct > 0.08: # Even Institutional has a hard cap for technical madness
                return

        # 3. AI Predictive Alpha (Machine Learning Classifier)
        prediction = self.latest_data[symbol].get('prediction')
        if isinstance(prediction, dict) and 'direction' in prediction:
            direction = prediction['direction']
            confidence = prediction['confidence']
            # Confidence Threshold: 0.70 (Increased from 0.55 for higher quality)
            price_above_ema = price > ema200 if ema200 else True
            if direction == 1 and confidence > 0.70 and price_above_ema:
                asyncio.create_task(self.handle_signal(symbol, "AI", "buy", weight_modifier=1.2, current_price=price, ema200=ema200, ai_confidence=confidence, mtf_context=mtf_context))
            elif direction == 0 and confidence > 0.70 and not price_above_ema:
                asyncio.create_task(self.handle_signal(symbol, "AI", "sell", weight_modifier=1.2, current_price=price, ema200=ema200, ai_confidence=confidence, mtf_context=mtf_context))

    async def handle_signal(self, symbol, type, side, weight_modifier=1.0, is_black_swan=False, current_price=None, ema200=None, ai_confidence=0.0, mtf_context="N/A"):
        if not self.signal_manager:
            return
            
        now = time.time()
        
        # --- [V19.0] DEDUPLICATION & CONFLICT RESOLVER ---
        # 1. Check local lock (HFT time-domain)
        if type in ["TECH", "AI"]:
            lock_time = self.hft_locks.get(symbol, 0)
            if now - lock_time < 60: # 60s lock for standard signals
                logger.info(f"⏳ [HFT LOCK] Ignoring strategic {type} signal for {symbol} (HFT priority active).")
                return

        # 2. Check active positions
        existing_pos = self.active_positions.get(symbol)
        if existing_pos:
            existing_side = 'buy' if existing_pos == 'LONG' else 'sell'
            if existing_side == side.lower():
                logger.debug(f"ℹ️ [DEDUPLICATION] Already in {existing_pos} for {symbol}. Skipping redundant signal.")
                return
            
            # --- REVERSAL / FLIP LOGIC ---
            reversal_signals = ["GATEKEEPER", "NEWS", "NEW_LISTING", "EVENT_PUMP", "EVENT_DUMP", "LIQUIDATION"]
            is_reversal = is_black_swan or type in reversal_signals or (type == "AI" and ai_confidence > 0.85)
            
            if not is_reversal:
                logger.warning(f"🚫 [CONFLICT] Existing {existing_pos} for {symbol}. Aborting {side.upper()} {type}.")
                return
            
            # Proceed to reversal (if not shielded)
            if self._is_startup_shield_active():
                logger.info(f"🛡️ [Shield] Skipping Emergency Reversal for {symbol} (Startup Protection active)")
                return

            logger.critical(f"🆘 [REVERSAL] Emergency Flip for {symbol}: Closing {existing_pos} for {side.upper()} {type}")
            current_level = self.trade_levels.get(symbol)
            await self.close_position(symbol, current_level, reason=f"REVERSAL_{type}")
            # Continue to open the new side after closing

        # --- CONSENSUS ENGINE ---
        approved, consensus_score = await self.signal_manager.add_signal(symbol, type, side, weight_modifier, current_price=current_price, ema200=ema200, ai_confidence=ai_confidence)
        
        if approved:
            # If it's an HFT signal, set the lock
            if type in ["LIQUIDATION", "DEX_ARBITRAGE", "NEW_LISTING"]:
                self.hft_locks[symbol] = now
                logger.warning(f"🔒 [TIME-DOMAIN LOCK] Lock ACTIVE for {symbol} (60s).")

            # Check global margin ratio
            if self.current_margin_ratio < self.max_global_margin_ratio or is_black_swan:
                self.active_positions[symbol] = 'LONG' if side.lower() in ['buy', 'long'] else 'SHORT'
                self.pending_orders_count += 1
                
                await self.execute_order(symbol, side.lower(), current_price or 0.0, 
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
        
        # --- [CRITICAL] DYNAMIC TP2: GEMINI CONSULTATION (v17.5) ---
        if trade.get('status') == 'RUNNING_DYNAMIC' and self.profile_type in ['blitz', 'aggressive']:
            last_check = trade.get('last_dynamic_check', 0)
            if time.time() - last_check > 600: # Every 10 minutes (Reduced to save cost)
                trade['last_dynamic_check'] = time.time()
                # Run evaluation in background to avoid blocking HFT loop
                asyncio.create_task(self._handle_dynamic_tp2_evaluation(symbol, trade, current_price))
            
            # While in Dynamic Mode, we also enforce a Tightening Trailing Stop (1.5% from ATH)
            ath = trade.get('highest_price', current_price) if is_long else trade.get('lowest_price', current_price)
            trailing_floor = ath * 0.985 if is_long else ath * 1.015
            if (is_long and current_price < trailing_floor) or (not is_long and current_price > trailing_floor):
                logger.warning(f"🔔 [DYNAMIC TRAILING HIT] {symbol} hit protective floor {trailing_floor:.4f}. Closing last 50%.")
                asyncio.create_task(self.close_position(symbol, trade, reason="DYNAMIC_TRAILING_EXIT"))
                return
        
        # Determine Stop-Loss and Take-Profit Levels
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
            
            # --- PROFILE-SPECIFIC BREAK-EVEN (v14.5) ---
            pnl_pct = (current_price - entry_price) / entry_price
            be_threshold = self.be_trigger_pct
            
            if pnl_pct >= be_threshold and not trade.get('be_hit', False):
                # Move SL to entry + 0.1% buffer to cover fees
                trade['sl'] = entry_price * 1.001 
                trade['be_hit'] = True
                logger.warning(f"🛡️ [BREAK-EVEN {self.profile_type.upper()}] {symbol} profit reached {be_threshold:.1%}. SL moved to entry ({trade['sl']})")

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
                    # ENFORCE MINIMUM TP1 profit (0.5% for Blitz, 1% others)
                    pnl_tp1 = (current_price - entry_price) / entry_price
                    min_tp1 = 0.005 if self.profile_type in ['aggressive', 'extreme'] else 0.010
                    if pnl_tp1 < min_tp1: 
                         logger.info(f"⏳ [TP1 GUARD] Skipping premature TP1 for {symbol}: {pnl_tp1:.2%} < {min_tp1:.2%} | Current: {current_price} | TP1: {tp1}")
                    else:
                        # Aggressive Recovery Exit (v9.8.5)
                        # If it's a recovered zombie and ROI > 20%, close 100% immediately
                        # Aggressive Recovery Exit (v17.5 Dynamic)
                        # If it's a recovered zombie and ROI > 20%, we hit TP1 and enter DYNAMIC mode for the rest (maximize profit)
                        roe_pct = pnl_tp1 * getattr(self, 'leverage', 1.0)
                        
                        # v17.16: Recognition of all Recovery versions (V1, V11, etc)
                        is_recov = str(trade.get('status', '')).startswith('RECOVERED_ZOMBIE')
                        
                        if is_recov and roe_pct > 0.15:
                             if self.profile_type in ['blitz', 'aggressive']:
                                 logger.warning(f"🚀 [DYNAMIC ZOMBIE ADOPTION] {symbol} at {roe_pct:.2%} ROE. Closing 50% (TP1) and moving to DYNAMIC TP2.")
                                 asyncio.create_task(self.close_position(symbol, trade, partial_pct=0.5, reason="DYNAMIC_ZOMBIE_TP1"))
                                 trade['tp1_hit'] = True
                                 trade['status'] = 'RUNNING_DYNAMIC'
                                 trade['last_dynamic_check'] = time.time()
                                 # Move SL to entry + 0.5% (Safe)
                                 trade['sl'] = entry_price * 1.005
                                 return
                             else:
                                 logger.warning(f"🆘 [AGGRESSIVE ZOMBIE EXIT] {symbol} at {roe_pct:.2%} ROE (>20%). Closing 100% (TP2).")
                                 asyncio.create_task(self.close_position(symbol, trade, partial_pct=1.0, reason="AGGRESSIVE_ZOMBIE_EXIT"))
                                 return 
                        
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
            
            # --- PROFILE-SPECIFIC BREAK-EVEN (v14.5 SHORT) ---
            pnl_pct = (entry_price - current_price) / entry_price
            be_threshold = self.be_trigger_pct
            
            if pnl_pct >= be_threshold and not trade.get('be_hit', False):
                # Move SL to entry - 0.1% buffer
                trade['sl'] = entry_price * 0.999
                trade['be_hit'] = True
                logger.warning(f"🛡️ [BREAK-EVEN {self.profile_type.upper()} SHORT] {symbol} profit reached {be_threshold:.1%}. SL moved to entry ({trade['sl']})")

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
                    # ENFORCE MINIMUM TP1 profit (0.5% for Blitz, 1% others)
                    pnl_tp1 = (entry_price - current_price) / entry_price
                    min_tp1 = 0.005 if self.profile_type in ['aggressive', 'extreme'] else 0.010
                    if pnl_tp1 < min_tp1:
                         logger.info(f"⏳ [TP1 GUARD] Skipping premature TP1 for {symbol}: {pnl_tp1:.2%} < {min_tp1:.2%}")
                    else:
                        # Aggressive Recovery Exit (v9.8.5)
                        # If it's a recovered zombie and ROI > 20%, close 100% immediately
                        # Aggressive Recovery Exit (v17.5 Dynamic)
                        roe_pct = pnl_tp1 * getattr(self, 'leverage', 1.0)
                        if trade.get('status') == 'RECOVERED_ZOMBIE' and roe_pct > 0.20:
                             if self.profile_type in ['blitz', 'aggressive']:
                                 logger.warning(f"🚀 [DYNAMIC ZOMBIE ADOPTION] {symbol} at {roe_pct:.2%} ROE. Closing 50% (TP1) and moving to DYNAMIC TP2.")
                                 asyncio.create_task(self.close_position(symbol, trade, partial_pct=0.5, reason="DYNAMIC_ZOMBIE_TP1"))
                                 trade['tp1_hit'] = True
                                 trade['status'] = 'RUNNING_DYNAMIC'
                                 trade['last_dynamic_check'] = time.time()
                                 trade['sl'] = entry_price * 0.995 # Protective move for shorts
                                 return
                             else:
                                 logger.warning(f"🆘 [AGGRESSIVE ZOMBIE EXIT] {symbol} at {roe_pct:.2%} ROE (>20%). Closing 100% (TP2).")
                                 asyncio.create_task(self.close_position(symbol, trade, partial_pct=1.0, reason="AGGRESSIVE_ZOMBIE_EXIT"))
                                 return 

                        logger.warning(f"🎯 PARTIAL TP1 HIT for {symbol} at {current_price} (+{pnl_tp1:.2%})")
                        asyncio.create_task(self.close_position(symbol, trade, partial_pct=0.5, reason="PARTIAL_EXIT_TP1"))
                        trade['tp1_hit'] = True
                        # --- [CRITICAL] DYNAMIC TP2 ACTIVATION (v17.5) ---
                        # For Blitz and Aggressive, we don't close 100% at TP2.
                        # We enter Gemini-Managed Dynamic mode for the last 50%.
                        if self.profile_type in ['blitz', 'aggressive']:
                            trade['status'] = 'RUNNING_DYNAMIC'
                            trade['last_dynamic_check'] = time.time()
                            logger.warning(f"🚀 [DYNAMIC TP2 Alpha] {symbol} is now managed by Gemini for the last 50%.")
                        
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

    async def _handle_dynamic_tp2_evaluation(self, symbol, trade, current_price):
        """Consults Gemini to decide if we should hold or close the last 50% (v17.5)."""
        try:
            logger.info(f"🧠 [AI TP2] Consulting Gemini for dynamic exit evaluation on {symbol}...")
            
            # Fetch indicators for the symbol
            indicators = self.latest_data.get(symbol, {})
            pnl_pct = (current_price - trade['entry_price']) / trade['entry_price'] if trade['side'] == 'long' else (trade['entry_price'] - current_price) / trade['entry_price']
            
            action, confidence, reasoning = await self.analyst.evaluate_active_position(symbol, trade['side'], indicators, pnl_pct * 100)
            
            if action in ["CLOSE", "PIVOT"] and confidence >= 0.70:
                logger.warning(f"🆘 [DYNAMIC TP2 EXIT] Gemini signals exhaustion for {symbol}: {reasoning}. Closing now.")
                await self.close_position(symbol, trade, reason=f"DYNAMIC_AI_EXIT_{action}")
            else:
                logger.debug(f"💎 [DYNAMIC TP2 HOLD] Gemini suggests {action} for {symbol}: {reasoning}. Trend remains valid.")
                
        except Exception as e:
            logger.error(f"❌ Error during Dynamic TP2 evaluation for {symbol}: {e}")

    def _standardize_order_response(self, response):
        """Recursively drills down into nested lists to find a dictionary response (v17.4 Robust)."""
        if isinstance(response, list):
            if len(response) > 0:
                return self._standardize_order_response(response[0])
            else:
                return {}
        if isinstance(response, dict):
            return response
        return {}

    async def close_position(self, symbol, trade, partial_pct=1.0, reason="EXIT"):
        try:
            side = str(trade.get('side', 'buy')).lower()
            is_long = side in ['buy', 'long']
            close_side = 'sell' if is_long else 'buy'
            
            # --- ROBUST AMOUNT EXTRACTION (Binance/Bitget/Testnet Compat v9.9.3) ---
            total_amount = 0.0
            if trade and 'amount' in trade and trade['amount'] is not None:
                total_amount = float(trade['amount'])
            
            # If trade state is missing or corrupted
            if total_amount <= 0:
                logger.warning(f"⚠️ [RECOVERY] Trade state for {symbol} is missing amount. Fetching from Balance...")
                # v17.7: Ultra-Robust Balance/Position Extraction
                bal_raw = await self.exchange.fetch_balance()
                # Defensive drill-down
                if isinstance(bal_raw, list):
                    bal = bal_raw[0] if len(bal_raw) > 0 else {}
                else:
                    bal = bal_raw
                
                # Double-check bal is now a dict
                if not isinstance(bal, dict):
                    bal = {}
                    
                info = bal.get('info', {})
                if isinstance(info, list): 
                    info = info[0] if len(info) > 0 else {}
                
                raw_positions = info.get('positions', [])
                if not raw_positions:
                     # Check 'data' as fallback
                     data = bal.get('data', {})
                     if isinstance(data, list): data = data[0] if len(data) > 0 else {}
                     raw_positions = data.get('positions', [])
                     
                for p in raw_positions:
                    if not isinstance(p, dict): continue
                    p_id = p.get('symbol', p.get('instId'))
                    # Match by ID or CCXT symbol
                    market = self.exchange.markets.get(symbol, {})
                    if p_id == symbol or p_id == market.get('id'):
                         # v17.18.2: Try 'total' or 'size' for Bitget V2
                         total_amount = abs(float(p.get('positionAmt', p.get('total', p.get('size', 0)))))
                         break

            amount_to_close = total_amount * partial_pct
            
            # Precision adjustment with safety fallback (v9.9.3)
            try:
                amount_str = self.exchange.amount_to_precision(symbol, amount_to_close)
                amount_to_close = float(amount_str)
            except Exception as e:
                logger.warning(f"⚠️ [PRECISION ERROR] {symbol} metadata broken: {e}. Falling back to raw amount.")
                # Fallback: Round to 8 decimals as a safety ceiling
                amount_to_close = round(amount_to_close, 8)
            
            if amount_to_close <= 0:
                logger.warning(f"Skipping closure for {symbol}: amount too small ({total_amount}).")
                return

            logger.info(f"🔄 Closing position for {symbol} ({'PARTIAL' if partial_pct < 1 else 'FULL'}): {close_side} {amount_to_close} (Reason: {reason})")
            
            # --- EMERGENCY EXECUTION BLOCK (v9.9.3) ---
            try:
                # v17.17 Bitget-Hedged-Mode Exit Fix: Force holdSide and marginCoin
                params = {'reduceOnly': True}
                if self.active_exchange_name == "bitget":
                    params.update({'holdSide': side, 'marginCoin': 'USDT'})
                
                order_response = await self.exchange.create_order(symbol, 'market', close_side, amount_to_close, None, params)
            except Exception as e:
                # v9.9.3 TRIA/USDT FIX: If "greater than 0" or precision error, try integer-only fallback
                if "precision" in str(e).lower() or "greater than 0" in str(e).lower():
                    logger.warning(f"🆘 [FALLBACK EXIT] Attempting integer-only close for {symbol} due to API Error.")
                    amount_to_close = int(amount_to_close)
                    if amount_to_close > 0:
                         # v17.17 Bitget-Hedged-Mode Exit Fix: Force holdSide and marginCoin
                         params = {'reduceOnly': True}
                         if self.active_exchange_name == "bitget":
                             params.update({'holdSide': side, 'marginCoin': 'USDT'})
                         
                         order_response = await self.exchange.create_order(symbol, 'market', close_side, amount_to_close, None, params)
                    else:
                         raise e
                else:
                    raise e
            
            # Extract execution details using Robust Standardization (v17.4)
            order_response = self._standardize_order_response(order_response)
            order_id = order_response.get('id')
            # Bitget/CCXT might return None for price or average in market orders
            raw_avg = order_response.get('average')
            raw_price = order_response.get('price')
            execution_price = float(raw_avg if raw_avg is not None else (raw_price if raw_price is not None else 0))
            
            # Fallback for execution price if not in response (v17.20 Bitget Sync)
            if execution_price <= 0:
                # 1. Try last known price from latest_data
                execution_price = self.latest_data[symbol].get('price', 0) if symbol in self.latest_data else 0
                
                # 2. If still 0 (e.g. recovered zombie), fetch fresh ticker (v17.20 Absolute Fail-safe)
                if execution_price <= 0:
                    try:
                        ticker = await self.exchange.fetch_ticker(symbol)
                        execution_price = float(ticker.get('last') or ticker.get('close') or 0)
                        logger.info(f"🔍 [PRICE SYNC] Fetched fresh ticker for {symbol} exit: {execution_price}")
                    except: pass
                
                if execution_price <= 0:
                    # 3. Final fallback to entry_price to show 0% instead of -100%
                    execution_price = float(trade.get('entry_price', 0))
                    logger.warning(f"⚠️ [REPORTING FAIL] Could not get execution price for {symbol}. Using entry price as fallback.")
            
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
            
            # Finalize AI Memory (Outcome learning)
            if trade and 'snapshot_id' in trade:
                self.db.update_trade_outcome(trade['snapshot_id'], execution_price, pnl)
            
            # Use provided reason or fallback to default
            if not reason:
                reason = "EXIT" if partial_pct >= 1.0 else "PARTIAL_EXIT"
            
            # Log with exchange_trade_id to prevent duplicates in sync_binance_trades
            # Capture Real Exit Price for Shadow PnL
            real_exit_price = 0.0
            # Standard Ticker Path
            ticker = await self.exchange.fetch_ticker(symbol)
            real_exit_price = float(ticker.get('last') or ticker.get('close') or 0)

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
        score_weight = (consensus_score / 10.0) if consensus_score else 0.5
        
        is_aggressive = risk_pct >= 8.0 
        dampening_factor = 1.0 if is_aggressive else 0.5 
        
        # v13.0 Dynamic Aggressive: 10% to 20% mapping
        if is_aggressive:
             # v17.6: Aggressive Profile - No dampening, high daring.
             # Use risk_pct directly as the base without AI modification unless AI is extremely high
             ai_mod = max(1.0, raw_ai_mod) 
             logger.info(f"🚀 [SIZING] Aggressive Profile: Base {risk_pct}% * Mod {ai_mod:.2f} = {risk_pct * ai_mod:.2f}%")
        else:
             ai_mod = 1.0 + (raw_ai_mod - 1.0) * dampening_factor * score_weight
             # v10.5 Conservative refinement: 3% base, up to 10% max (mod 3.33)
             ai_mod = max(0.8, min(3.33, ai_mod))
        
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
        
        # v17.6: For Aggressive profiles, we don't reduce size for low ATR (daring mode)
        if is_aggressive:
            volatility_modifier = max(1.0, volatility_modifier)
            regime_mult = max(1.0, regime_mult)
            
        adjusted_usdt = base_usdt * volatility_modifier * regime_mult
        
        # v17.6 Debug Sizing (Visible in logs)
        logger.info(f"📊 [SIZING DATA] {symbol} | Base: ${base_usdt:.2f} | Final: ${adjusted_usdt:.2f} | Equity: ${current_equity:.2f} | Risk%: {total_pct:.1f}%")
        
        # --- GLOBAL SAFETY CAP (DYNAMIC v9.7.4) ---
        max_limit_val = getattr(self, 'max_margin_pct', 15.0)
        max_limit_usdt = current_equity * (max_limit_val / 100.0)
        
        if adjusted_usdt > max_limit_usdt:
             logger.warning(f"🛡️ [SIZING CAP] Capping margin to {max_limit_val}%: {max_limit_usdt:.2f} USDT.")
             adjusted_usdt = max_limit_usdt

        # --- DYNAMIC FLOOR (v13.5: Notional-Aware) ---
        # Low floor (0.50 USDT) to allow multiple positions on small accounts.
        # Exchange minimums are handled later by the purchasing_power floor.
        min_margin = max(0.50, current_equity * 0.005) 
        if adjusted_usdt < min_margin and not self.circuit_breaker_active:
             adjusted_usdt = min_margin

        # --- EMERGENCY GLOBAL MARGIN CAP (v15.2 Strategic) ---
        # Hard stop if we are already using > Profile Max (Institutional 20%, Extreme 95%, etc)
        current_used_margin = self.latest_account_data.get('used_margin', 0)
        global_cap = current_equity * self.max_global_margin_ratio
        if current_used_margin > global_cap:
             logger.error(f"☢️ [EMERGENCY] Global Margin Usage is too high ({current_used_margin:.2f} / {global_cap:.2f} Cap). Blocking NEW entry.")
             return 0.0

        # --- DYNAMIC LEVERAGE CLAMP (v17.4 Core Floor) ---
        min_lev = getattr(self, 'leverage_range', [1.0, 50.0])[0]
        max_lev = getattr(self, 'leverage_range', [1.0, 50.0])[1]
        
        # v17.7: Strong Floor Enforcement
        clamped_leverage = max(min_lev, min(max_lev, leverage))
        
        if raw_ai_mod >= 1.5 and leverage > max_lev:
             clamped_leverage = min(max_lev, leverage)
             logger.info(f"🔥 [AI LEVERAGE BOOST] Conviction High: Allowing {clamped_leverage}x leverage for {symbol}.")
        
        # Final Force Floor (Strategic)
        clamped_leverage = max(clamped_leverage, min_lev)
        logger.info(f"📐 [LEVERAGE] {symbol}: Planning {clamped_leverage}x (Base: {leverage}x, Range: {min_lev}-{max_lev}x)")

        # --- [FIX] PORTFOLIO-AWARE SIZING (v9.9.0) ---
        # 1. Get existing notional for this symbol
        positions = self.latest_account_data.get('positions', [])
        current_notional = 0.0
        for p in positions:
            p_sym = p.get('symbol')
            # Match both direct symbol and base symbol (USDT-M conventions)
            if p_sym == symbol or p_sym == symbol.split(':')[0]:
                 current_notional = abs(float(p.get('notional', 0)))
                 break

        # 2. Calculate Max Notional Allowed (e.g. 10% of equity * leverage)
        max_notional_allowed = current_equity * (max_limit_val / 100.0) * clamped_leverage
        
        # 3. Check and Clamp New Order
        potential_notional = adjusted_usdt * clamped_leverage
        if (current_notional + potential_notional) > max_notional_allowed:
             remaining_notional = max(0.0, max_notional_allowed - current_notional)
             if remaining_notional <= 5.0: # Too small to trade
                  logger.warning(f"🛡️ [EXPOSURE LIMIT] Maximum capacity reached for {symbol} (${current_notional:.2f}). Blocking NEW entry.")
                  return 0.0
             
             adjusted_usdt = remaining_notional / clamped_leverage
             logger.warning(f"🛡️ [SIZING CAP] Global Exposure limit reached for {symbol}. Reducing new order to ${remaining_notional:.2f} Notional.")

        if current_price <= 0:
            logger.error(f"❌ Impossibile calcolare l'ordine per {symbol}: prezzo non disponibile (0).")
            return 0.0

        # RE-CALCULATE amount with final leverage (v9.8.9 Fix)
        amount_to_buy = (adjusted_usdt * clamped_leverage) / current_price
        
        purchasing_power = adjusted_usdt * clamped_leverage
        
        if purchasing_power < self.min_notional_usdt:
            purchasing_power = self.min_notional_usdt


            
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
        
        # 4. Dynamic Floor Based on Confidence (v13.0 Aggressive)
        # Using profile-defined leverage range
        floor_lev = self.leverage_range[0]
        
        # v17.8: BLITZ/EXTREME Hard Floor Enforcement (Always at least 15)
        if self.profile_type in ['blitz', 'extreme']:
            floor_lev = max(floor_lev, 15)
            
        if not is_aggressive:
            if consensus_score >= 9.2:
                floor_lev = max(floor_lev, 10)
            elif consensus_score >= 8.5:
                floor_lev = max(floor_lev, 8)
            
        # 5. Major-Aware Caps (BTC/ETH up to 75x for Aggressive/Blitz)
        is_major = any(m in symbol.upper() for m in ["BTC/", "ETH/", "BITCOIN", "ETHEREUM"])
        final_max = self.leverage_range[1]
        if (is_aggressive or self.profile_type == 'blitz') and is_major:
            final_max = 75
            
        final_lev = max(floor_lev, min(final_max, round(lev_calc)))
        
        # v19.0: FORCE Blitz Floor (Emergency reinforcement)
        if self.profile_type == 'blitz':
            final_lev = max(final_lev, 15)
            
        logger.info(f"📐 [Leverage] {symbol} (v19.0): AI={ai_lev}, Trust={score_trust:.2f}, Major={is_major}, Range=[{floor_lev}0x - {final_max}x] -> Final={final_lev}x")
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

                # --- [FIX] ATOMIC POSITION GUARD (v19.0) ---
                # Real-time exchange check inside the lock to absolute-prevent hedging
                has_pos, existing_side = await self._atomic_position_check(symbol)
                if has_pos:
                    logger.warning(f"🚫 [ATOMIC STOP] {symbol} already in {existing_side}. Aborting new {side.upper()} order.")
                    return

                # --- NEW SAFETY GUARD: Total Symbol Exposure Cap ---
                current_equity = self.latest_account_data.get('equity', 0)
                
                # v11.5: LIQUIDITY GUARD - Check spread for listings and high-risk entries

                if signal_type == "NEW_LISTING" or (isinstance(is_black_swan, bool) and is_black_swan):
                    try:
                        orderbook = await self.exchange.fetch_order_book(symbol, limit=5)
                        bids = orderbook['bids']
                        asks = orderbook['asks']
                        if bids and asks:
                            best_bid = bids[0][0]
                            best_ask = asks[0][0]
                            real_spread = (best_ask - best_bid) / best_bid * 100
                            if real_spread > 2.5:
                                logger.warning(f"🚫 [LIQUIDITY GUARD] Rejecting {symbol}: Spread too high ({real_spread:.2f}% > 2.50%)")
                                self.active_positions[symbol] = None
                                self.pending_orders_count = max(0, self.pending_orders_count - 1)
                                return
                            logger.info(f"✅ [LIQUIDITY GUARD] {symbol} spread is safe: {real_spread:.2f}%")
                    except Exception as e:
                        logger.error(f"⚠️ Liquidity Guard Error for {symbol}: {e}")
                        # If we can't check liquidity, safe-reject the listing
                        if signal_type == "NEW_LISTING":
                            self.active_positions[symbol] = None
                            self.pending_orders_count = max(0, self.pending_orders_count - 1)
                            return
                    # v17.8: ATOMIC POSITION CHECK (Bitget-Specific Upgrade)
                    # Instead of fetch_balance, use fetch_positions for higher reliability in hedge mode
                    logger.info(f"🔍 [ATOMIC CHECK] Verifying real-time exchange position for {symbol}...")
                    
                    try:
                        raw_positions = await self.exchange.fetch_positions([symbol])
                        # Bitget positions list might match symbol or ID
                        matching_position = next((p for p in raw_positions if p.get('symbol') == symbol or p.get('contracts', 0) != 0), None)
                    except Exception as e:
                        logger.error(f"⚠️ fetch_positions failed for {symbol}: {e}. Falling back to balance sync.")
                        fresh_bal = await self.exchange.fetch_balance()
                        info = fresh_bal.get('info', [])
                        raw_positions = info if isinstance(info, list) else info.get('positions', [])
                        market = self.exchange.markets.get(symbol, {})
                        market_id = market.get('id')
                        matching_position = next((p for p in raw_positions if p.get('symbol') == symbol or p.get('symbol') == market_id), None)
                    
                    if matching_position:
                        # Extract amount (contracts for Bitget CCXT unified, total/amount for raw info)
                        amt = float(matching_position.get('contracts', 0)) or float(matching_position.get('positionAmt', 0)) or float(matching_position.get('total', 0)) or float(matching_position.get('amount', 0))
                        is_p_active = abs(amt) > 0
                    else:
                        is_p_active = False
                    
                    if not is_p_active and (self.trade_levels.get(symbol) or self.active_positions.get(symbol)):
                         # Internal state says active but Exchange says empty -> Manual closure recovery
                         logger.warning(f"🔓 [RECOVERY] {symbol} found empty on exchange. Clearing internal state for entry.")
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
                        # --- TREND-FLIP GUARD (v17.8) ---
                        # In Bitget Hedge Mode, we MUST close the opposite side before opening new one.
                        # Determine current side from exchange data
                        raw_side = matching_position.get('side', '').upper() # LONG/SHORT
                        if not raw_side:
                             # Fallback for raw info
                             amt_val = float(matching_position.get('positionAmt', matching_position.get('total', 0)) or 0)
                             raw_side = 'LONG' if amt_val > 0 else 'SHORT'
                             
                        current_side = raw_side
                        new_side_norm = 'LONG' if side.lower() in ['buy', 'long'] else 'SHORT'
                        
                        if current_side != new_side_norm:
                             logger.warning(f"🔄 [TREND FLIP] {symbol} Reversal: Closing {current_side} before entering {new_side_norm}.")
                             # Retrieve the local trade object for closure
                             existing_trade = self.trade_levels.get(symbol)
                             if existing_trade:
                                  await self.close_position(symbol, existing_trade, reason="TREND_FLIP_FLUSH")
                                  # Reset local state to allow the new entry logic to proceed normally
                                  self.trade_levels[symbol] = None
                                  is_already_trading = False 
                        
                        # Only proceed with Anti-Pyramiding if we didn't just flip
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
                
                # Cooldown Duration: Dynamic from config
                cooldown_active = (now - last_review) < self.llm_cooldown_seconds if (0 < last_review < now) else False
                
                # Check for "Future Scheduled Cooldowns" (Negative time bug fix)
                if last_review > now:
                     cooldown_active = True
                
                # v11.6.1: Startup Audit Bypass - Allow LLM review for everything in the first N seconds
                is_startup_window = (now - self.start_time) < self.startup_shield_seconds
                
                # v17.0: Dynamic Cooldown Bypass from config
                is_cooldown_bypass = self.llm_cooldown_bypass
                
                # Logic: If bypass is active, we NEVER enter this skip block.
                if not is_cooldown_bypass:
                    if (cooldown_active and not is_news_signal and not is_high_priority and not is_side_flip and not is_startup_window):
                        logger.info(f"⏳ [COOLDOWN] Skipping secondary Gemini review for {symbol} (Last: {int(now - last_review)}s ago).")
                        self.active_positions[symbol] = None
                        self.pending_orders_count = max(0, self.pending_orders_count - 1)
                        return
                else:
                    logger.info(f"🔥 [DYNAMIC MODE] Bypassing cooldown for {symbol} (Analyzing immediately)...")


                # Initial defaults
                approved, ai_strength, ai_leverage, ai_sl_mult, ai_tp_mult, ai_tp_price, reason = False, 1.0, self.leverage, 1.0, 1.0, None, "Pending Review"

                # --- NEW: STARTUP NEWS SHIELD (Dynamic) ---
                if is_news_signal and (time.time() - self.start_time) < self.news_shield_seconds:
                    logger.warning(f"🛡️ [STARTUP SHIELD] News Signal ignored for {symbol}: Waiting for fresh data (elapsed: {int(time.time() - self.start_time)}s)")
                    self.active_positions[symbol] = None
                    self.pending_orders_count = max(0, self.pending_orders_count - 1)
                    return

                # --- REFINED: Force LLM Review even for News (Bypass Removed) ---
                # Ask the LLM to analyze the setup
                if signal_type == "NEW_LISTING":
                    # v11.5: Optimized Flash Audit for New Listings
                    try:
                        ticker = await self.exchange.fetch_ticker(symbol)
                        current_price = ticker.get('last', 0)
                        bid = ticker.get('bid', 0)
                        ask = ticker.get('ask', 0)
                        spread_pct = ((ask - bid) / bid * 100) if bid > 0 else 0
                        
                        approved, ai_strength, ai_leverage, ai_sl_mult, ai_tp_mult, ai_tp_price, reason = await self.analyst.decide_listing_strategy(
                            symbol, current_price, spread_pct
                        )
                    except Exception as e:
                        logger.error(f"⚠️ Listing Ticker Error for {symbol}: {e}")
                        approved = False
                        reason = "Failed to fetch ticker for listing"
                else:
                    # Standard Multi-Indicator Strategy
                    indicators = self.latest_data.get(symbol, {})
                    # Add computed convenience fields for the LLM analyst
                    indicators['price_vs_ema200'] = (indicators.get('price', 0) - indicators.get('ema200', 0)) / indicators.get('ema200', 1) if indicators.get('ema200', 0) > 0 else 0
                    indicators['volume_change'] = 1.0 # Fallback 

                    approved, ai_strength, ai_leverage, ai_sl_mult, ai_tp_mult, ai_tp_price, reason = await self.analyst.decide_strategy(
                        symbol, side, signal_type, indicators, mtf_context=mtf_context, news_context=self.current_news
                    )


                    
                # Update Cooldown state
                # If REJECTED, set a longer cooldown to avoid spamming the same setup
                reject_cooldown = self.llm_cooldown_seconds * 2 if self.llm_cooldown_seconds > 0 else 600
                self.llm_cooldowns[symbol] = now if approved else now + reject_cooldown
                self.last_llm_side[symbol] = side.lower()
                
                if not approved:
                    logger.warning(f"🧠 [LLM STRATEGIC REVIEW] REJECTED {symbol} {side.upper()}: {reason}")
                    self.active_positions[symbol] = None
                    self.pending_orders_count = max(0, self.pending_orders_count - 1)
                    return
                
                # Dynamic leverage calculation (using AI suggestion as a target)
                if signal_type == "GATEKEEPER":
                    # v17.8: Blitz profile always requires at least 15x
                    floor = 15 if self.profile_type in ['blitz', 'extreme'] else 10
                    active_leverage = max(floor, self.leverage_range[0]) 
                    logger.info(f"🛡️ [NEWS LEVERAGE] Adjusted to {active_leverage}x (Profile Floor) for {symbol} News Trade.")
                elif is_black_swan:
                    base_leverage = self.calculate_dynamic_leverage(symbol, side, consensus_score, current_price, ai_leverage=ai_leverage)
                    active_leverage = min(20, base_leverage) # Hard cap at 20x for High Impact Events
                else:
                    active_leverage = self.calculate_dynamic_leverage(symbol, side, consensus_score, current_price, ai_leverage=ai_leverage)
                
                # REINFORCE: set_leverage is CRITICAL before order (v17.16 Absolute Lock)
                try:
                    target_lev = int(active_leverage)
                    # v17.16: Force BOTH sides for Bitget Hedged Mode compatibility (Ensure no 5x fallback)
                    await self.exchange.set_leverage(target_lev, symbol, params={'marginCoin': 'USDT', 'holdSide': 'long'})
                    await self.exchange.set_leverage(target_lev, symbol, params={'marginCoin': 'USDT', 'holdSide': 'short'})
                    logger.info(f"⚙️ [LEVERAGE SYNC] {symbol} locked to {target_lev}x (BOTH SIDES) for this trade.")
                except Exception as e:
                    logger.error(f"Failed to set leverage for {symbol}: {e}")
                
                # Calculate size with AI strength modifier
                amount_to_buy = self._calculate_order_amount(symbol, current_price, custom_leverage=active_leverage, consensus_score=consensus_score, ai_strength=ai_strength)
                
                logger.info(f"🧠 [LLM STRATEGIC REVIEW] APPROVED {symbol} {side.upper()} (Lev: {active_leverage}x, Strength: {ai_strength:.2f}): {reason}")

                logger.warning(f"🚀 [ORDER] {side.upper()} {amount_to_buy} {symbol} @ {current_price} (Consensus Order)")
                
                # 1. Place Main Market Order
                order = await self.exchange.create_market_order(symbol, side.lower(), amount_to_buy)
                logger.info(f"✅ Main order success for {symbol}: {order['id']}")
                
                # Gatekeeper alert removed per user request (v10.1)
                pass
            
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
            else:
                # --- NEW: PROFILE-AWARE DYNAMIC RISK MATRIX (v14.5) ---
                # Default multipliers by profile
                p_matrix = {
                    "institutional": {"sl": 1.5, "tp1": 3.0, "tp2": 6.0},
                    "conservative": {"sl": 1.2, "tp1": 1.8, "tp2": 4.0},
                    "aggressive": {"sl": 0.8, "tp1": 0.8, "tp2": 2.5},
                    "extreme": {"sl": 0.5, "tp1": 1.5, "tp2": 5.0} # Aggressive TP2 for moonbags
                }
                
                settings = p_matrix.get(self.profile_type, p_matrix["aggressive"])
                sl_mult = settings["sl"]
                tp1_mult = settings["tp1"]
                tp2_mult = settings["tp2"]
                
                # --- APPLY AI MULTIPLIERS (v10.5 Override) ---
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
                    logger.info(f"🧠 [AI TP/SL] Applied AI Multipliers over {self.profile_type} base: SL={sl_mult:.2f}x, TP={tp1_mult:.2f}x")

                # v12.0.2: [BLITZ SURVIVAL] Hard safety cap for high-leverage positions (>= 20x)
                if self.leverage >= 20:
                    max_sl_dist = current_price * 0.018 # Hard cap at 1.8%
                    if sl_distance > max_sl_dist:
                         logger.warning(f"🛡️ [BLITZ CAP] Reducing too-wide SL for {symbol} from {sl_distance/current_price:.2%} to 1.80% (Survival Cap).")
                         sl_distance = max_sl_dist
            
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
                'real_entry_price': real_entry_price, 
                'open_time': time.time(), 
                'opened_at': time.time(),
                'sl': sl_price,
                'sl_distance': sl_distance,
                'tp1': tp1_price,
                'tp2': tp2_price,
                'amount': amount_to_buy,
                'tp1_hit': False,
                'is_black_swan': is_black_swan,
                'signal_type': signal_type,
                'snapshot_id': snapshot_id,
                'profile_type': self.profile_type,
                'trailing_delayed': True if signal_type == "GATEKEEPER" else False,
                'highest_price': current_price if is_long else 0,
                'lowest_price': current_price if not is_long else 0,
                'ai_tp_price': ai_tp_price if 'ai_tp_price' in locals() else tp1_price
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
        
        # Notifica Telegram per avvisi importanti (Silenziamo GATEKEEPER come richiesto)
        if type != "GATEKEEPER":
            self.notifier.notify_alert(type, title, str(value))

    async def update_loop(self):
        logger.info("Starting bot market data update loop...")
        
        config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), self.config_file)
        last_mtime = os.path.getmtime(config_path) if os.path.exists(config_path) else 0
        
        while True:
            # --- [FIX] AUTO-CLEANUP AUDIT (v9.9.0) ---
            # Periodically sync internal memory with exchange reality to avoid Ghost Locks
            try:
                real_positions = self.latest_account_data.get('positions', [])
                real_symbols = {p.get('symbol') for p in real_positions}
                # Also include base symbols for matching
                real_base_symbols = {s.split(':')[0] for s in real_symbols if s}
                
                # Check internal memory against reality
                for symbol in list(self.active_positions.keys()):
                    if self.active_positions[symbol] is not None:
                        # If symbol is not in exchange positions, clear it
                        base_symbol = symbol.split(':')[0]
                        if symbol not in real_symbols and base_symbol not in real_base_symbols:
                             logger.warning(f"🧹 [GHOST CLEANUP] Position for {symbol} no longer exists. Clearing internal lock.")
                             self.active_positions[symbol] = None
                             # Also clear trade levels if they weren't cleared by order completion
                             if self.trade_levels.get(symbol):
                                  self.trade_levels[symbol] = None
                
                # Inverse sync: If a new position is found but not in memory, add it
                for pos in real_positions:
                    p_sym = pos.get('symbol')
                    p_amt = float(pos.get('positionAmt', 0))
                    if p_amt != 0:
                        match_symbol = next((s for s in self.symbols if s == p_sym or s.split(':')[0] == p_sym), None)
                        if match_symbol and self.active_positions.get(match_symbol) is None:
                             side = 'LONG' if p_amt > 0 else 'SHORT'
                             logger.info(f"📦 [SYNC] Found background position for {match_symbol} ({side}). Updating memory.")
                             self.active_positions[match_symbol] = side

            except Exception as e:
                logger.error(f"⚠️ [AUDIT ERROR] Position sync failed: {e}")

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

    async def _reconcile_active_trades(self):
        """
        Adapts recovered positions to the current risk profile (SL/TP, multipliers).
        v15.2: Retroactive Risk Adaptation.
        """
        try:
            logger.info(f"🔄 [ADAPTATION] Reconciling targets with current {self.profile_type.upper()} parameters...")
            
            # --- [CRITICAL] PROFILE ADOPTION ENGINE (v17.2) ---
            # Instead of hardcoded matrix, we use the actual config values 
            # loaded in __init__ for the current active profile.
            
            def_sl = self.stop_loss_pct or 0.02
            def_tp = self.take_profit_pct or 0.05
            
            # Boost SL for institutional wide-stop protection if needed
            if self.profile_type == "institutional":
                def_sl = max(def_sl, 0.03)

            for symbol, trade in self.trade_levels.items():
                if not trade:
                    continue
                
                # Check if we should re-calculate (profile mismatch or zombie)
                is_zombie = trade.get('status') == 'RECOVERED_ZOMBIE'
                profile_mismatch = trade.get('profile_type') != self.profile_type
                
                if not (is_zombie or profile_mismatch):
                    continue
                
                # Check current market state (priority to latest_data)
                price_data = self.latest_data.get(symbol, {})
                current_price = price_data.get('price', trade.get('entry_price', 0))
                
                if current_price <= 0:
                     logger.warning(f"⚠️ [ADAPTATION] Skipping {symbol}: Missing base price.")
                     continue
                
                old_profile = trade.get('profile_type', 'unknown')
                entry_price = float(trade.get('entry_price', current_price))
                is_long = trade.get('side', 'long').lower() in ['buy', 'long']
                
                logger.info(f"🛡️ [PROFILE ADOPTION] Adapting {symbol} from {old_profile.upper()} to {self.profile_type.upper()}...")
                
                # Use entry_price for target recalculation
                sl_price = entry_price * (1 + def_sl if not is_long else 1 - def_sl)
                tp1_price = entry_price * (1 - def_tp/2 if not is_long else 1 + def_tp/2) # 50% TP at half-way
                tp2_price = entry_price * (1 - def_tp if not is_long else 1 + def_tp)
                
                # Precision adjustment
                try:
                    sl_price = float(self.exchange.price_to_precision(symbol, sl_price))
                    tp1_price = float(self.exchange.price_to_precision(symbol, tp1_price))
                    tp2_price = float(self.exchange.price_to_precision(symbol, tp2_price))
                except: pass

                trade['sl'] = sl_price
                trade['tp1'] = tp1_price
                trade['tp2'] = tp2_price
                trade['profile_type'] = self.profile_type
                trade['status'] = 'ADAPTED' if not is_zombie else 'RECOVERED_ADAPTED'
                
                # Sync Leverage immediately for this symbol (with v17.17 robust lock)
                target_lev = max(int(self.leverage), int(self.leverage_range[0]))
                try:
                    # Force both sides for absolute lockdown
                    await self.exchange.set_leverage(target_lev, symbol, params={'marginCoin': 'USDT', 'holdSide': 'long'})
                    await self.exchange.set_leverage(target_lev, symbol, params={'marginCoin': 'USDT', 'holdSide': 'short'})
                    logger.info(f"⚙️ [LEVERAGE LOCK] {symbol} confirmed at {target_lev}x.")
                except Exception as ex: 
                    logger.warning(f"⚠️ [LEVERAGE SYNC FAIL] {symbol}: {ex}")
                
                # Trigger an immediate audit to see if we should close right now
                self._check_soft_stop_loss(symbol, current_price)

            # Update DB after mass adaptation
            self.db.save_state("trade_levels", self.trade_levels)
            logger.info(f"✅ [ADAPTATION COMPLETE] All positions aligned with {self.profile_type.upper()}.")
            
        except Exception as e:
            logger.error(f"❌ Error during trade reconciliation: {e}")
            logger.error(traceback.format_exc())

    async def _update_account_state(self):
        """Standardizes and updates the current account state (equity, positions, PnL)."""
        try:
            # 1. Fetch Balance (The ultimate source of truth for all positions)
            balances = await self.exchange.fetch_balance()
            
            # 2. Derive active positions from balance info (Standardized for Bitget V2 / Binance v11.4)
            info = balances.get('info', [])
            all_raw_pos = []
            
            if isinstance(info, list): # Bitget V2
                all_raw_pos = info
            elif isinstance(info, dict): # Binance
                all_raw_pos = info.get('positions', [])
            
            # v11.4 Bitget Fix: If no positions found in standard fields, try fetching explicitly
            if self.active_exchange_name == "bitget" and not all_raw_pos:
                try:
                    # Bitget CCXT requires symbols list for fetch_positions
                    raw_bitget_pos = await self.exchange.fetch_positions(self.symbols)
                    all_raw_pos = [p['info'] for p in raw_bitget_pos if 'info' in p]
                except Exception as ex:
                    logger.warning(f"⚠️ [BITGET SYNC] Explicit fetchPositions failed: {ex}")
            
            active_pos = []
            id_to_symbol = {m['id']: s for s, m in self.exchange.markets.items()}
            
            for p in all_raw_pos:
                # v17.15 Robust Extraction: Check info nesting for Bitget V2
                p_info = p if 'symbol' in p and 'total' in p else p.get('info', {})
                p_amt = float(p.get('positionAmt') or p.get('total') or p_info.get('total') or p_info.get('size', 0))
                
                if p_amt != 0:
                    p_id = p.get('symbol', p.get('instId') or p_info.get('symbol'))
                    full_symbol = id_to_symbol.get(p_id, p_id)
                    p['symbol'] = full_symbol
                    p['amount'] = p_amt
                    active_pos.append(p)

            wallet_balance, equity, unrealized_pnl, margin_ratio = self._calculate_unified_balance(balances)
            
            # --- PERSISTENCE & SYNC ---
            self.current_margin_ratio = margin_ratio
            self.latest_account_data['balance'] = wallet_balance
            self.latest_account_data['equity'] = equity
            self.latest_account_data['unrealized_pnl'] = unrealized_pnl
            self.latest_account_data['positions'] = active_pos # CRITICAL: Update the position list used by Sizing/Audit
            self.latest_account_data['alerts'] = self.alert_history
            
            # v17.20: Persist latest_account_data to DB for generate_report.py visibility
            self.db.save_state("latest_account_data", self.latest_account_data)
            
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
                # v11.2: Added 2s settlement delay at the start of loop
                # This prevents 'Ghost Exits' (BINANCE_SYNC) by giving the API time to register orders.
                await asyncio.sleep(2)
                
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

                # --- NEW: CAPITAL RESET DETECTION (v15.5) ---
                # Detect and fix stale 'initial_balance' data if the difference is >15% and NO positions are open.
                # v15.5: Reset AT START OF EVALUATION to prevent false CB LOCK
                if abs(effective_pnl_pct) >= 0.15 and not active_pos and (time.time() - self.start_time) < 600:
                     logger.warning(f"🔄 [CAPITAL RESET] Major balance shift from ${self.initial_wallet_balance:.2f} to ${wallet_balance:.2f} with zero positions. Syncing benchmark.")
                     self.initial_wallet_balance = wallet_balance
                     self.db.save_state("initial_balance", self.initial_wallet_balance)
                     # v15.7: EXPLICITLY RESET CB During Reset
                     self.circuit_breaker_active = False
                     self.global_panic_notified = False
                     
                     # Re-calc to prevent immediate loop trigger
                     wallet_pnl_pct = 0
                     equity_pnl_pct = 0
                     effective_pnl_pct = 0




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
                # v15.2: Profile-specific Holistic Account Protection
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
                
                # v15.4: Dynamic CB Reset (Reset if PnL recovered to 80% of the limit)
                reset_threshold = -self.daily_loss_limit * 0.8
                if effective_pnl_pct > reset_threshold:
                    if self.circuit_breaker_active:
                        logger.warning(f"🟢 [CB RESET] Circuit Breaker Disarmed: Current loss at {effective_pnl_pct:.2%} (Reset Threshold: {reset_threshold:.2%})")
                        self.circuit_breaker_active = False
                        self.global_panic_notified = False 

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
                
                # v11.0: Robust Manual Closure Detection (3-cycle threshold)
                # We only clear the state if the position is missing for 3 consecutive sync cycles
                manual_check_targeted = [s for s in state_symbols if s not in pos_symbols and self.trade_levels.get(s)]
                
                for s in manual_check_targeted:
                    self.missing_pos_counter[s] += 1
                    if self.missing_pos_counter[s] >= 3 and not is_startup_protected:
                        logger.warning(f"🔓 [PYRAMID UNLOCKED] Manual closure confirmed on {self.active_exchange_name.upper()} for {s}. Freeing internal state.")
                        self.trade_levels[s] = None
                        self.active_positions[s] = None
                        self.missing_pos_counter[s] = 0
                        # Force state save
                        self.db.save_state("trade_levels", self.trade_levels)
                    elif self.missing_pos_counter[s] < 3:
                        logger.debug(f"🔍 [SYNC] Position {s} missing from active list ({self.missing_pos_counter[s]}/3). Waiting for confirmation.")

                # Reset counter for symbols that are present
                for s in pos_symbols:
                    if s in self.missing_pos_counter:
                        self.missing_pos_counter[s] = 0

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
            if self.active_exchange_name == "bitget":
                positions = await self.exchange.fetch_positions(self.symbols)
            else:
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
            if active_pos:
                logger.warning("✅ All positions closed successfully.")
                self.notifier.notify_alert("CRITICAL", "VENDITA DI EMERGENZA (GLOBAL PANIC)", "Tutte le posizioni sono state chiuse a causa del drawdown eccessivo.")
            else:
                logger.info("ℹ️ [INFO] Emergency Cleanup: No active positions to close, alert suppressed.")

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
            if self.active_exchange_name == "bitget":
                # v17.1: Removed self.symbols restriction to scan the entire account for untracked positions
                positions = await self.exchange.fetch_positions()
            else:
                positions = await self.exchange.fetch_positions()
            # v17.13: Use raw list, filtering will be handled inside the loop for Bitget 'total' compatibility
            active_pos = [p for p in positions] 
            
            for pos in active_pos:
                symbol = pos['symbol']
                
                # v17.13: Robust Amount Detection (Bitget 'total' support)
                info = pos.get('info', {})
                # Some exchanges use 'amount' or 'contracts', Bitget V2 uses 'total' or 'size'
                is_active = (
                    float(pos.get('amount', 0) or 0) != 0 or 
                    float(pos.get('contracts', 0) or 0) != 0 or 
                    float(info.get('total', 0) or info.get('size', 0) or 0) != 0
                )
                if not is_active:
                    continue
                    
                # v20.2: Normalize symbol for matching (Bitget handling)
                norm_sym = symbol.replace(':USDT', '') if ':USDT' in symbol else symbol
                
                # Check if already tracked, but force-re-sync if it's a recovered position
                is_tracked = False
                matched_key = symbol
                
                if self.trade_levels.get(symbol) is not None:
                    is_tracked = True
                elif self.trade_levels.get(norm_sym) is not None:
                    is_tracked = True
                    matched_key = norm_sym
                
                if is_tracked:
                    # If it's already there but marked as RECOVERED, keep going to update entry/leverage
                    if self.trade_levels[matched_key] and str(self.trade_levels[matched_key].get('status', '')).startswith('RECOVERED'):
                        logger.info(f"🔄 [REFRESH] Re-syncing existing recovered position for {matched_key} to fix potential stale data...")
                        symbol = matched_key # Use the tracked key
                    else:
                        continue

                logger.warning(f"🧟 [ZOMBIE FOUND] Rediscovered untracked position for {symbol}. Restoring monitoring...")
                # v17.12 FINAL DEBUG: Inspect RAW data for sync calibration
                logger.warning(f"🔍 [RAW POS DATA] {symbol}: {pos}")
                
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
                # v17.10: Ultimate Entry Price Extraction (Prioritizing CCXT Normalization)
                info = pos.get('info', {})
                # Diagnostic script proved pos.get('entryPrice') is the most reliable for Bitget Swap
                entry_price = float(pos.get('entryPrice') or info.get('openPriceAvg') or info.get('averagePrice') or pos.get('avgCost') or pos.get('markPrice', 0))
                current_price = float(pos.get('markPrice', entry_price))
                # v17.18.2: Ultimate Amount Discovery (Bitget 'contracts' or info 'total')
                amount = abs(float(pos.get('contracts') or pos.get('amount') or info.get('total') or info.get('size', 0)))
                
                # Use current profile defaults (aligned with _reconcile_active_trades)
                def_sl = self.stop_loss_pct or 0.02
                def_tp = self.take_profit_pct or 0.05
                
                # Recalculate targets based on Profile
                side = pos['side'].lower()
                is_long = side in ['buy', 'long']
                sl_price = entry_price * (1 + def_sl if not is_long else 1 - def_sl)
                tp1_price = entry_price * (1 - def_tp/2 if not is_long else 1 + def_tp/2)
                tp2_price = entry_price * (1 - def_tp if not is_long else 1 + def_tp)
                
                # Precision adjustment
                try:
                    sl_price = float(self.exchange.price_to_precision(symbol, sl_price))
                    tp1_price = float(self.exchange.price_to_precision(symbol, tp1_price))
                    tp2_price = float(self.exchange.price_to_precision(symbol, tp2_price))
                except: pass

                # v17.11: Compare against old state to see if we corrected something
                old_entry = 0
                if self.trade_levels.get(symbol) and isinstance(self.trade_levels[symbol], dict):
                    old_entry = float(self.trade_levels[symbol].get('entry_price', 0))

                self.trade_levels[symbol] = {
                    'side': 'long' if is_long else 'short',
                    'entry_price': entry_price,
                    'sl': sl_price,
                    'tp1': tp1_price,
                    'tp2': tp2_price,
                    'amount': amount,
                    'open_time': time.time() - 3600, 
                    'status': 'RECOVERED_ZOMBIE_V11', # Incremental version to force state refresh
                    'profile_type': self.profile_type,
                    'tp1_hit': False,
                    'highest_price': current_price if is_long else 0,
                    'lowest_price': current_price if not is_long else 999999
                }
                
                if old_entry > 0 and abs(old_entry - entry_price) / entry_price > 0.01:
                    logger.warning(f"🎯 [DATA CORRECTED] {symbol} Entry updated: {old_entry:.4f} -> {entry_price:.4f} (Mismatch corrected)")
                
                logger.warning(f"🛡️ [RECOVERY] {symbol} restored with {self.profile_type.upper()} Defaults: SL @ {sl_price:.4f}, TP1 @ {tp1_price:.4f}")
                
                # Set leverage immediately (with Bitget Hedged-Mode compatibility v17.10)
                target_lev = max(int(self.leverage), int(self.leverage_range[0]))
                try:
                    # v17.10: Force sync leverage with holdSide AND marginCoin AND verification
                    side_param = 'long' if is_long else 'short'
                    logger.info(f"⚙️ [LEVERAGE SYNC] Forcing {symbol} to {target_lev}x ({side_param.upper()})...")
                    # Try setting leverage with both holdSide and marginCoin for universal Bitget v2 support
                    await self.exchange.set_leverage(target_lev, symbol, params={'holdSide': side_param, 'marginCoin': 'USDT'})
                    
                    # Verify immediately
                    updated_pos = await self.exchange.fetch_positions([symbol])
                    current_lev = 0
                    if updated_pos:
                        current_lev = float(updated_pos[0].get('leverage', 0))
                    
                    if int(current_lev) == int(target_lev):
                        logger.info(f"✅ [LEVERAGE SYNC] {symbol} confirmed at {target_lev}x.")
                    else:
                        logger.warning(f"⚠️ [LEVERAGE DRIFT] {symbol} mismatch. Target {target_lev}x, Exchange {current_lev}x. Proceeding with caution.")
                except Exception as lev_err:
                    logger.error(f"⚠️ [LEVERAGE FAIL] Could not sync leverage for {symbol}: {lev_err}")
                
                # v21.0: [CRITICAL] Ensure HARD Stop Loss exists on Exchange for recovered positions
                try:
                    # Place or Update Hard SL Order on Exchange
                    sl_side = 'sell' if is_long else 'buy'
                    hold_side = 'long' if is_long else 'short'
                    
                    sl_params = {
                        'stopPrice': sl_price,
                        'triggerPrice': sl_price,
                        'reduceOnly': True,
                        'triggerType': 'mark_price'
                    }
                    if self.active_exchange_name == "bitget":
                        sl_params.update({'holdSide': hold_side, 'marginCoin': 'USDT'})
                    
                    logger.warning(f"⚙️ [RECOVERY SL] Placing active SL order on exchange for {symbol} at {sl_price}...")
                    # Success testing confirmed 'market' order with stopPrice params works for Bitget trigger orders
                    await self.exchange.create_order(symbol, 'market', sl_side, amount, params=sl_params)
                    logger.info(f"✅ [RECOVERY SL] Hard SL successfully placed on exchange for {symbol}.")
                except Exception as sl_order_err:
                    logger.error(f"⚠️ [RECOVERY SL FAIL] Failed to place hard SL for {symbol}: {sl_order_err}")

                # [CRITICAL] Ensure HARD TP exists on Exchange for recovered positions
                try:
                    tp_side = 'sell' if is_long else 'buy'
                    hold_side = 'long' if is_long else 'short'
                    
                    tp_params = {
                        'stopPrice': tp2_price,
                        'triggerPrice': tp2_price,
                        'reduceOnly': True,
                        'triggerType': 'mark_price'
                    }
                    if self.active_exchange_name == "bitget":
                        tp_params.update({'holdSide': hold_side, 'marginCoin': 'USDT'})
                    
                    logger.warning(f"⚙️ [RECOVERY TP] Placing active TP order on exchange for {symbol} at {tp2_price}...")
                    await self.exchange.create_order(symbol, 'market', tp_side, amount, params=tp_params)
                    logger.info(f"✅ [RECOVERY TP] Hard TP successfully placed on exchange for {symbol}.")
                except Exception as tp_order_err:
                    logger.error(f"⚠️ [RECOVERY TP FAIL] Failed to place hard TP for {symbol}: {tp_order_err}")
            
            # Save state to DB
            self.db.save_state("trade_levels", self.trade_levels)
            
        except Exception as e:
            logger.error(f"❌ Error during zombie synchronization: {e}")

    async def monitor_position_health_loop(self):
        """Background task to periodically evaluate active positions with AI."""
        # Initial wait to let bot settle
        await asyncio.sleep(600) 
        
        iteration = 0
        while True:
            try:
                # v21.5: [CRITICAL] Run Orphan Recovery every 10 iterations (~20 minutes)
                if iteration % 10 == 0:
                    logger.warning("🔍 [SCHEDULED] Running Orphan Position Recovery audit...")
                    await self.sync_zombie_positions()
                
                iteration += 1
                
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
