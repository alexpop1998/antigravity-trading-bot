import logging
import asyncio
import time
import json
import os
import traceback
from typing import Dict, List, Any
from datetime import datetime
from collections import deque
from dotenv import load_dotenv

# Modular Managers (v30.0)
from exchange_gateway import ExchangeGateway
from safety_shield import SafetyShield
from strategy_engine import StrategyEngine

# Legacy / Utility (To be modularized later)
import generate_report
from database import BotDatabase
from telegram_notifier import TelegramNotifier
from asset_scanner import AssetScanner
from llm_analyst import LLMAnalyst
from sector_manager import SectorManager

logger = logging.getLogger("TradingBot")

class CryptoBot:
    def __init__(self, config_file=None):
        # 1. Load Environment Variables
        load_dotenv()
        
        # 2. Determine Config File (Env priority, then arg, then default)
        env_profile = os.getenv("CONFIG_PROFILE")
        if not config_file:
            config_file = f"config_{env_profile}.json" if env_profile else "config_aggressive.json"
        
        self.config_file = config_file
        self.initialized = False
        self.start_time = time.time()
        
        # 3. Load Config Logic
        self._load_config()
        
        # 4. Initialize Core Managers (v30.3 Corrected Signatures)
        self.gateway = ExchangeGateway(self.active_exchange_name)
        self.shield = SafetyShield(self)
        self.strategy = StrategyEngine(self)
        
        # 5. Initialize Utility Tools
        self.db = BotDatabase()
        self.notifier = TelegramNotifier()
        self.scanner = AssetScanner(self.gateway.exchange)
        self.sector_manager = SectorManager()
        
        # 6. Shared State
        self.trade_levels = self.db.load_state("trade_levels") or {}
        self.active_positions = {}
        self.latest_data = {}
        self.latest_account_data = {}
        
        # Locks & Cooldowns
        self.order_lock = asyncio.Lock()
        self.llm_cooldowns = {}
        
        logger.warning(f"🤖 [V30.3] CryptoBot Initialized | Profile: {self.profile_type.upper()} | Config: {self.config_file}")

    def _load_config(self):
        try:
            # 1. Load JSON file
            config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), self.config_file)
            if not os.path.exists(config_path):
                logger.error(f"❌ Config file not found: {config_path}")
                raise FileNotFoundError(config_path)
                
            with open(config_path, 'r') as f:
                self.config = json.load(f)
            
            # 2. Handle Nested Structures (v30.0 Compatibility)
            t_params = self.config.get('trading_parameters', {})
            s_params = self.config.get('strategic_params', {})
            
            # 3. Map Essentials (Environment Variables Priority)
            self.active_exchange_name = os.getenv("ACTIVE_EXCHANGE", self.config.get('exchange', 'bitget'))
            prefix = self.active_exchange_name.upper()
            
            self.api_key = os.getenv(f"{prefix}_API_KEY") or self.config.get('api_key')
            self.api_secret = os.getenv(f"{prefix}_API_SECRET") or self.config.get('api_secret')
            self.api_passphrase = os.getenv(f"{prefix}_PASSWORD") or os.getenv(f"{prefix}_API_PASS") or self.config.get('api_passphrase', '')
            
            if not self.api_key:
                raise KeyError(f"Missing API Key for {self.active_exchange_name} (Checked Env and JSON)")

            # Latency/Profile Mapping
            self.profile_type = self.config.get('profile_type', t_params.get('profile_type', 'aggressive'))
            self.leverage = t_params.get('leverage', self.config.get('leverage', 10))
            self.stop_loss_pct = t_params.get('stop_loss_pct', self.config.get('stop_loss_pct', 0.02))
            self.take_profit_pct = t_params.get('take_profit_pct', self.config.get('take_profit_pct', 0.04))
            self.max_global_margin_ratio = t_params.get('max_global_margin_ratio', self.config.get('max_global_margin_ratio', 0.8))
            self.leverage_range = s_params.get('leverage_range', self.config.get('leverage_range', [1.0, 50.0]))
            self.min_notional_usdt = s_params.get('min_notional_usdt', self.config.get('min_notional_usdt', 5.0))
            
            self.is_macro_paused = False
            
        except Exception as e:
            logger.error(f"❌ Failed to load config: {e}")
            raise e

    async def sync_state(self):
        """[v30.0] Atomic State Synchronization."""
        try:
            exchange_positions = await self.gateway.fetch_positions_robustly()
            self.latest_account_data = await self.gateway.fetch_balance_safe()
            
            current_active_symbols = [p['symbol'] for p in exchange_positions]
            
            for p in exchange_positions:
                symbol = p['symbol']
                if symbol not in self.trade_levels or self.trade_levels[symbol] is None:
                    logger.warning(f"🧟 [SYNC] Adopting orphaned position: {symbol}")
                    self.trade_levels[symbol] = {
                        "symbol": symbol,
                        "side": 'long' if p['contracts'] > 0 else 'short',
                        "entry_price": p['entry_price'],
                        "amount": abs(p['contracts']),
                        "status": "RECOVERED_ZOMBIE",
                        "opened_at": time.time(),
                        "profile_type": self.profile_type
                    }
            
            for sym, trade in list(self.trade_levels.items()):
                if trade and sym not in current_active_symbols:
                    logger.info(f"👻 [SYNC] Clearing ghost trade memory: {sym}")
                    self.trade_levels[sym] = None
                    self.active_positions[sym] = None

            self.db.save_state("trade_levels", self.trade_levels)
        except Exception as e:
            logger.error(f"❌ [SYNC FAILED] {e}")

    async def initialize(self):
        """[CORE INIT] v30.0 System Boot"""
        logger.warning(f"🚀 [BOOT] Starting Modular Core v30.0 on {self.active_exchange_name.upper()}...")
        await self.gateway.exchange.load_markets()
        await self.sync_state()
        self.initialized = True
        logger.info("✅ [BOOT] System Ready. High-frequency heartbeats starting.")
        await self.notifier.startup_notify()

    async def execute_order(self, symbol: str, side: str, analysis: Dict[str, Any]):
        """Executes a market order with dynamic sizing and leverage."""
        async with self.order_lock:
            try:
                # 🛡️ Conflict Resolver (Flip Logic v29.x Restore)
                positions = await self.gateway.fetch_positions_robustly()
                existing = next((p for p in positions if p['symbol'] == symbol), None)
                if existing:
                    existing_side = 'buy' if float(existing['positionAmt']) > 0 else 'sell'
                    if existing_side != side.lower():
                        # High-Priority Reversal Check
                        is_strong = analysis.get('confidence', 0) > 0.85 or analysis.get('is_black_swan', False)
                        if is_strong:
                            logger.critical(f"🆘 [FLIP] Strong reversal for {symbol}. Closing {existing_side} for {side.upper()}.")
                            await self.gateway.close_all_for_symbol(symbol)
                            await asyncio.sleep(1) # Settlement delay
                        else:
                            logger.warning(f"🚫 [CONFLICT] Existing {existing_side} on {symbol}. Skipping {side.upper()}.")
                            return

                price = self.latest_data.get(symbol, {}).get('price', 0)
                amount = self._calculate_order_amount(symbol, price)
                leverage = analysis.get('leverage', self.leverage)
                
                if amount <= 0: return

                await self.gateway.set_leverage(symbol, int(leverage))
                await self.gateway.place_order(symbol, side.lower(), amount)
                
                # Register trade for monitoring
                await self._register_trade(symbol, side.lower(), price, amount, analysis)
                await self.notifier.send_message(f"✅ *NUOVA POSIZIONE {side.upper()}*\n🪙 {symbol} @ {price}")
            except Exception as e:
                logger.error(f"❌ [EXECUTION FAILED] {e}")
                
                is_long = side.lower() == 'buy'
                self.trade_levels[symbol] = {
                    "symbol": symbol,
                    "side": 'long' if is_long else 'short',
                    "entry_price": price,
                    "amount": amount,
                    "sl": price * (1 - self.stop_loss_pct if is_long else 1 + self.stop_loss_pct),
                    "tp1": price * (1 + self.take_profit_pct/2 if is_long else 1 - self.take_profit_pct/2),
                    "opened_at": time.time(),
                    "status": "RUNNING",
                    "profile_type": self.profile_type,
                    "tp1_hit": False
                }
                self.db.save_state("trade_levels", self.trade_levels)
                await self.notifier.send_message(f"✅ *NUOVA POSIZIONE {side.upper()}*\n🪙 {symbol} @ {price}")
            except Exception as e:
                logger.error(f"❌ [EXECUTION FAILED] {e}")

    async def close_position(self, symbol: str, trade: Dict[str, Any], reason: str = "EXIT"):
        """Standardized exit execution."""
        try:
            logger.warning(f"🔄 [CLOSE] Closing {symbol} ({reason})")
            await self.gateway.close_all_for_symbol(symbol)
            self.trade_levels[symbol] = None
            self.db.save_state("trade_levels", self.trade_levels)
            await self.notifier.send_message(f"🏁 *POSIZIONE CHIUSA*\n🪙 {symbol} | 🎯 {reason}")
        except Exception as e:
            logger.error(f"❌ [CLOSE FAILED] {e}")

    def _calculate_order_amount(self, symbol, price):
        """Standard Sizing (25% of Equity * Leverage for Blitz)"""
        try:
            equity = self.latest_account_data.get('equity', 0)
            if equity <= 0: return 0
            
            risk_pct = self.config.get('trading_parameters', {}).get('percent_per_trade', 25.0) / 100.0
            margin_usdt = equity * risk_pct
            notional = margin_usdt * self.leverage
            
            if notional < self.min_notional_usdt:
                notional = self.min_notional_usdt
                
            amount = notional / price
            return float(self.gateway.exchange.amount_to_precision(symbol, amount))
        except Exception as e:
            logger.error(f"Error calculating order amount: {e}")
            return 0

    def add_alert(self, source, message, severity="INFO"):
        """Standardized alerting mechanism for background modules."""
        icon = "💡" if severity == "INFO" else "⚠️" if severity == "WARNING" else "🚨"
        log_msg = f"{icon} [{source}] {message}"
        logger.warning(log_msg)
        try:
            asyncio.create_task(self.notifier.send_message(f"🔔 *{source} ALERT*\n{message}"))
        except:
            pass

    async def run_reactive_safety_loop(self):
        """[HIGH FREQ] - Reflexes (10s)"""
        logger.warning("🛡️ [HEARTBEAT] Reactive Safety Loop Active")
        while True:
            try:
                if not self.initialized: await asyncio.sleep(5); continue
                
                positions = await self.gateway.fetch_positions_robustly()
                for p in positions:
                    symbol = p['symbol']
                    trade = self.trade_levels.get(symbol)
                    if trade:
                        entry = trade['entry_price']
                        notional = p['notional']
                        pnl = p['unrealized_pnl']
                        current_price = entry * (1 + pnl/notional) if notional > 0 else entry
                        
                        triggered, reason = await self.shield.check_position(symbol, trade, current_price)
                        if triggered:
                            if reason == "TAKE_PROFIT_1":
                                await self.scale_out_position(symbol, trade, reason=reason)
                            else:
                                await self.close_position(symbol, trade, reason=reason)
            except Exception as e:
                logger.error(f"❌ Safety Loop: {e}")
            await asyncio.sleep(10)

    async def scale_out_position(self, symbol: str, trade: Dict[str, Any], reason: str = "TP1"):
        """Partial close (50%) and move SL to Break-Even."""
        async with self.order_lock:
            try:
                logger.warning(f"🎯 [SCALE OUT] {symbol} hit TP1. Closing 50%.")
                amount_to_close = trade['amount'] / 2
                side = 'sell' if trade['side'] == 'long' else 'buy'
                
                await self.gateway.place_order(symbol, side, amount_to_close, params={'reduceOnly': True})
                
                # Update Trade State
                trade['amount'] -= amount_to_close
                trade['tp1_hit'] = True
                
                # Move SL to Break-Even (Entry + 0.3%)
                entry = trade['entry_price']
                trade['sl'] = entry * (1.003 if trade['side'] == 'long' else 0.997)
                
                self.db.save_state("trade_levels", self.trade_levels)
                await self.notifier.send_message(f"🎯 *TARGET RAGGIUNTO*\n🪙 {symbol} | ✅ TP1 (50%) | 🛡️ SL @ Break-Even")
            except Exception as e:
                logger.error(f"❌ Scale-Out Failed for {symbol}: {e}")

    async def run_deliberative_analysis_loop(self):
        """[MED FREQ] - Brain (60s)"""
        logger.warning("🧠 [HEARTBEAT] Deliberative Analysis Loop Active")
        while True:
            try:
                if not self.initialized: await asyncio.sleep(10); continue
                
                await self.sync_state()
                top_assets = await self.scanner.scan()
                for asset in top_assets:
                    symbol = asset['symbol']
                    try:
                        import pandas as pd
                        # Fetch 100 candles on 15m timeframe for ML + Regime
                        ohlcv = await self.gateway.exchange.fetch_ohlcv(symbol, '15m', limit=100)
                        if ohlcv and len(ohlcv) >= 20:
                            df = pd.DataFrame(ohlcv, columns=['timestamp','open','high','low','close','volume'])
                            # Compute basic indicators on the DataFrame
                            df['rsi'] = 50.0  # placeholder — real RSI needs ta-lib or pandas-ta
                            df['macd_hist'] = df['close'].diff(3)
                            df['bb_upper'] = df['close'].rolling(20).max()
                            df['bb_lower'] = df['close'].rolling(20).min()
                            df['atr'] = df['high'] - df['low']
                            closes = df['close'].tolist()
                            volumes = df['volume'].tolist()
                            indicators = {
                                'close': closes[-1],
                                'volume': volumes[-1],
                                'volume_change': (volumes[-1] / volumes[-2] - 1) if volumes[-2] > 0 else 0,
                                'rsi': asset.get('rsi', 50),
                                'macd_hist': float(df['macd_hist'].iloc[-1]),
                                'bb_upper': float(df['bb_upper'].iloc[-1]),
                                'bb_lower': float(df['bb_lower'].iloc[-1]),
                                'atr': float(df['atr'].iloc[-1]),
                                'price_vs_ema200': closes[-1] / (sum(closes[-50:]) / min(50, len(closes))),
                                'df': df  # full DataFrame for MLPredictor
                            }
                            self.latest_data[symbol] = indicators
                        else:
                            indicators = self.latest_data.get(symbol, {})
                    except Exception as fe:
                        logger.debug(f"OHLCV fetch skipped for {symbol}: {fe}")
                        indicators = self.latest_data.get(symbol, {})
                    
                    analysis = await self.strategy.analyze_opportunity(symbol, indicators)
                    if analysis['score'] >= 0.8:
                        await self.execute_order(symbol, 'buy', analysis)
                    
                    # 🚀 v30.29: Add delay to avoid Gemini rate limits (503 errors)
                    await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"❌ Analysis Loop: {e}")
            await asyncio.sleep(60)


    async def start_all_loops(self):
        """The Orchestrator."""
        logger.warning("🏁 [ORCHESTRATOR] Starting Core Task Groups...")
        reactive = asyncio.create_task(self.run_reactive_safety_loop())
        deliberative = asyncio.create_task(self.run_deliberative_analysis_loop())
        reporting = asyncio.create_task(self._scheduled_reporting())
        await asyncio.gather(reactive, deliberative, reporting)

    async def _scheduled_reporting(self):
        while True:
            try:
                await generate_report.async_generate()
            except: pass
            await asyncio.sleep(3600)

if __name__ == "__main__":
    bot = CryptoBot()
    asyncio.run(bot.initialize())
