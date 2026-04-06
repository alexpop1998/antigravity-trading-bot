import ccxt.async_support as ccxt
import pandas as pd
import asyncio
import logging
import os
import json
import time
from typing import Any, Dict, List, Optional
from dotenv import load_dotenv

from exchange_gateway import ExchangeGateway
from safety_shield import SafetyShield
from strategy_engine import StrategyEngine
from database import BotDatabase
from telegram_notifier import TelegramNotifier
from asset_scanner import AssetScanner
from news_radar import NewsRadar
from reporter import BotReporter

load_dotenv(override=True)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("CryptoBot")

class CryptoBot:
    def __init__(self, profile=None):
        load_dotenv(override=True)
        # 🟢 UNIFIED PROFILE LOADING (v30.51)
        profile = os.getenv('ACTIVE_PROFILE', os.getenv('CONFIG_PROFILE', 'aggressive')).lower()
        self.profile_type = profile
        self.config_file = f"config_{profile}.json"
        
        # 1. Core Services
        self.db = BotDatabase()
        self.notifier = TelegramNotifier()
        
        # 🛡️ GLOBAL AI CONCURRENCY (v31.12 UNLOCKED)
        # Increased to 5 for maximum speed after payment fix.
        self.ai_semaphore = asyncio.Semaphore(5)
        
        # 2. Config Loading
        self.config = self._load_config()
        self.profile_type = self.config.get("profile_type", "aggressive")
        self.consensus_threshold = self.config.get("trading_parameters", {}).get("consensus_threshold", 0.55)
        self.min_ai_confidence = self.config.get("trading_parameters", {}).get("min_ai_confidence", 0.55)
        self.leverage = self.config.get("trading_parameters", {}).get("leverage", 10)
        self.percent_per_trade = self.config.get("trading_parameters", {}).get("percent_per_trade", 25.0)
        self.max_concurrent_positions = self.config.get("trading_parameters", {}).get("max_concurrent_positions", 3)
        self.stop_loss_pct = self.config.get("trading_parameters", {}).get("stop_loss_pct", 0.02)
        self.take_profit_pct = self.config.get("trading_parameters", {}).get("take_profit_pct", 0.04)
        self.min_notional_usdt = float(self.config.get("strategic_params", {}).get("min_notional_usdt", 5.0))
        
        # 3. Parameters
        params = self.config.get("trading_parameters", {})
        self.symbols = params.get("symbols", ["BTC/USDT:USDT"])
        self.timeframe = params.get("timeframe", "15m")
        # 🟢 ROBUST CONFIG LOADING (v30.50)
        tp = self.config.get('trading_parameters', {})
        
        # Priority: trading_parameters -> fallback
        self.consensus_threshold = float(tp.get('consensus_threshold', 0.55))
        
        self.min_notional_usdt = float(self.config.get("strategic_params", {}).get("min_notional_usdt", 5.0))
        
        # 4. Modules
        exchange_name = self.config.get("strategic_params", {}).get("active_exchange", os.getenv("ACTIVE_EXCHANGE", "bitget"))
        self.active_exchange_name = exchange_name  # Exposed attribute
        self.gateway = ExchangeGateway(exchange_name)
        self.shield = SafetyShield(self)
        self.strategy = StrategyEngine(self)
        self.scanner = AssetScanner(self.gateway.exchange)
        self.news_radar = NewsRadar(self)
        self.reporter = BotReporter()
        
        # 5. Shared State
        self.trade_levels = self.db.load_state("trade_levels") or {}
        self.active_positions = {}
        self.latest_data = {}
        self.latest_account_data = {'equity': 0.0, 'balance': 0.0, 'margin_ratio': 0.0}
        self.initial_wallet_balance = self.db.load_state("initial_balance") or 0.0
        # 🛡️ GLOBAL AI CONCURRENCY CONTROL (v31.07)
        # Shared across NewsRadar and LLMAnalyst to avoid 429 collisions.
        self.ai_semaphore = asyncio.Semaphore(5)
        self.semaphore = self.ai_semaphore
        self.order_lock = asyncio.Lock()
        
        # v33.8 Cost Safe Headers
        self.initialized = False
        self.start_time = time.time()
        self.last_pair_update = 0
        self.pair_update_interval = 3600
        self.dynamic_symbols = []
        # Gemini 1.5 Flash (v33.8 Cost Safe)
        self.api_key = os.getenv("LLM_API_KEY")
        model_name = os.getenv("LLM_MODEL_NAME", "gemini-1.5-flash")
        self.gemini_url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={self.api_key}"

    def _load_config(self):
        try:
            # 🔴 DIAGNOSTIC LOG (v30.52)
            abs_path = os.path.abspath(self.config_file)
            logger.info(f"📂 [BOOT] Loading config from: {abs_path}")
            
            with open(self.config_file, 'r') as f:
                config = json.load(f)
                logger.info(f"💾 [BOOT] Config content preview: {str(config)[:100]}...")
                return config
        except Exception as e:
            logger.error(f"Error loading config {self.config_file}: {e}")
            return {}

    async def initialize(self):
        if self.initialized: return
        try:
            await self.gateway.exchange.load_markets()
            await self.sync_state()
            
            if self.initial_wallet_balance <= 0:
                await self.sync_state()
                self.initial_wallet_balance = self.latest_account_data['equity']
                self.db.save_state("initial_balance", self.initial_wallet_balance)
                logger.info(f"💰 [BOOT] Initial Balance synced: ${self.initial_wallet_balance:.2f}")

            # Start Loops (Moved after balance sync)
            asyncio.create_task(self.run_deliberative_analysis_loop())
            asyncio.create_task(self.run_reactive_safety_loop())
            asyncio.create_task(self.run_zombie_sync_loop())
            asyncio.create_task(self.run_margin_cleanup_loop())
            # v33.8: News and Audit loops disabled for cost management
            # asyncio.create_task(self.run_news_radar_loop())
            # asyncio.create_task(self.run_daily_audit_loop())
            # Start Report Loops (v31.02)
            asyncio.create_task(self.run_automated_report_loop())
            try:
                self.reporter.generate_html()
                logger.info("📊 [BOOT] Initial Investor Report generated.")
            except Exception as e:
                logger.error(f"❌ Failed to generate initial report: {e}")

            self.initialized = True
            # 🛡️ Notification Spam Guard (v31.02)
            last_boot = self.db.load_state("last_boot_time") or 0
            if (time.time() - float(last_boot)) > 300: # 5 min cooldown
                await self.notifier.send_message(f"🚀 *SISTEMA ATTIVO*\nProfilo: {self.profile_type.upper()}\nSoglia: {self.consensus_threshold}")
                self.db.save_state("last_boot_time", time.time())
            else:
                logger.info("⏭️ [SILENT BOOT] Suppressing start message (cooldown active).")

            self.initialized = True
        except Exception as e:
            logger.error(f"Initialization Failed: {e}")

    def add_alert(self, source, title, message):
        """[v33.5] Alert proxy for compatibility with news/social modules."""
        logger.warning(f"🔔 [ALERT] {source} | {title}: {message}")
        try:
            asyncio.create_task(self.notifier.send_message(f"🔔 *{source} ALERT*\n📌 {title}\n📝 {message}"))
        except: pass

    async def sync_state(self):
        """Syncs balance and positions with exchange."""
        try:
            balance_data = await self.gateway.fetch_balance_safe()
            self.latest_account_data['equity'] = balance_data['equity']
            # Estimate margin ratio
            raw = balance_data['raw']
            self.latest_account_data['margin_ratio'] = float(raw.get('info', {}).get('marginRatio', 0)) if isinstance(raw.get('info'), dict) else 0.0
            
            positions = await self.gateway.fetch_positions_robustly()
            self.active_positions = {p['symbol']: ('LONG' if p['side'] == 'long' else 'SHORT') for p in positions}
        except Exception as e:
            logger.error(f"Sync State Failed: {e}")

    async def handle_signal(self, symbol, source, side, confidence=1.0, is_black_swan=False, metadata=None):
        """
        [V29 SIGNAL HANDLER]
        Gateway for all signals (News, Strategy, External).
        Handles Black-Swan emergency entries.
        """
        try:
            logger.warning(f"📡 [SIGNAL] Source: {source} | Symbol: {symbol} | Side: {side.upper()} | Conf: {confidence}")
            
            if symbol == "GLOBAL":
                # For global news, we trigger on the core set of symbols
                target_symbols = list(self.latest_data.keys())[:3] # Focus top 3
                for s in target_symbols:
                    await self.handle_signal(s, f"{source}_PROC", side, confidence, is_black_swan)
                return

            # 🛡️ [SECURITY FIX v37.1]
            # Signals must pass through StrategyEngine to ensure AI Consensus
            logger.info(f"🧠 [ROUTING] Routing signal for {symbol} to StrategyEngine...")
            # We bypass tech scoring for signal sources if needed, but Gemini must approve
            await self.run_targeted_analysis(symbol, side_hint=side)
            
        except Exception as e:
            logger.error(f"Error handling signal: {e}")

    async def execute_order(self, symbol: str, side: str, analysis: Dict[str, Any]):
        """Executes a market order with dynamic sizing and leverage."""
        async with self.order_lock:
            try:
                # 🛡️ [BLACKLIST GUARD v37.1] (Moved to TOP)
                blacklist = self.config.get('blacklisted_symbols', [])
                if symbol in blacklist:
                    logger.warning(f"🚫 [BLACKLIST] Execution blocked for {symbol} as requested.")
                    return

                new_score = analysis.get('score', 0)
                logger.info(f"🚀 [EXECUTION] Triggering order for {symbol} | Side: {side.upper()} | Score: {new_score}")
                
                # ⚔️ [ATOMIC SWAP LOGIC] (v30.60)
                active_positions = await self.gateway.fetch_positions_robustly()
                if len(active_positions) >= self.max_concurrent_positions:
                    if new_score >= 0.90:
                        # Find weakest link
                        weakest_symbol = None
                        weakest_score = 2.0
                        
                        for p in active_positions:
                            s = p['symbol']
                            stored_trade = self.trade_levels.get(s, {})
                            # Use stored score or default to threshold
                            entry_score = stored_trade.get('entry_score', self.consensus_threshold)
                            if entry_score < weakest_score:
                                weakest_score = entry_score
                                weakest_symbol = s
                        
                        if weakest_symbol and (new_score - weakest_score) >= 0.15:
                            logger.critical(f"🔄 [ATOMIC SWAP] Replacing {weakest_symbol} (Score: {weakest_score}) with {symbol} (Score: {new_score})")
                            await self._close_position_internal(weakest_symbol, self.trade_levels.get(weakest_symbol), "SWAP_PRIORITY")
                            await asyncio.sleep(1)
                        else:
                            logger.info(f"⏭️ [LIMIT] Max positions ({len(active_positions)}) and no suitable swap candidate.")
                            return
                    else:
                        logger.info(f"⏭️ [LIMIT] Max positions reached ({len(active_positions)}).")
                        return
                
                # 🛡️ Conflict Resolver (Flip Logic & Duplicate Guard)
                positions = await self.gateway.fetch_positions_robustly()
                existing = next((p for p in positions if p['symbol'] == symbol), None)
                if existing:
                    existing_side = 'buy' if float(existing['positionAmt']) > 0 else 'sell'
                    if existing_side != side.lower():
                        is_strong = analysis.get('confidence', 0) > 0.85
                        if is_strong:
                            logger.critical(f"🆘 [FLIP] Reversal for {symbol}")
                            await self.gateway.close_all_for_symbol(symbol)
                            await asyncio.sleep(1)
                        else:
                            logger.info(f"⏭️ [CONFLICT] Mismatched side for {symbol}, but signal not strong enough to flip.")
                            return
                    else:
                        # v34.1: Duplicate Protection
                        logger.info(f"⏭️ [EXISTS] Position already open for {symbol} ({existing_side.upper()}). Skipping duplicate entry.")
                        return

                price = self.latest_data.get(symbol, {}).get('price', 0)
                if price <= 0: 
                    logger.error(f"❌ [EXECUTION ERROR] Valid price not found for {symbol}")
                    return
                
                amount = self._calculate_order_amount(symbol, price)
                leverage = int(analysis.get('leverage', self.leverage))
                
                logger.info(f"📦 [SIZING] Symbol: {symbol} | Price: {price} | Amount: {amount} | Lev: {leverage}x")
                
                if amount <= 0: 
                    logger.error(f"❌ [EXECUTION ERROR] Amount calculation resulted in 0 for {symbol}")
                    return

                # 🚀 SEND TO EXCHANGE
                # --- V29 LEVERAGE FLOOR ---
                if self.profile_type in ['blitz', 'extreme']:
                    leverage = max(leverage, 15)
                
                await self.gateway.set_leverage(symbol, leverage)
                logger.info(f"⚙️ [LEVERAGE] Set to {leverage} for {symbol}")
                
                await self.gateway.place_order(symbol, side.lower(), amount)
                logger.info(f"🔥 [EXCHANGE] Order placed for {symbol} | {side.upper()} @ {price}")
                
                # Register locally
                is_long = side.lower() in ['buy', 'long']
                
                # --- V29 RISK MATRIX ---
                sl_pct = self.config.get("trading_parameters", {}).get("stop_loss_pct", 0.02)
                tp_pct = self.config.get("trading_parameters", {}).get("take_profit_pct", 0.04)
                
                # 1. FETCH CANDLES (v31.40 - High Reliability)
                # v32.0: For Blitz and Aggressive, we favor Trailing Stop over Fixed TP
                use_trailing = self.profile_type in ['blitz', 'aggressive']
                initial_sl = price * (1 - sl_pct if is_long else 1 + sl_pct)
                
                self.trade_levels[symbol] = {
                    "symbol": symbol,
                    "side": 'long' if is_long else 'short',
                    "entry_price": price,
                    "entry_score": analysis.get('score', 0),
                    "opened_at": time.time(),
                    "amount": amount,
                    "sl": initial_sl,
                    "original_sl": initial_sl, # Persist for adaptive tightening
                    "tp1": 0 if use_trailing else price * (1 + tp_pct/2 if is_long else 1 - tp_pct/2),
                    "tp2": 0 if use_trailing else price * (1 + tp_pct if is_long else 1 - tp_pct),
                    "tp1_hit": False,
                    "status": "RUNNING",
                    "sl_tightness": "RUN"
                }
                self.db.save_state("trade_levels", self.trade_levels)
                await self.notifier.send_message(f"✅ *NUOVA POSIZIONE {side.upper()}*\n🪙 {symbol} @ {price}\nLev: {leverage}x | Size: {amount}")
            except Exception as e:
                logger.error(f"❌ [EXECUTION FAILED] {e}")

    async def close_position(self, symbol: str, trade: Dict[str, Any], reason: str = "EXIT"):
        """Closes 100% of the position with lock protection."""
        async with self.order_lock:
            await self._close_position_internal(symbol, trade, reason)

    async def _close_position_internal(self, symbol: str, trade: Dict[str, Any], reason: str = "EXIT"):
        """Internal logic for closure (no lock, for re-entrant calls)."""
        try:
            # Atomic Guard for closure
            await self.gateway.close_all_for_symbol(symbol)
            
            # Post-Mortem Learning
            if trade:
                curr_price = self.latest_data.get(symbol, {}).get('price', trade.get('entry_price', 0))
                pnl = (curr_price - trade['entry_price']) / trade['entry_price'] if trade['side'] == 'long' else (trade['entry_price'] - curr_price) / trade['entry_price']
                # v35.0: AI Audit disabled for cost/loop management
                # asyncio.create_task(self.strategy.analyst.perform_post_mortem(symbol, trade, pnl))

            if symbol in self.trade_levels:
                del self.trade_levels[symbol]
                
            self.db.save_state("trade_levels", self.trade_levels)
            await self.notifier.send_message(f"🏁 *POSIZIONE CHIUSA*\n🪙 {symbol} | 🎯 {reason}")
        except Exception as e:
            logger.error(f"❌ [CLOSE FAILED] {e}")

    async def execute_pivot(self, symbol: str, trade: Dict[str, Any], new_side: str):
        """
        [V29 ATOMIC PIVOT]
        Closes current position and immediately opens in opposite direction.
        """
        async with self.order_lock:
            try:
                logger.warning(f"🔄 [PIVOT] Reversing {symbol} to {new_side.upper()}...")
                # 1. Close current using internal logic (already locked)
                await self._close_position_internal(symbol, trade, "PIVOT_EXIT")
                
                # 2. Trigger immediate new analysis (v37.1 Atomic Logic)
                logger.info(f"⚡ [PIVOT] Launching immediate {new_side.upper()} re-entry...")
                await self.run_targeted_analysis(symbol, side_hint=new_side)
            except Exception as e:
                logger.error(f"❌ [PIVOT FAILED] {e}")

    async def partial_close_position(self, symbol: str, trade: Dict[str, Any], percent: float = 0.5, reason: str = "PARTIAL_EXIT"):
        """Closes a percentage of the position (e.g. 50% for TP1)."""
        try:
            # Fetch current position to get exact contracts
            pos = await self.gateway.fetch_atomic_position(symbol)
            if not pos: return
            
            contracts_to_close = float(pos['amount']) * percent
            side = 'sell' if pos['side'] == 'long' else 'buy'
            
            await self.gateway.place_order(symbol, side, contracts_to_close)
            
            # Update local state
            if trade:
                trade['amount'] = float(pos['amount']) - contracts_to_close
                if "TP1" in reason:
                    trade['tp1_hit'] = True
                self.trade_levels[symbol] = trade
                self.db.save_state("trade_levels", self.trade_levels)
            
            await self.notifier.send_message(f"✂️ *CHIUSURA PARZIALE ({percent*100:.0f}%)*\n🪙 {symbol} | 🎯 {reason}")
        except Exception as e:
            logger.error(f"❌ [PARTIAL CLOSE FAILED] {e}")

    def _calculate_order_amount(self, symbol, price):
        try:
            equity = self.latest_account_data.get('equity', 0)
            if equity <= 0: return 0
            
            risk_pct = self.config.get('trading_parameters', {}).get('percent_per_trade', 25.0) / 100.0
            margin_usdt = equity * risk_pct
            
            # 🔥 SMALL ACCOUNT OVERRIDE (v30.54)
            # If account is small ($12), 25% is $3. Bitget often requires 5 USDT margin.
            if margin_usdt < 5.1 and equity >= 5.1:
                logger.info(f"⚠️ [SMALL ACCT] Low margin {margin_usdt:.2f} -> Forcing 5.1 USDT Floor")
                margin_usdt = 5.1
            elif equity < 5.1:
                logger.error(f"❌ [SIZING] Equity too low ({equity:.2f}) to meet Bitget 5 USDT minimum.")
                return 0

            notional = margin_usdt * self.leverage
            
            # Ensure notional is also at least min_notional_usdt
            if notional < self.min_notional_usdt:
                notional = self.min_notional_usdt
                
            amount = notional / price
            final_amount = float(self.gateway.exchange.amount_to_precision(symbol, amount))
            
            logger.info(f"📦 [SIZING] Symbol: {symbol} | Price: {price} | Margin: {margin_usdt:.2f} | Notional: {notional:.2f} | Amount: {final_amount}")
            return final_amount
        except Exception as e:
            logger.error(f"Error calculating size: {e}")
            return 0

    async def run_reactive_safety_loop(self):
        """
        [V30.60 HIGH-SPEED SAFETY]
         독립적 인 루프 (Independent Loop). Fetch tickers directly for active positions 
         to ensure SL/TP/SOS reflexes are sub-second.
        """
        while True:
            try:
                if not self.initialized: await asyncio.sleep(5); continue
                
                # 🛡️ GLOBAL PANIC CHECK
                equity = self.latest_account_data.get('equity', 0)
                if await self.shield.check_panic_drawdown(equity, self.initial_wallet_balance):
                    await self.emergency_cleanup_all()
                    continue

                # Fetch fresh tickers for ALL active symbols in one call if possible
                active_symbols = list(self.active_positions.keys())
                if not active_symbols:
                    await asyncio.sleep(10); continue
                
                tickers = await self.gateway.exchange.fetch_tickers(active_symbols)
                
                for symbol in active_symbols:
                    trade = self.trade_levels.get(symbol)
                    if not trade: continue
                    
                    ticker = tickers.get(symbol, {})
                    curr_price = float(ticker.get('last', 0))
                    if curr_price <= 0: continue
                    
                    # Update latest price for the rest of the bot
                    if symbol not in self.latest_data: self.latest_data[symbol] = {}
                    self.latest_data[symbol]['price'] = curr_price
                    
                    # Fetch ATR (stale ATR from analysis loop is fine for scaling SL)
                    atr = self.latest_data.get(symbol, {}).get('atr', 0)
                    
                    exit_triggered, reason = await self.shield.check_position(symbol, trade, curr_price, current_atr=atr)
                    if exit_triggered:
                        if reason == "TAKE_PROFIT_1":
                            await self.partial_close_position(symbol, trade, 0.5, reason)
                        elif "PIVOT" in reason:
                            new_side = 'short' if trade['side'] == 'long' else 'long'
                            await self.execute_pivot(symbol, trade, new_side)
                        else:
                            await self.close_position(symbol, trade, reason)
                
                await asyncio.sleep(5) # Real-time reflex (5s)
            except Exception as e:
                logger.error(f"Safety Loop Error: {e}")
                await asyncio.sleep(10)

    async def run_zombie_sync_loop(self):
        """
        [V20.1 RECOVERY ENGINE]
        Periodically checks for untracked positions and adopts them.
        """
        while True:
            try:
                if not self.initialized: await asyncio.sleep(30); continue
                
                zombies = await self.gateway.sync_zombie_positions()
                for z in zombies:
                    symbol = z['symbol']
                    if symbol not in self.trade_levels or self.trade_levels[symbol] is None:
                        logger.warning(f"🧟 [ZOMBIE] Adopting {symbol} ({z['side'].upper()})")
                        # Recalculate targets based on Profile
                        is_long = z['side'] == 'long'
                        sl_pct = self.config.get("trading_parameters", {}).get("stop_loss_pct", 0.05)
                        tp_pct = self.config.get("trading_parameters", {}).get("take_profit_pct", 0.10)
                        entry = z['entry_price']
                        
                        self.trade_levels[symbol] = {
                            "symbol": symbol,
                            "side": z['side'],
                            "entry_price": entry,
                            "opened_at": time.time() - 3600, # Fake an hour ago
                            "amount": z['amount'],
                            "sl": entry * (1 - sl_pct if is_long else 1 + sl_pct),
                            "tp1": entry * (1 + tp_pct/2 if is_long else 1 - tp_pct/2),
                            "tp2": entry * (1 + tp_pct if is_long else 1 - tp_pct),
                            "tp1_hit": False,
                            "status": "RECOVERED_ZOMBIE"
                        }
                        self.db.save_state("trade_levels", self.trade_levels)
                        await self.notifier.send_message(f"🧟 *POSIZIONE ADOTTATA*\n🪙 {symbol} @ {entry}")
                
                await asyncio.sleep(300) # Every 5 minutes
            except Exception as e:
                logger.error(f"Zombie Sync Error: {e}")
                await asyncio.sleep(60)

    async def run_margin_cleanup_loop(self):
        """
        [V29 MARGIN DANGER]
        Closes worst position if margin ratio exceeds 95%.
        """
        while True:
            try:
                if not self.initialized: await asyncio.sleep(30); continue
                
                margin_ratio = self.latest_account_data.get('margin_ratio', 0)
                if margin_ratio >= 0.95:
                    logger.critical(f"⚠️ [MARGIN DANGER] Ratio at {margin_ratio*100:.1f}%. Executing Cleanup.")
                    positions = await self.gateway.fetch_positions_robustly()
                    if positions:
                        # Find worst PnL
                        worst = min(positions, key=lambda x: x['unrealized_pnl'])
                        logger.warning(f"🛡️ [CLEANUP] Closing worst position {worst['symbol']} (PnL: {worst['unrealized_pnl']})")
                        await self.close_position(worst['symbol'], self.trade_levels.get(worst['symbol']), "MARGIN_CLEANUP")
                
                await asyncio.sleep(20)
            except Exception as e:
                logger.error(f"Margin Cleanup Error: {e}")
                await asyncio.sleep(20)

    async def emergency_cleanup_all(self):
        """Emergency function to close every single open position immediately."""
        try:
            logger.warning("🛡️ Starting Emergency Cleanup of ALL positions...")
            positions = await self.gateway.fetch_positions_robustly()
            for pos in positions:
                await self.close_position(pos['symbol'], self.trade_levels.get(pos['symbol']), "EMERGENCY_CLEANUP")
            logger.warning("✅ All positions closed.")
        except Exception as e:
            logger.error(f"Emergency cleanup failed: {e}")

    async def run_deliberative_analysis_loop(self):
        """
        [v33.0 BRAIN - DYNAMIC 60]
        Ranked Opportunity Scanner with Dynamic AI Selection (30-min window).
        """
        while True:
            try:
                if not self.initialized: await asyncio.sleep(10); continue
                await self.sync_state()
                
                # 1. 🔄 DYNAMIC SYMBOL SELECTION (Every 30 Minutes)
                current_time = time.time()
                if not self.dynamic_symbols or (current_time - self.last_pair_update) > self.pair_update_interval:
                    logger.info("🧠 [AI SCANNER] Selecting 60 best candidates for the next 30 minutes...")
                    raw_assets = await self.scanner.scan(limit=150)
                    self.dynamic_symbols = await self.strategy.analyst.refine_market_selection(raw_assets, limit=60)
                    self.last_pair_update = current_time
                    logger.info(f"✅ [AI SCANNER] Dynamic window updated. Monitoring: {self.dynamic_symbols[:5]}... (+{len(self.dynamic_symbols)-5})")

                # 2. Scan and Filter based on Dynamic Set
                active_symbols = list(self.active_positions.keys())
                all_raw = await self.scanner.scan(active_symbols=active_symbols, limit=150)
                
                # Only keep assets that are in our dynamic 60 list
                assets = [a for a in all_raw if a['symbol'] in self.dynamic_symbols]
                candidates = []
                
                logger.info(f"🔍 [SCANNER] Analyzing {len(assets)} dynamic assets for technical setups...")
                
                # 3. FAST TECHNICAL FILTERING
                # We process them in chunks or all at once? Let's stay async.
                for asset in assets:
                    symbol = asset['symbol']
                    try:
                        ohlcv = await self.gateway.exchange.fetch_ohlcv(symbol, '15m', limit=50)
                        if not ohlcv: continue
                        
                        df = pd.DataFrame(ohlcv, columns=['timestamp','open','high','low','close','volume'])
                        self.latest_data[symbol] = {'price': df['close'].iloc[-1], 'df': df}
                        
                        # Get Technical-only score
                        tech_snapshot = await self.strategy.get_technical_score(symbol, {'df': df})
                        tech_score = tech_snapshot['tech_score']
                        
                        # 5. DATA SANITY GUARD (v35.0)
                        # If indicators are effectively zero/null, skip to avoid AI hallucination loops
                        if tech_score <= 0.0 or abs(tech_score) < 0.001:
                            logger.warning(f"⏭️ [SANITY] {symbol} has zero technical momentum. Skipping.")
                            continue
                        
                        # Minimum bar to be considered for Deep AI Audit
                        # v33.3 COST SAVER: Relaxed from 0.35 to 0.15 (0.0 was too expensive)
                        threshold = 0.15
                        if tech_score >= threshold: 
                            candidates.append({
                                'symbol': symbol,
                                'tech_snapshot': tech_snapshot,
                                'df': df
                            })
                    except Exception as e:
                        logger.warning(f"⚠️ Error filtering {symbol}: {e}")
                
                # 4. RANK BY TECHNICAL STRENGTH
                candidates.sort(key=lambda x: x['tech_snapshot']['tech_score'], reverse=True)
                # v33.4 ULTRA SAVER: Audit only top 1 promising setup
                top_candidates = candidates[:1]
                
                if top_candidates:
                    logger.info(f"🏆 [TOP CANDIDATS] Best {len(top_candidates)} tech setups found. Sending to Deep AI Audit...")
                else:
                    logger.info("⏭️ [SCAN] No viable technical setups found in this sweep.")
                    top_candidates = []
                
                # 5. DEEP AI AUDIT (Gemini 2.5 Flash)
                for cand in top_candidates:
                    symbol = cand['symbol']
                    tech_snapshot = cand['tech_snapshot']
                    
                    # Call Deep Analysis (Includes LLM)
                    cand['data'] = {'df': cand['df'], 'funding_rate': self.latest_data.get(symbol, {}).get('funding_rate', 0)}
                    analysis = await self.strategy.analyze_opportunity(symbol, cand['data'], tech_snapshot)
                    score = analysis.get('score', 0)
                    
                    # 6. EXECUTION TRIGGER
                    threshold = self.consensus_threshold
                    if score >= threshold:
                        logger.info(f"🎯 [TRIGGER] {symbol} score {score:.2f} meets threshold {threshold}!")
                        # v33.5: Use final side decided by consensus (AI + Tech)
                        final_side = analysis.get('side', 'buy')
                        await self.execute_order(symbol, final_side, analysis)
                        if len(self.active_positions) >= self.max_concurrent_positions:
                            break
                    else:
                        logger.info(f"⏭️ [SCAN] {symbol} score {score:.2f} insufficient.")
                
                # Sweep interval
                # v38.1 COST SAVER: Wait 10 minutes between sweeps to balance costs.
                wait_time = 600 
                logger.info(f"💤 [SLEEP] Cycle complete. Waiting {wait_time}s.")
                await asyncio.sleep(wait_time)
                
            except Exception as e:
                logger.error(f"Analysis Loop Error: {e}")
                await asyncio.sleep(60)

    async def run_news_radar_loop(self):
        """[V29] Periodically polls news and handles high-impact signals."""
        logger.info("📰 [BOOT] Starting News Radar loop...")
        while True:
            try:
                import httpx
                async with httpx.AsyncClient(timeout=30.0) as client:
                    self.news_radar.http_client = client
                    await self.news_radar.poll_news()
                await asyncio.sleep(300) # Poll every 5 mins (Maximum Alpha)
            except Exception as e:
                logger.error(f"❌ News Radar Loop Error: {e}")
                await asyncio.sleep(60)

    async def run_daily_audit_loop(self):
        """[V29] Nightly AI self-reflection on past performance."""
        while True:
            try:
                # Initial wait to not spam on restart
                await asyncio.sleep(3600) 
                logger.info("📊 [SCHEDULED] Initiating AI Performance Audit...")
                # Use robust history fetch
                history = self.db.get_trades(limit=50) 
                if history:
                    await self.strategy.analyst.perform_self_audit(history)
                await asyncio.sleep(82800) # Total 24h
            except Exception as e:
                logger.error(f"❌ Daily Audit Loop Error: {e}")
                await asyncio.sleep(3600)

    async def run_automated_report_loop(self):
        """[V29] Hourly HTML Dashboard generation."""
        while True:
            try:
                logger.info("📊 [REPORT] Regenerating Investor Dashboard...")
                self.reporter.generate_html()
                # v33.4 COST SAVER: Wait 5 minutes between sweeps
                await asyncio.sleep(300)
            except Exception as e:
                logger.error(f"❌ Reporter Loop Error: {e}")
                await asyncio.sleep(600)

async def main():
    bot = CryptoBot()
    await bot.initialize()
    # Keep the main coroutine alive forever
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.warning("Bot stopped by user.")
    except Exception as e:
        logger.critical(f"FATAL ERROR: {e}")
