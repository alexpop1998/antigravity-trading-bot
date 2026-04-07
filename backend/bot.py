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
        profile = os.getenv('ACTIVE_PROFILE', os.getenv('CONFIG_PROFILE', 'aggressive')).lower()
        self.profile_type = profile
        self.config_file = f"config_{profile}.json"
        
        self.db = BotDatabase()
        self.notifier = TelegramNotifier()
        self.ai_semaphore = asyncio.Semaphore(5)
        
        self.config = self._load_config()
        self.profile_type = self.config.get("profile_type", "aggressive")
        tp = self.config.get('trading_parameters', {})
        self.consensus_threshold = float(tp.get('consensus_threshold', 0.55))
        self.min_ai_confidence = float(tp.get('min_ai_confidence', 0.55))
        self.leverage = int(tp.get('leverage', 10))
        self.percent_per_trade = float(tp.get('percent_per_trade', 25.0))
        self.max_concurrent_positions = int(tp.get('max_concurrent_positions', 3))
        self.stop_loss_pct = float(tp.get('stop_loss_pct', 0.02))
        self.take_profit_pct = float(tp.get('take_profit_pct', 0.04))
        self.min_notional_usdt = float(self.config.get("strategic_params", {}).get("min_notional_usdt", 5.0))
        
        # v43.3.1 [GWEN OVERDRIVE] Force Sizing 50% for Blitz
        if self.profile_type == 'blitz':
            self.percent_per_trade = max(self.percent_per_trade, 50.0)
            logger.warning("⚔️ [BLITZ OVERDRIVE] Sizing forced to 50% per trade.")

        exchange_name = self.config.get("strategic_params", {}).get("active_exchange", os.getenv("ACTIVE_EXCHANGE", "bitget"))
        self.active_exchange_name = exchange_name
        self.gateway = ExchangeGateway(exchange_name)
        self.shield = SafetyShield(self)
        self.strategy = StrategyEngine(self)
        self.scanner = AssetScanner(self.gateway.exchange)
        self.news_radar = NewsRadar(self)
        self.reporter = BotReporter()
        
        self.trade_levels = self.db.load_state("trade_levels") or {}
        self.active_positions = {}
        self.latest_data = {}
        self.latest_account_data = {'equity': 0.0, 'balance': 0.0, 'margin_ratio': 0.0}
        self.initial_wallet_balance = self.db.load_state("initial_balance") or 0.0
        self.semaphore = self.ai_semaphore
        self.order_lock = asyncio.Lock()
        
        self.initialized = False
        self.start_time = time.time()
        self.last_pair_update = 0
        self.pair_update_interval = 3600
        self.dynamic_symbols = []

    def _load_config(self):
        try:
            with open(self.config_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading config {self.config_file}: {e}")
            return {}

    async def initialize(self):
        if self.initialized: return
        try:
            await self.gateway.exchange.load_markets()
            await self.sync_state()
            if self.initial_wallet_balance <= 0:
                self.initial_wallet_balance = self.latest_account_data['equity']
                self.db.save_state("initial_balance", self.initial_wallet_balance)

            asyncio.create_task(self.run_main_atomic_loop())
            asyncio.create_task(self.run_reactive_safety_loop())
            asyncio.create_task(self.run_macro_regime_loop())
            asyncio.create_task(self.run_news_radar_loop())
            asyncio.create_task(self.run_daily_audit_loop())
            asyncio.create_task(self.run_automated_report_loop())
            
            self.initialized = True
            await self.notifier.send_message(f"🚀 *SNIPER ACTIVATED v43.3.1*\nProfile: {self.profile_type.upper()}")
        except Exception as e:
            logger.error(f"Initialization Failed: {e}")

    async def sync_state(self):
        """🧬 [ATOMIC PULSE] Unified state synchronization."""
        try:
            # v44.0.0 [GWEN FIX] Synchronous sequential fetch to ensure consistency
            balance_data = await self.gateway.fetch_balance_safe()
            self.latest_account_data['balance'] = float(balance_data.get('balance', 0))
            self.latest_account_data['equity'] = float(balance_data.get('equity', 0))
            self.latest_account_data['available'] = float(balance_data.get('available', 0))
            
            raw = balance_data['raw']
            self.latest_account_data['margin_ratio'] = float(raw.get('info', {}).get('marginRatio', 0)) if isinstance(raw.get('info'), dict) else 0.0
            
            # v44.0.0 [SYNC] Immediate position adoption (Zombie & Ghost protection)
            positions = await self.gateway.fetch_positions_robustly()
            current_symbols = [p['symbol'] for p in positions]
            
            # v44.0.0 [SIDE RECONCILE] Ensure local side matches exchange truth
            for p in positions:
                sym = p['symbol']
                exchange_side = p['side'].lower()
                
                if sym not in self.trade_levels:
                    logger.warning(f"🧟 [ADOPTION] Adopting orphan {sym} into internal levels.")
                    sl_pct = self.config.get("trading_parameters", {}).get("stop_loss_pct", 0.02)
                    self.trade_levels[sym] = {
                        'symbol': sym, 'side': exchange_side, 'entry_price': p['entry_price'],
                        'amount': p['amount'], 'leverage': p['leverage'], 'opened_at': time.time(),
                        'sl': p['entry_price'] * (1 - sl_pct if exchange_side == 'long' else 1 + sl_pct),
                        'tp1_hit': False, 'entry_score': 0.8 # Manual assumption
                    }
                else:
                    local_side = self.trade_levels[sym].get('side', '').lower()
                    if local_side != exchange_side:
                        logger.warning(f"🔄 [SIDE RECONCILE] {sym} mismatch: Local {local_side} vs Exch {exchange_side}. Correcting.")
                        self.trade_levels[sym]['side'] = exchange_side
                        # Force SL recalculation on side flip
                        sl_pct = self.config.get("trading_parameters", {}).get("stop_loss_pct", 0.02)
                        self.trade_levels[sym]['sl'] = p['entry_price'] * (1 - sl_pct if exchange_side == 'long' else 1 + sl_pct)
            
            # prune stale positions
            for sym in list(self.trade_levels.keys()):
                if sym not in current_symbols:
                    logger.warning(f"🧹 [PRUNE] Removing stale {sym} (Manual close detected).")
                    del self.trade_levels[sym]
            
            self.active_positions = {p['symbol']: ('LONG' if p['side'] == 'long' else 'SHORT') for p in positions}
            self.db.save_state("trade_levels", self.trade_levels)
        except Exception as e:
            logger.error(f"❌ [ATOMIC SYNC FAILED] {e}")

    async def handle_signal(self, symbol, source, side, confidence=1.0, is_black_swan=False, metadata=None):
        try:
            if symbol == "GLOBAL":
                target_symbols = list(self.latest_data.keys())[:3]
                for s in target_symbols:
                    await self.handle_signal(s, f"{source}_PROC", side, confidence, is_black_swan)
                return
            await self.run_targeted_analysis(symbol, side_hint=side)
        except Exception as e:
            logger.error(f"Error handling signal: {e}")

    async def run_targeted_analysis(self, symbol, side_hint=None):
        try:
            ohlcv = await self.gateway.exchange.fetch_ohlcv(symbol, '5m' if self.profile_type == 'blitz' else '15m', limit=50)
            df = pd.DataFrame(ohlcv, columns=['timestamp','open','high','low','close','volume'])
            self.latest_data[symbol] = {'price': df['close'].iloc[-1], 'df': df}
            tech_snapshot = await self.strategy.get_technical_score(symbol, {'df': df})
            analysis = await self.strategy.analyze_opportunity(symbol, {'df': df}, tech_snapshot)
            if analysis.get('score', 0) >= self.consensus_threshold:
                await self.execute_order(symbol, analysis.get('side', 'buy'), analysis)
        except Exception as e:
            logger.error(f"Targeted Analysis Failed for {symbol}: {e}")

    async def execute_order(self, symbol: str, side: str, analysis: Dict[str, Any]):
        async with self.order_lock:
            try:
                blacklist = self.config.get('blacklisted_symbols', [])
                if symbol in blacklist: return

                new_score = analysis.get('score', 0)
                ref_price = analysis.get('reference_price', 0)
                curr_price = self.latest_data.get(symbol, {}).get('price', 0)
                
                # v43.3 [GWEN FIX] Slippage Guard
                if ref_price > 0 and curr_price > 0:
                    slippage = abs(curr_price - ref_price) / ref_price
                    # [v52.1.0] [BLITZ OVERDRIVE] Increased slippage for Blitz (1.5%)
                    max_slip = 0.015 if self.profile_type == 'blitz' else 0.005
                    if slippage > max_slip:
                        logger.warning(f"🚫 [SLIPPAGE] {symbol} cancelled (Slip: {slippage:.2%}, Max: {max_slip:.2%}, Ref: {ref_price}, Curr: {curr_price})")
                        return

                logger.info(f"🚀 [EXECUTION] Triggering order for {symbol} | Score: {new_score}")
                
                # Atomic Swap & Limit Check
                active_positions = await self.gateway.fetch_positions_robustly()
                # v43.3.12 [SURVIVOR FIX] Force Swap if margin is low or max positions reached
                avail_margin = self.latest_account_data.get('available', 0)
                if len(active_positions) >= self.max_concurrent_positions or avail_margin < 2.0:
                    if new_score >= 0.90:
                        weakest_symbol = None
                        weakest_score = 2.0
                        for p in active_positions:
                            s = p['symbol']
                            entry_score = self.trade_levels.get(s, {}).get('entry_score', self.consensus_threshold)
                            if entry_score < weakest_score:
                                weakest_score = entry_score
                                weakest_symbol = s
                        
                        if weakest_symbol and (new_score - weakest_score) >= 0.10:
                            logger.warning(f"📉 [MARGIN SWAP] Clearing weakest position {weakest_symbol} to prioritize {symbol}")
                            await self._close_position_internal(weakest_symbol, self.trade_levels.get(weakest_symbol), "MARGIN_PRIORITY")
                            await asyncio.sleep(1)
                            # Refresh active positions after clear
                            active_positions = await self.gateway.fetch_positions_robustly()
                        if avail_margin < 2.0: 
                            logger.warning(f"🛡️ [MARGIN GUARD] {symbol} skipped. Insufficient margin ({avail_margin:.2f} USDT).")
                            return # No room for mid-score signals
                        else:
                            logger.warning(f"🛡️ [TECH GUARD] {symbol} score {new_score:.2f} insufficient for Margin Swap.")
                            return

                # Conflict/Flip Logic
                existing = next((p for p in active_positions if p['symbol'] == symbol), None)
                if existing:
                    existing_side = existing['side'].lower()
                    if existing_side != side.lower():
                        # v43.3.11 [GWEN FIX] Force Flip for Blitz always to free margin (Ignore 0.85 conf)
                        if self.profile_type == 'blitz' or analysis.get('confidence', 0) > 0.85:
                            logger.info(f"🔄 [FORCED FLIP] Closing opposite side for {symbol} to free margin.")
                            await self.gateway.close_all_for_symbol(symbol)
                            await asyncio.sleep(1)
                        else: 
                            logger.info(f"🛡️ [FLIP GUARD] Skipping {symbol} opposite side (Confidence too low for swap).")
                            return
                    else: 
                        logger.info(f"🛡️ [DUPLICATE GUARD] Already in {symbol} {existing_side}. Skipping.")
                        return

                price = curr_price or self.latest_data.get(symbol, {}).get('price', 0)
                leverage = int(analysis.get('leverage', self.leverage) if analysis else self.leverage)
                
                # v43.3.1 [GWEN OVERDRIVE] Target 50x for Blitz
                if self.profile_type == 'blitz':
                    leverage = max(leverage, 25)
                    if analysis.get('confidence', 0) > 0.85: leverage = 50
                
                # v43.4.4 [GWEN MASTER] Adaptive Sniper Sizing
                # v43.5.1 [ASYNC FIX] Now using await for sizing
                # v43.5.2 [DIAGNOSTIC] Checkpoint before sizing
                logger.info(f"📍 [CHECKPOINT] Calculating sizing for {symbol}...")
                amount = await self._calculate_order_amount(symbol, price, leverage=leverage)
                logger.info(f"📍 [CHECKPOINT] Sizing for {symbol}: {amount}")
                
                # v43.4.1 [GWEN FIX] Log if amount is too low to trade
                if amount <= 0:
                    logger.warning(f"⚠️ [EXECUTION] Skipping order for {symbol}: Calculated amount is 0.0 (Check margin balance).")
                    return
                
                await self.gateway.set_leverage(symbol, leverage)
                await self.gateway.place_order(symbol, side.lower(), amount)
                
                is_long = side.lower() in ['buy', 'long']
                sl_pct = self.config.get("trading_parameters", {}).get("stop_loss_pct", 0.02)
                initial_sl = price * (1 - sl_pct if is_long else 1 + sl_pct)
                
                self.trade_levels[symbol] = {
                    "symbol": symbol, "side": 'long' if is_long else 'short',
                    "entry_price": price, "entry_score": new_score,
                    "leverage": leverage, "opened_at": time.time(), "amount": amount,
                    "sl": initial_sl, "original_sl": initial_sl,
                    "tp1": 0, "tp1_hit": False, "status": "RUNNING", "sl_tightness": "RUN"
                }
                self.db.save_state("trade_levels", self.trade_levels)
                await self.notifier.send_message(f"✅ *NUOVA POSIZIONE {side.upper()}*\n🪙 {symbol} @ {price}\nLeva: {leverage}x | Sizing: {self.percent_per_trade}%")
            except Exception as e:
                logger.error(f"❌ [EXECUTION FAILED] {e}")

    async def close_position(self, symbol: str, trade: Dict[str, Any], reason: str = "EXIT"):
        async with self.order_lock:
            await self._close_position_internal(symbol, trade, reason)

    async def _close_position_internal(self, symbol: str, trade: Dict[str, Any], reason: str = "EXIT"):
        try:
            await self.gateway.close_all_for_symbol(symbol)
            if symbol in self.trade_levels: del self.trade_levels[symbol]
            self.db.save_state("trade_levels", self.trade_levels)
            await self.notifier.send_message(f"🏁 *POSIZIONE CHIUSA*\n🪙 {symbol} | 🎯 {reason}")
        except Exception as e:
            logger.error(f"❌ [CLOSE FAILED] {e}")

    async def execute_pivot(self, symbol: str, trade: Dict[str, Any], new_side: str):
        async with self.order_lock:
            try:
                await self._close_position_internal(symbol, trade, "PIVOT_EXIT")
                await self.run_targeted_analysis(symbol, side_hint=new_side)
            except Exception as e:
                logger.error(f"❌ [PIVOT FAILED] {e}")

    async def partial_close_position(self, symbol: str, trade: Dict[str, Any], percent: float = 0.5, reason: str = "PARTIAL_EXIT"):
        try:
            pos = await self.gateway.fetch_atomic_position(symbol)
            if not pos: return
            contracts_to_close = float(pos['amount']) * percent
            side = 'sell' if pos['side'] == 'long' else 'buy'
            await self.gateway.place_order(symbol, side, contracts_to_close)
            if trade:
                trade['tp1_hit'] = True
                self.trade_levels[symbol] = trade
                self.db.save_state("trade_levels", self.trade_levels)
            await self.notifier.send_message(f"✂️ *CHIUSURA PARZIALE*\n🪙 {symbol} | 🎯 {reason}")
        except Exception as e:
            logger.error(f"❌ [PARTIAL FAILED] {e}")

    async def _calculate_order_amount(self, symbol, price, leverage=None):
        """🧬 [LEGACY SIZING] Robust margin calculation with safety buffer."""
        try:
            # v44.0.0 [ZERO PRICE GUARD]
            if not price or float(price) <= 0:
                logger.error(f"❌ [SIZING] Invalid price for {symbol}: {price}. Skipping.")
                return 0
            
            # Use current atomic state (updated in main loop)
            equity = float(self.latest_account_data.get('equity', 0))
            avail = float(self.latest_account_data.get('available', 0))
            target_leverage = leverage or self.leverage
            
            # v44.0.0 [DEBUG LOG] Pure transparency
            logger.info(f"💰 [SIZING AUDIT] {symbol} | Avail: ${avail:.2f} | Eq: ${equity:.2f} | Pct: {self.percent_per_trade}%")
            
            if avail < 1.0:
                logger.warning(f"⚠️ [SIZING] Available margin too low ($ {avail:.2f}). Skipping.")
                return 0
            
            # Target sizing based on equity percentage (e.g., 50% for Blitz)
            target_margin = equity * (self.percent_per_trade / 100.0)
            
            # v44.0.0 [MARGIN BUFFER] Legacy v29 logic: use 95% of available to allow for fees/price-shift
            # Ensure we respect Bitget's $5 minimum notional by targeting at least $5.1
            margin_to_use = min(target_margin, avail * 0.95)
            
            # Hard floor for Bitget execution ($5 min notional)
            if (margin_to_use * target_leverage) < 5.0:
                # Try to boost margin to reach $5.1 if available
                if avail >= 5.1 / target_leverage:
                    margin_to_use = 5.1 / target_leverage
                else:
                    logger.warning(f"⚠️ [SIZING] Notional too low for {symbol} ($ {margin_to_use * target_leverage:.2f} < $5.0)")
                    return 0
                
            notional = margin_to_use * target_leverage
            amount = notional / price
            
            # Apply precision filter from exchange
            final_amount = float(self.gateway.exchange.amount_to_precision(symbol, amount))
            logger.info(f"✅ [SIZING DONE] {symbol} | Final Amount: {final_amount} (Notional: ${notional:.2f})")
            return final_amount
        except Exception as e:
            logger.error(f"❌ [SIZING ERROR] {e}")
            return 0

    async def run_main_atomic_loop(self):
        """🧬 [MAIN CORE] The Atomic Sniper Heartbeat (Inspired by v29)."""
        while True:
            try:
                if not self.initialized: await asyncio.sleep(10); continue
                
                # 1. ATOMIC SYNC: Ensured state consistency BEFORE analysis
                await self.sync_state()
                
                # 2. CIRCUIT BREAKER
                if await self.shield.check_daily_circuit_breaker():
                    logger.warning("🚨 [HALT] Daily Loss Limit reached.")
                    await asyncio.sleep(600); continue

                # 3. PANORAMIC SCAN (v46.0.0 [GWEN BLACKLIST ENFORCEMENT])
                # Uses AssetScanner to respect hard blacklist (AAPL, TSLA, Gold, etc.)
                active_symbols = list(self.active_positions.keys())
                raw_candidates = await self.scanner.scan(active_symbols=active_symbols, limit=60)
                
                logger.info(f"🔍 [PULSE] Scanner retrieved {len(raw_candidates)} elite crypto assets.")
                
                # 4. DEEP ANALYSIS
                sniper_candidates = []
                for cand in raw_candidates:
                    symbol = cand['symbol']
                    try:
                        ohlcv = await self.gateway.exchange.fetch_ohlcv(symbol, '5m' if self.profile_type == 'blitz' else '15m', limit=50)
                        df = pd.DataFrame(ohlcv, columns=['timestamp','open','high','low','close','volume'])
                        self.latest_data[symbol] = {'price': df['close'].iloc[-1], 'df': df}
                        tech_snapshot = await self.strategy.get_technical_score(symbol, {'df': df})
                        score = tech_snapshot.get('tech_score', 0)
                        
                        # v44.2.0 [GWEN MASTER FIX] - Remove invisible wall for Blitz
                        if self.profile_type == 'blitz' or score >= 0.25:
                            logger.info(f"🧬 [TECH AUDIT] {symbol} | Score: {score} | Status: {'PASS (Blitz Neutral)' if score < 0.25 else 'PASS'} -> Proceeding to AI Deep Analysis.")
                            sniper_candidates.append({'symbol': symbol, 'tech_snapshot': tech_snapshot, 'df': df})
                        else:
                            logger.info(f"⏭️ [TECH AUDIT] {symbol} | Score: {score} | Status: REJECTED (Below 0.25 floor)")
                    except: continue

                # 5. AI EXECUTION
                for cand in sniper_candidates[:20]:
                    symbol = cand['symbol']
                    analysis = None
                    for attempt in range(3):
                        try:
                            analysis = await self.strategy.analyze_opportunity(symbol, {'df': cand['df']}, cand['tech_snapshot'])
                            if analysis: break
                        except: await asyncio.sleep(2)
                    
                    if analysis and analysis.get('verdict') == 'APPROVE':
                          await self.execute_order(symbol, analysis.get('side', 'buy'), analysis)
                
                # 6. STAGNATION AUDIT (v44.1.0 [GWEN NORMALIZATION])
                # Auto-exit if position is dead flat for 3 hours
                for symbol, trade in list(self.trade_levels.items()):
                    if not trade: continue
                    age_h = (time.time() - trade.get('opened_at', 0)) / 3600
                    if age_h >= 3.0:
                         # Normalize symbol for ticker lookup (handle Bitget suffix)
                         ticker_key = symbol if symbol in tickers else next((k for k in tickers if k in symbol), None)
                         curr_price = float(tickers.get(ticker_key or symbol, {}).get('last', 0))
                         
                         if curr_price > 0:
                             entry = trade['entry_price']
                             pnl = abs(curr_price - entry) / entry
                             if pnl < 0.005: 
                                 logger.warning(f"🛡️ [STAGNATION EXIT] {symbol} flat for 3h. Freeing margin.")
                                 await self.close_position(symbol, trade, reason="STAGNATION_CLEANUP")

                # Sleep until next pulse
                wait_time = 300 if self.profile_type == 'blitz' else 600
                await asyncio.sleep(wait_time)
            except Exception as e:
                logger.error(f"❌ [CORE LOOP ERROR] {e}")
                await asyncio.sleep(60)

    async def run_reactive_safety_loop(self):
        while True:
            try:
                if not self.initialized: await asyncio.sleep(5); continue
                active_symbols = list(self.active_positions.keys())
                if not active_symbols: await asyncio.sleep(10); continue
                tickers = await self.gateway.exchange.fetch_tickers(active_symbols)
                for symbol in active_symbols:
                    trade = self.trade_levels.get(symbol)
                    if not trade: continue
                    curr_price = float(tickers.get(symbol, {}).get('last', 0))
                    if curr_price <= 0: continue
                    atr = self.latest_data.get(symbol, {}).get('atr', 0)
                    exit_triggered, reason = await self.shield.check_position(symbol, trade, curr_price, current_atr=atr)
                    if exit_triggered: await self.close_position(symbol, trade, reason)
                await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"Safety Loop Error: {e}")
                await asyncio.sleep(10)

    async def run_macro_regime_loop(self):
        while True:
            try:
                if not self.initialized: await asyncio.sleep(60); continue
                raw_market = await self.scanner.scan(limit=5)
                vibe = await self.strategy.perform_macro_audit(raw_market)
                if vibe.get('emergency_close', False):
                    for symbol in list(self.active_positions.keys()): await self.gateway.close_all_for_symbol(symbol)
                await asyncio.sleep(3600)
            except: await asyncio.sleep(300)

    # [v44.0.0] run_zombie_sync_loop and run_margin_cleanup_loop merged into run_main_atomic_loop

    async def run_news_radar_loop(self):
        while True:
            try:
                import httpx
                async with httpx.AsyncClient(timeout=30.0) as client:
                    self.news_radar.http_client = client
                    await self.news_radar.poll_news()
                await asyncio.sleep(300)
            except: await asyncio.sleep(60)

    async def run_daily_audit_loop(self):
        while True:
            try:
                await asyncio.sleep(3600)
                history = self.db.get_trades(limit=50)
                if history: await self.strategy.analyst.perform_self_audit(history)
                await asyncio.sleep(82800)
            except: await asyncio.sleep(3600)

    async def run_automated_report_loop(self):
        while True:
            try:
                self.reporter.generate_html()
                await asyncio.sleep(300)
            except: await asyncio.sleep(600)

async def main():
    bot = CryptoBot()
    await bot.initialize()
    while True: await asyncio.sleep(3600)

if __name__ == "__main__":
    try: asyncio.run(main())
    except Exception as e: logger.critical(f"FATAL: {e}")
