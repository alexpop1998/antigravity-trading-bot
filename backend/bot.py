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

            asyncio.create_task(self.run_deliberative_analysis_loop())
            asyncio.create_task(self.run_reactive_safety_loop())
            asyncio.create_task(self.run_zombie_sync_loop())
            asyncio.create_task(self.run_margin_cleanup_loop())
            asyncio.create_task(self.run_macro_regime_loop())
            asyncio.create_task(self.run_news_radar_loop())
            asyncio.create_task(self.run_daily_audit_loop())
            asyncio.create_task(self.run_automated_report_loop())
            
            self.initialized = True
            await self.notifier.send_message(f"🚀 *SNIPER ACTIVATED v43.3.1*\nProfile: {self.profile_type.upper()}")
        except Exception as e:
            logger.error(f"Initialization Failed: {e}")

    async def sync_state(self):
        try:
            balance_data = await self.gateway.fetch_balance_safe()
            self.latest_account_data['equity'] = balance_data['equity']
            raw = balance_data['raw']
            self.latest_account_data['margin_ratio'] = float(raw.get('info', {}).get('marginRatio', 0)) if isinstance(raw.get('info'), dict) else 0.0
            positions = await self.gateway.fetch_positions_robustly()
            self.active_positions = {p['symbol']: ('LONG' if p['side'] == 'long' else 'SHORT') for p in positions}
        except Exception as e:
            logger.error(f"Sync State Failed: {e}")

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
                    if slippage > 0.005:
                        logger.warning(f"🚫 [SLIPPAGE] {symbol} cancelled (Slip: {slippage:.2%})")
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
                        else:
                            if avail_margin < 2.0: return # Still no room
                    else:
                        if avail_margin < 2.0: return # No room for mid-score signals

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
                        else: return
                    else: return

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
        try:
            # v43.4.5 [BLINDATURA] Zero Price Protection
            if not price or float(price) <= 0:
                logger.error(f"❌ [SIZING] Invalid price for {symbol}: {price}. Skipping.")
                return 0
            
            # v43.3.11 [GWEN FIX] Use dynamic leverage for precise sizing
            # v43.5.1 [ASYNC FIX] Added await to gateway call
            balance = await self.gateway.fetch_balance_safe()
            target_leverage = leverage or self.leverage
            equity = float(balance.get('equity', 0) or self.latest_account_data.get('equity', 10.0))
            avail = float(balance.get('available', 0) or self.latest_account_data.get('available', 5.0))
            
            # v43.5.2 [FALLBACK] Ensure we have some default values if gateway fails
            if equity <= 0: equity = float(self.latest_account_data.get('equity', 10.0))
            if avail <= 0: avail = float(self.latest_account_data.get('available', 5.0))
            
            # v43.5.0 [DEBUG LOG] Force transparency on margin
            logger.info(f"💰 [MARGIN CHECK] {symbol} | Available: ${avail:.2f} | Equity: ${equity:.2f}")
            
            if avail < 1.0:
                return 0
            
            # v43.3.12 [SURVIVOR FIX] Adapt to available margin
            target_margin = equity * (self.percent_per_trade / 100.0)
            # Use 95% of available if we can't reach target, but ensure it's at least $1 for Bitget limits
            margin_to_use = min(max(5.1, target_margin), avail * 0.95)
            
            if margin_to_use < 1.0: 
                logger.warning(f"⚠️ [SURVIVOR] Insufficient margin for {symbol} ($ {margin_to_use:.2f})")
                return 0
                
            if margin_to_use < target_margin:
                logger.info(f"🦾 [SURVIVOR SIZING] Reduced margin for {symbol} ($ {margin_to_use:.2f})")
                
            notional = margin_to_use * target_leverage
            amount = notional / price
            return float(self.gateway.exchange.amount_to_precision(symbol, amount))
        except: return 0

    async def run_deliberative_analysis_loop(self):
        while True:
            try:
                if not self.initialized: await asyncio.sleep(10); continue
                await self.sync_state()
                
                if await self.shield.check_daily_circuit_breaker():
                    logger.warning("🚨 [HALT] Daily Loss Limit reached.")
                    await asyncio.sleep(600); continue
                
                # v43.5.3 [DB RESET] Clear ghost positions if no real positions on exchange
                # Ensures new Sniper positions can be opened after manual closes
                active_on_exchange = await self.gateway.fetch_positions_robustly()
                if not active_on_exchange:
                    logger.info("🧹 [DB RESET] No active positions on Bitget. Clearing local trade_levels ghost records.")
                    self.trade_levels = {}

                current_time = time.time()
                if not self.dynamic_symbols or (current_time - self.last_pair_update) > self.pair_update_interval:
                    raw_assets = await self.scanner.scan(limit=150)
                    sorted_assets = sorted(raw_assets, key=lambda x: x.get('volume', 0), reverse=True)
                    self.dynamic_symbols = [a['symbol'] for a in sorted_assets[:60]]
                    self.last_pair_update = current_time

                all_raw = await self.scanner.scan(active_symbols=list(self.active_positions.keys()), limit=150)
                assets = [a for a in all_raw if a['symbol'] in self.dynamic_symbols]
                candidates = []
                
                for asset in assets:
                    symbol = asset['symbol']
                    try:
                        ohlcv = await self.gateway.exchange.fetch_ohlcv(symbol, '5m' if self.profile_type == 'blitz' else '15m', limit=50)
                        df = pd.DataFrame(ohlcv, columns=['timestamp','open','high','low','close','volume'])
                        self.latest_data[symbol] = {'price': df['close'].iloc[-1], 'df': df}
                        tech_snapshot = await self.strategy.get_technical_score(symbol, {'df': df})
                        if tech_snapshot['tech_score'] >= 0.25: # v43.5.3 [SENSITIVITY] Lowered to 0.25 for immediate unlock
                            candidates.append({'symbol': symbol, 'tech_snapshot': tech_snapshot, 'df': df})
                    except: continue
                
                # v43.5.0 [COST Capping] Only analyze top 3 best signals per cycle
                candidates = sorted(candidates, key=lambda x: x['tech_snapshot']['tech_score'], reverse=True)[:3]
                
                if not candidates:
                    pass
                for cand in candidates[:3]:
                    symbol = cand['symbol']
                    # v43.5.3 [RESILIENCE] Added retry for LLM 503/Server errors
                    analysis = None
                    for attempt in range(3):
                        try:
                            analysis = await self.strategy.analyze_opportunity(symbol, {'df': cand['df']}, cand['tech_snapshot'])
                            if analysis: break
                        except Exception as e:
                            logger.warning(f"⚠️ [RETRY] AI Analysis attempt {attempt+1} failed for {symbol}: {e}")
                            await asyncio.sleep(3)
                    
                    if analysis and analysis.get('decision') == 'APPROVE':
                         await self.execute_order(symbol, analysis.get('side', 'buy'), analysis)
                
                # v43.3.1 [GWEN OVERDRIVE] Fast analysis for Blitz (5 min instead of 10 min)
                wait_time = 300 if self.profile_type == 'blitz' else 600
                await asyncio.sleep(wait_time)
            except Exception as e:
                logger.error(f"Analysis Loop Error: {e}")
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

    async def run_zombie_sync_loop(self):
        while True:
            try:
                if not self.initialized: await asyncio.sleep(10); continue
                zombies = await self.gateway.sync_zombie_positions()
                zombie_symbols = [self.gateway.normalize_symbol(z['symbol']) for z in zombies]
                
                # v43.4.0 [PRUNING FIX] Remove stale positions from internal database if not on exchange
                for symbol in list(self.trade_levels.keys()):
                    if symbol not in zombie_symbols:
                        logger.warning(f"🧹 [SYNC] Removing stale position for {symbol} (manual close detected).")
                        del self.trade_levels[symbol]
                        self.db.save_state("trade_levels", self.trade_levels)

                for z in zombies:
                    # v43.3.4 [GWEN FIX] ALWAYS Normalize symbol before adoption
                    symbol = self.gateway.normalize_symbol(z['symbol'])
                    if symbol not in self.trade_levels:
                        logger.warning(f"🧟 [ZOMBIE ADOPTION] Found orphan position for {symbol}. Adopting now.")
                        # v43.3.3 [GWEN FIX] Default SL at 1.5% from current entry if unknown
                        sl_price = z['entry_price'] * (0.985 if z['side'] == 'long' else 1.015)
                        self.trade_levels[symbol] = {
                            'symbol': symbol,
                            'side': z['side'],
                            'entry_price': z['entry_price'],
                            'sl': sl_price,
                            'opened_at': int(time.time()),
                            'leverage': z['leverage'],
                            'tp1_hit': False
                        }
                await asyncio.sleep(60) # Increased frequency for emergency recovery
            except Exception as e: 
                logger.error(f"❌ Zombie loop error: {e}")
                await asyncio.sleep(30)

    async def run_margin_cleanup_loop(self):
        while True:
            try:
                if not self.initialized: await asyncio.sleep(30); continue
                if self.latest_account_data.get('margin_ratio', 0) >= 0.95:
                    positions = await self.gateway.fetch_positions_robustly()
                    if positions:
                        worst = min(positions, key=lambda x: x['unrealized_pnl'])
                        await self.close_position(worst['symbol'], self.trade_levels.get(worst['symbol']), "MARGIN_CLEANUP")
                await asyncio.sleep(20)
            except: await asyncio.sleep(20)

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
