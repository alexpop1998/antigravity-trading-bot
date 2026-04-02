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

load_dotenv(override=True)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("CryptoBot")

class CryptoBot:
    def __init__(self, profile=None):
        load_dotenv(override=True)
        if profile is None:
            profile = os.getenv("CONFIG_PROFILE", "aggressive").lower()
        
        self.profile_type = profile
        self.config_file = f"config_{profile}.json"
        
        # 1. Database & Notifier
        self.db = BotDatabase()
        self.notifier = TelegramNotifier()
        
        # 2. Config Loading
        self.config = self._load_config()
        params = self.config.get("trading_parameters", {})
        
        # 3. Parameters
        self.symbols = params.get("symbols", ["BTC/USDT:USDT"])
        self.timeframe = params.get("timeframe", "15m")
        self.leverage = params.get("leverage", 10)
        self.stop_loss_pct = params.get("stop_loss_pct", 0.02)
        self.take_profit_pct = params.get("take_profit_pct", 0.06)
        self.consensus_threshold = float(params.get("consensus_threshold", 0.80))
        self.min_notional_usdt = float(self.config.get("strategic_params", {}).get("min_notional_usdt", 5.0))
        
        # 4. Modules
        exchange_name = self.config.get("strategic_params", {}).get("active_exchange", os.getenv("ACTIVE_EXCHANGE", "bitget"))
        self.gateway = ExchangeGateway(exchange_name)
        self.shield = SafetyShield(self)
        self.strategy = StrategyEngine(self)
        self.scanner = AssetScanner(self.gateway.exchange)
        
        # 5. Shared State
        self.trade_levels = self.db.load_state("trade_levels") or {}
        self.active_positions = {}
        self.latest_data = {}
        self.latest_account_data = {'equity': 0.0, 'balance': 0.0}
        
        # Locks & Cooldowns
        self.order_lock = asyncio.Lock()
        self.initialized = False

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
            
            # Start Loops
            asyncio.create_task(self.run_deliberative_analysis_loop())
            asyncio.create_task(self.run_reactive_safety_loop())
            
            self.initialized = True
            await self.notifier.send_message(f"🚀 *SISTEMA ATTIVO*\nProfilo: {self.profile_type.upper()}\nSoglia: {self.consensus_threshold}")
        except Exception as e:
            logger.error(f"Initialization Failed: {e}")

    async def sync_state(self):
        """Syncs balance and positions with exchange."""
        try:
            balance = await self.gateway.exchange.fetch_balance()
            self.latest_account_data['equity'] = float(balance.get('total', {}).get('USDT', 0))
            self.latest_account_data['balance'] = float(balance.get('free', {}).get('USDT', 0))
            
            positions = await self.gateway.fetch_positions_robustly()
            self.active_positions = {p['symbol']: ('LONG' if float(p['positionAmt']) > 0 else 'SHORT') for p in positions}
        except Exception as e:
            logger.error(f"Sync State Failed: {e}")

    async def execute_order(self, symbol: str, side: str, analysis: Dict[str, Any]):
        """Executes a market order with dynamic sizing and leverage."""
        async with self.order_lock:
            try:
                logger.info(f"🚀 [EXECUTION] Triggering order for {symbol} | Side: {side.upper()} | Score: {analysis.get('score', 0)}")
                
                # 🛡️ Conflict Resolver (Flip Logic)
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
                await self.gateway.set_leverage(symbol, leverage)
                logger.info(f"⚙️ [LEVERAGE] Set to {leverage} for {symbol}")
                
                await self.gateway.place_order(symbol, side.lower(), amount)
                logger.info(f"🔥 [EXCHANGE] Order placed for {symbol} | {side.upper()} @ {price}")
                
                # Register locally
                is_long = side.lower() == 'buy'
                self.trade_levels[symbol] = {
                    "symbol": symbol,
                    "side": 'long' if is_long else 'short',
                    "entry_price": price,
                    "amount": amount,
                    "sl": price * (1 - self.stop_loss_pct if is_long else 1 + self.stop_loss_pct),
                    "tp1": price * (1 + self.take_profit_pct/2 if is_long else 1 - self.take_profit_pct/2),
                    "opened_at": time.time(),
                    "tp1_hit": False
                }
                self.db.save_state("trade_levels", self.trade_levels)
                await self.notifier.send_message(f"✅ *NUOVA POSIZIONE {side.upper()}*\n🪙 {symbol} @ {price}")
            except Exception as e:
                logger.error(f"❌ [EXECUTION FAILED] {e}")

    async def close_position(self, symbol: str, trade: Dict[str, Any], reason: str = "EXIT"):
        try:
            await self.gateway.close_all_for_symbol(symbol)
            self.trade_levels[symbol] = None
            self.db.save_state("trade_levels", self.trade_levels)
            await self.notifier.send_message(f"🏁 *POSIZIONE CHIUSA*\n🪙 {symbol} | 🎯 {reason}")
        except Exception as e:
            logger.error(f"❌ [CLOSE FAILED] {e}")

    def _calculate_order_amount(self, symbol, price):
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
            logger.error(f"Error calculating size: {e}")
            return 0

    async def run_reactive_safety_loop(self):
        while True:
            try:
                if not self.initialized: await asyncio.sleep(5); continue
                positions = await self.gateway.fetch_positions_robustly()
                for p in positions:
                    symbol = p['symbol']
                    trade = self.trade_levels.get(symbol)
                    if trade:
                        curr_price = self.latest_data.get(symbol, {}).get('price', 0)
                        if curr_price <= 0: continue
                        
                        exit_triggered, reason = await self.shield.check_position(symbol, trade, curr_price)
                        if exit_triggered:
                            await self.close_position(symbol, trade, reason)
                await asyncio.sleep(10)
            except Exception as e:
                logger.error(f"Safety Loop Error: {e}")
                await asyncio.sleep(10)

    async def run_deliberative_analysis_loop(self):
        while True:
            try:
                if not self.initialized: await asyncio.sleep(10); continue
                await self.sync_state()
                assets = await self.scanner.scan()
                for asset in assets:
                    symbol = asset['symbol']
                    ohlcv = await self.gateway.exchange.fetch_ohlcv(symbol, '15m', limit=100)
                    if ohlcv:
                        df = pd.DataFrame(ohlcv, columns=['timestamp','open','high','low','close','volume'])
                        self.latest_data[symbol] = {'price': df['close'].iloc[-1], 'df': df}
                        
                        # 🚦 VERBOSE ANALYSIS
                        logger.info(f"🧪 [ANALYSIS] Evaluating {symbol} | Current Threshold: {self.consensus_threshold}")
                        analysis = await self.strategy.analyze_opportunity(symbol, {'df': df})
                        score = analysis.get('score', 0)
                        
                        if score >= self.consensus_threshold:
                            logger.info(f"🎯 [TRIGGER] {symbol} score {score} meets threshold {self.consensus_threshold}!")
                            await self.execute_order(symbol, analysis.get('side', 'buy'), analysis)
                        else:
                            logger.info(f"⏭️ [SCAN] {symbol} score {score:.2f} insufficient (Min: {self.consensus_threshold})")
                    await asyncio.sleep(2) # Rate limit protection
                await asyncio.sleep(60)
            except Exception as e:
                logger.error(f"Analysis Loop Error: {e}")
                await asyncio.sleep(60)
