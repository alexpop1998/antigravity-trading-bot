import ccxt.async_support as ccxt
import logging
import asyncio
import os
from typing import Dict, Any, List, Optional

logger = logging.getLogger("ExchangeGateway")

class ExchangeGateway:
    """
    Centralized Gateway for CCXT interactions.
    Handles Symbol Normalization, Position Recovery, and Robust Order Placement.
    """
    
    def __init__(self, exchange_name: str = "bitget"):
        self.exchange_name = exchange_name.lower()
        self.exchange = self._initialize_exchange()
        self.markets = {}
        self.symbol_map = {} # Unslashed -> Canonical
        self.symbol_cache = {} # v37.1 Cache for speed

    def _initialize_exchange(self):
        exchange_id = self.exchange_name
        config = {
            'apiKey': os.getenv(f"{exchange_id.upper()}_API_KEY"),
            'secret': os.getenv(f"{exchange_id.upper()}_API_SECRET"),
            'password': os.getenv(f"{exchange_id.upper()}_PASSWORD"),
            'enableRateLimit': True,
            'timeout': 15000,
        }
        if exchange_id == "bitget":
            config['options'] = {'defaultType': 'swap'}
            return ccxt.bitget(config)
        else:
            return ccxt.binanceusdm(config)

    async def load_markets(self):
        """Pre-fetch and cache markets for fast symbol mapping."""
        try:
            self.markets = await self.exchange.load_markets()
            # Create a reverse map for unslashed symbols (e.g., NOMUSDT -> NOM/USDT:USDT)
            for symbol, market in self.markets.items():
                if market.get('id'):
                    self.symbol_map[market['id']] = symbol
            logger.info(f"✅ [{self.exchange_name.upper()}] Loaded {len(self.markets)} markets.")
        except Exception as e:
            logger.error(f"❌ Failed to load markets: {e}")

    def normalize_symbol(self, symbol: str) -> str:
        """
        Converts any symbol (NOMUSDT, NOM/USDT:USDT) to its canonical format.
        [v37.1] Cached for maximum speed.
        """
        if symbol in self.symbol_cache:
            return self.symbol_cache[symbol]
            
        if symbol in self.markets:
            res = symbol
        else:
            # Try unslashed mapping
            clean_symbol = symbol.replace('/', '').replace(':', '')
            res = self.symbol_map.get(clean_symbol, symbol)
            
        self.symbol_cache[symbol] = res
        return res

    async def fetch_positions_robustly(self) -> List[Dict[str, Any]]:
        """
        Fetches all active positions and normalizes them into a standard internal format.
        """
        try:
            raw_pos = await self.exchange.fetch_positions()
            active = []
            for p in raw_pos:
                # Filter out zero positions
                if float(p.get('contracts', 0)) > 0 or float(p.get('notional', 0)) > 0:
                    symbol = self.normalize_symbol(p['symbol'])
                    active.append({
                        'symbol': symbol,
                        'raw_symbol': p['symbol'],
                        'side': p['side'],
                        'contracts': float(p['contracts']),
                        'notional': float(p['notional']),
                        'entry_price': float(p['entryPrice']),
                        'unrealized_pnl': float(p['unrealizedPnl']),
                        'leverage': float(p['leverage']),
                        'timestamp': p['timestamp']
                    })
            return active
        except Exception as e:
            logger.error(f"❌ Failed to fetch positions: {e}")
            return []

    async def fetch_balance_safe(self) -> Dict[str, Any]:
        """
        Fetches account equity and balance safely, handling exchange-specific structures.
        v43.5.0 [TIMEOUT PROTECTION] Added explicit 10s wait_for.
        """
        try:
            # v43.5.0 [ROBUSTNESS] Force 10s timeout to prevent hanging
            # v43.5.2 [DIAGNOSTIC] Log API start
            logger.info("📍 [GATEWAY] Attempting Bitget balance fetch...")
            balance = await asyncio.wait_for(self.exchange.fetch_balance(), timeout=10.0)
            logger.info("✅ [GATEWAY] Bitget balance fetched successfully.")
            equity = 0.0
            
            if self.exchange_name == "bitget":
                info = balance.get('info', {})
                if isinstance(info, list):
                    for item in info:
                        if isinstance(item, dict):
                            val = item.get('accountEquity') or item.get('usdtEquity') or item.get('equity', 0)
                            equity += float(val)
                else:
                    equity = float(info.get('accountEquity') or info.get('usdtEquity') or info.get('equity', 0))
            
            avail = float(balance.get('free', {}).get('USDT', 0))
            
            # v43.4.1 [GWEN ROBUSTNESS] Fallback to raw info for Bitget V2 availability
            if self.exchange_name == "bitget" and avail <= 0:
                info = balance.get('info', {})
                if isinstance(info, list):
                    for item in info:
                        # v43.5.0 Improved check for Bitget V2 mapping
                        if item.get('marginCoin') == 'USDT' or item.get('coin') == 'USDT':
                            avail = float(item.get('available') or item.get('availableMargin') or item.get('availableAmount') or 0)
                            break
            
            return {'equity': equity, 'available': avail, 'raw': balance}
        except asyncio.TimeoutError:
            logger.error("🛑 [GATEWAY] Balance fetch TIMEOUT after 10s.")
            return {'equity': 0.0, 'available': 0.0, 'raw': {}}
        except Exception as e:
            logger.error(f"❌ Failed to fetch balance: {e}")
            return {'equity': 0.0, 'available': 0.0, 'raw': {}}

    async def place_order(self, symbol: str, side: str, amount: float, price: Optional[float] = None, params: Dict = None, is_close: bool = False):
        """Standardized order placement with error handling."""
        try:
            symbol = self.normalize_symbol(symbol)
            params = params or {}
            
            # 🛡️ BITGET HEDGED-MODE CLOSURE FIX (v31.17)
            if is_close and self.exchange_name == "bitget":
                params['reduceOnly'] = True
                # If we are selling to close, the holdSide must be 'long'
                params['holdSide'] = 'long' if side.lower() == 'sell' else 'short'
                logger.info(f"🛡️ [GATEWAY] Applying Close-Only params for {symbol} | Side: {side}")

            if price:
                return await self._execute_with_backoff(self.exchange.create_order, symbol, 'limit', side, amount, price, params)
            else:
                return await self._execute_with_backoff(self.exchange.create_order, symbol, 'market', side, amount, None, params)
        except Exception as e:
            logger.error(f"❌ Order failed for {symbol}: {e}")
            raise e

    async def close_all_for_symbol(self, symbol: str):
        """
        Panic close for a specific symbol.
        [v37.1] Optimized to only fetch relevant position.
        """
        p = await self.fetch_atomic_position(symbol)
        if p and p['amount'] > 0:
            side = 'sell' if p['side'] == 'long' else 'buy'
            await self.place_order(symbol, side, p['amount'], is_close=True)
            logger.warning(f"🛡️ [GATEWAY] Emergency closed {symbol}")

    async def _execute_with_backoff(self, func, *args, **kwargs):
        """
        [v37.1] Retries an async function with exponential backoff.
        """
        max_retries = 3
        for i in range(max_retries):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                if i == max_retries - 1: raise e
                wait = (2 ** i) + 0.1
                logger.warning(f"⚠️ [BACKOFF] Retrying {func.__name__} in {wait:.1f}s... ({e})")
                await asyncio.sleep(wait)

    async def set_leverage(self, symbol: str, leverage: int, params: Optional[Dict] = None):
        """Centralized leverage management with Hedged-mode support (Bitget)."""
        try:
            symbol = self.normalize_symbol(symbol)
            # v17.16: Force both sides for Bitget Hedged Mode if params are not provided
            if self.exchange_name == "bitget" and not params:
                await self._execute_with_backoff(self.exchange.set_leverage, leverage, symbol, params={'marginCoin': 'USDT', 'holdSide': 'long'})
                await self._execute_with_backoff(self.exchange.set_leverage, leverage, symbol, params={'marginCoin': 'USDT', 'holdSide': 'short'})
            else:
                await self._execute_with_backoff(self.exchange.set_leverage, leverage, symbol, params or {})
            logger.info(f"⚙️ [GATEWAY] Leverage set to {leverage}x for {symbol}")
        except Exception as e:
            logger.error(f"⚠️ Failed to set leverage for {symbol}: {e}")

    async def fetch_atomic_position(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        [V19.0 ATOMIC GUARD]
        Performs a real-time, high-priority exchange check to verify position state.
        Bypasses local cache for absolute accuracy.
        """
        try:
            symbol = self.normalize_symbol(symbol)
            if self.exchange_name == "bitget":
                # Direct V2 endpoint for Bitget truth
                params = {'productType': 'USDT-FUTURES', 'marginCoin': 'USDT'}
                res = await self.exchange.private_mix_get_v2_mix_position_all_position(params)
                pos_list = res.get('data', [])
                # Use unslashed ID for matching in Bitget V2
                market_id = next((m['id'] for s, m in self.markets.items() if s == symbol), symbol)
                matching = next((p for p in pos_list if p.get('symbol') == market_id), None)
                if matching and abs(float(matching.get('total', 0))) > 0:
                    return {
                        'symbol': symbol,
                        'side': matching.get('holdSide', '').lower(),
                        'amount': abs(float(matching.get('total', 0))),
                        'entry_price': float(matching.get('openPriceAvg', 0)),
                        'leverage': float(matching.get('leverage', 0))
                    }
            else:
                # Standard CCXT path for Binance/Others
                raw_pos = await self.exchange.fetch_positions([symbol])
                matching = next((p for p in raw_pos if p.get('symbol') == symbol and float(p.get('contracts', 0)) != 0), None)
                if matching:
                    return {
                        'symbol': symbol,
                        'side': matching['side'].lower(),
                        'amount': float(matching['contracts']),
                        'entry_price': float(matching['entryPrice']),
                        'leverage': float(matching['leverage'])
                    }
            return None
        except Exception as e:
            logger.error(f"⚠️ Atomic check failed for {symbol}: {e}")
            return None

    async def sync_zombie_positions(self) -> List[Dict[str, Any]]:
        """
        [V20.1 RECOVERY ENGINE]
        Scans all open positions and filters for untracked orphans.
        """
        try:
            if self.exchange_name == "bitget":
                params = {'productType': 'USDT-FUTURES', 'marginCoin': 'USDT'}
                res = await self.exchange.private_mix_get_v2_mix_position_all_position(params)
                raw_list = res.get('data', [])
                zombies = []
                for p in raw_list:
                    if abs(float(p.get('total', 0))) > 0:
                        symbol = self.normalize_symbol(p.get('symbol'))
                        zombies.append({
                            'symbol': symbol,
                            'side': p.get('holdSide', '').lower(),
                            'amount': abs(float(p.get('total', 0))),
                            'entry_price': float(p.get('openPriceAvg', 0)),
                            'leverage': int(p.get('leverage', 10))
                        })
                return zombies
            else:
                raw_pos = await self.exchange.fetch_positions()
                return [p for p in raw_pos if float(p.get('contracts', 0)) != 0]
        except Exception as e:
            logger.error(f"❌ Zombie sync failed: {e}")
            return []

    async def close(self):
        if self.exchange:
            await self.exchange.close()
