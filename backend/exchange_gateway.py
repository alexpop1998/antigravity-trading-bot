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

    def _initialize_exchange(self):
        if self.exchange_name == "bitget":
            return ccxt.bitget({
                'apiKey': os.getenv("BITGET_API_KEY"),
                'secret': os.getenv("BITGET_API_SECRET"),
                'password': os.getenv("BITGET_PASSWORD"),
                'enableRateLimit': True,
                'options': {'defaultType': 'swap'},
            })
        else:
            # Default to Binance USDM
            return ccxt.binanceusdm({
                'apiKey': os.getenv("EXCHANGE_API_KEY"),
                'secret': os.getenv("EXCHANGE_API_SECRET"),
                'enableRateLimit': True,
            })

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
        """
        if symbol in self.markets:
            return symbol
        
        # Try unslashed mapping
        clean_symbol = symbol.replace('/', '').replace(':', '')
        if clean_symbol in self.symbol_map:
            return self.symbol_map[clean_symbol]
            
        return symbol # Fallback to input

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
        """
        try:
            balance = await self.exchange.fetch_balance()
            equity = 0
            
            if self.exchange_name == "bitget":
                equity = float(balance.get('info', {}).get('equity', 0))
            else:
                equity = float(balance.get('total', {}).get('USDT', 0))
                
            return {
                'equity': equity,
                'raw': balance
            }
        except Exception as e:
            logger.error(f"❌ Failed to fetch balance: {e}")
            return {'equity': 0, 'raw': {}}

    async def place_order(self, symbol: str, side: str, amount: float, price: Optional[float] = None, params: Dict = None):
        """Standardized order placement with error handling."""
        try:
            symbol = self.normalize_symbol(symbol)
            if price:
                return await self.exchange.create_order(symbol, 'limit', side, amount, price, params or {})
            else:
                return await self.exchange.create_order(symbol, 'market', side, amount, None, params or {})
        except Exception as e:
            logger.error(f"❌ Order failed for {symbol}: {e}")
            raise e

    async def close_all_for_symbol(self, symbol: str):
        """Panic close for a specific symbol."""
        positions = await self.fetch_positions_robustly()
        for p in positions:
            if p['symbol'] == self.normalize_symbol(symbol):
                side = 'sell' if p['side'] == 'long' else 'buy'
                await self.place_order(symbol, side, p['contracts'])
                logger.warning(f"🛡️ [GATEWAY] Emergency closed {symbol}")

    async def set_leverage(self, symbol: str, leverage: int):
        """Centralized leverage management."""
        try:
            symbol = self.normalize_symbol(symbol)
            await self.exchange.set_leverage(leverage, symbol)
            logger.info(f"⚙️ [GATEWAY] Leverage set to {leverage}x for {symbol}")
        except Exception as e:
            logger.error(f"⚠️ Failed to set leverage for {symbol}: {e}")

    async def close(self):
        if self.exchange:
            await self.exchange.close()
