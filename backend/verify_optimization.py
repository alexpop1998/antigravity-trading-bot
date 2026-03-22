import asyncio
import logging
import os
import sys

# Add backend to path
sys.path.append(os.path.join(os.getcwd(), 'backend'))
from bot import CryptoBot

logging.basicConfig(level=logging.ERROR)

async def test_logic():
    # Mock bot instance
    bot = CryptoBot()
    # Bypass exchange calls with more robust mocks
    bot.exchange.load_markets = asyncio.coroutine(lambda: None)
    bot.exchange.set_leverage = asyncio.coroutine(lambda l, s: None)
    bot.exchange.create_market_order = asyncio.coroutine(lambda s, si, a: {'id': 'mock_id'})
    bot.exchange.create_order = asyncio.coroutine(lambda s, t, si, a, p, ex: {'id': 'mock_id'})
    bot.exchange.price_to_precision = lambda s, p: round(float(p), 2)
    bot.exchange.amount_to_precision = lambda s, a: round(float(a), 4)
    # Mock markets and market
    bot.exchange.markets = {'BTC/USDT:USDT': {'id': 'BTCUSDT'}}
    bot.exchange.market = lambda s: {'id': 'BTCUSDT', 'symbol': 'BTC/USDT:USDT'}
    
    # Mock account data
    bot.latest_account_data = {'equity': 10000, 'positions': []}
    bot.latest_data['BTC/USDT:USDT'] = {'price': 70000.0, 'atr': 700.0}
    
    # 1. Test Whale Scalping TP
    print("\n--- Testing WHALE Signal (Scalping) ---")
    bot.trade_levels['BTC/USDT:USDT'] = None # Reset
    await bot.execute_order('BTC/USDT:USDT', 'buy', 70000.0, is_black_swan=True, signal_type="WHALE")
    
    levels = bot.trade_levels.get('BTC/USDT:USDT')
    if levels:
        tp1_diff = ((levels['tp1'] - 70000) / 70000) * 100
        print(f"Signal Type: WHALE")
        print(f"Entry: {levels['entry_price']}")
        print(f"TP1: {levels['tp1']} ({tp1_diff:.2f}%) - Expecting 0.50%")
    else:
        print("Whale order execution failed.")
    
    # 2. Test Normal Signal TP (ATR based)
    print("\n--- Testing TECH Signal (Normal) ---")
    bot.trade_levels['BTC/USDT:USDT'] = None # Reset
    await bot.execute_order('BTC/USDT:USDT', 'buy', 70000.0, is_black_swan=False, signal_type="TECH")
    levels = bot.trade_levels.get('BTC/USDT:USDT')
    if levels:
        tp1_diff = ((levels['tp1'] - 70000) / 70000) * 100
        print(f"Signal Type: TECH")
        print(f"TP1: {levels['tp1']} ({tp1_diff:.2f}%) - Expecting 2.50% (700*2.5 = 1750)")
    else:
        print("TECH order execution failed.")

    os._exit(0)

if __name__ == "__main__":
    asyncio.run(test_logic())
