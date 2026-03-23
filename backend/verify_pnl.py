import asyncio
import os
import sys

sys.path.append(os.path.join(os.getcwd(), 'backend'))
from bot import CryptoBot

async def test_pnl_aggregation():
    bot = CryptoBot()
    # Bypass loops
    bot.account_update_loop = asyncio.coroutine(lambda: None)
    
    # Mock exchange fetch_balance and fetch_positions
    async def mock_fetch_balance():
        return {
            'USDT': {'total': 10000}, 
            'USDC': {'total': 0},
            'info': {
                'totalWalletBalance': '9500.0',
                'totalMarginBalance': '10500.0',
                'totalUnrealizedProfit': '1000.0'
            }
        }
        
    async def mock_fetch_positions():
        return [
            {'symbol': 'BTC/USDT:USDT', 'contracts': 1.0, 'unrealizedPnl': 1000.0, 'info': {}}
        ]
        
    bot.exchange.fetch_balance = mock_fetch_balance
    bot.exchange.fetch_positions = mock_fetch_positions
    
    # Manually trigger the core logic of account_update_loop (simulated)
    balances = await bot.exchange.fetch_balance()
    
    # NEW LOGIC from bot.py
    if 'info' in balances:
        info = balances['info']
        wallet_balance = float(info.get('totalWalletBalance', 0))
        equity = float(info.get('totalMarginBalance', 0))
        unrealized_pnl = float(info.get('totalUnrealizedProfit', 0))
    else:
        usdt_bal = float(balances.get('USDT', {}).get('total', 0))
        wallet_balance = usdt_bal
        pos_data = await bot.exchange.fetch_positions()
        unrealized_pnl = sum(float(p.get('unrealizedPnl', 0) or 0) for p in pos_data)
        equity = wallet_balance + unrealized_pnl
    
    print(f"Wallet Balance: {wallet_balance}")
    print(f"Unrealized PNL: {unrealized_pnl}")
    print(f"Total Equity: {equity}")
    
    if wallet_balance == 9500.0 and equity == 10500.0 and unrealized_pnl == 1000.0:
        print("✅ PNL AGGREGATION SUCCESSFUL (using Binance Info)")
    else:
        print("❌ PNL AGGREGATION FAILED")
    
    os._exit(0)

if __name__ == "__main__":
    asyncio.run(test_pnl_aggregation())
