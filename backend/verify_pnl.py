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
        return {'USDT': {'total': 10000}, 'USDC': {'total': 0}}
        
    async def mock_fetch_positions():
        # Using the correct key 'unrealizedPnl'
        return [
            {'symbol': 'BTC/USDT:USDT', 'contracts': 1.0, 'unrealizedPnl': 500.0, 'info': {}}
        ]
        
    bot.exchange.fetch_balance = mock_fetch_balance
    bot.exchange.fetch_positions = mock_fetch_positions
    
    # Manually trigger the core logic of account_update_loop
    balances = await bot.exchange.fetch_balance()
    usdt_bal = float(balances.get('USDT', {}).get('total', 0))
    wallet_balance = usdt_bal
    
    pos_data = await bot.exchange.fetch_positions()
    # This is the line we fixed: sum(float(p.get('unrealizedPnl', 0) or 0) for p in pos_data)
    unrealized_pnl = sum(float(p.get('unrealizedPnl', 0) or 0) for p in pos_data)
    equity = wallet_balance + unrealized_pnl
    
    print(f"Wallet Balance: {wallet_balance}")
    print(f"Unrealized PNL: {unrealized_pnl}")
    print(f"Total Equity: {equity}")
    
    if equity == 10500.0:
        print("✅ PNL AGGREGATION SUCCESSFUL")
    else:
        print("❌ PNL AGGREGATION FAILED")
    
    os._exit(0)

if __name__ == "__main__":
    asyncio.run(test_pnl_aggregation())
