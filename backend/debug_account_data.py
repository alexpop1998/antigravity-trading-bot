import asyncio
import os
from bot import CryptoBot
import json

async def debug_data():
    bot = CryptoBot()
    await bot.initialize()
    
    print("--- FETCHING BALANCE ---")
    balances = await bot.exchange.fetch_balance()
    # print(json.dumps(balances, indent=2))
    
    if 'info' in balances:
        info = balances['info']
        print(f"Binance Info - totalWalletBalance: {info.get('totalWalletBalance')}")
        print(f"Binance Info - totalMarginBalance: {info.get('totalMarginBalance')}")
        print(f"Binance Info - totalUnrealizedProfit: {info.get('totalUnrealizedProfit')}")
    
    usdt_bal = float(balances.get('USDT', {}).get('total', 0))
    usdc_bal = float(balances.get('USDC', {}).get('total', 0))
    print(f"Calculated Wallet Balance (USDT+USDC): {usdt_bal + usdc_bal}")

    print("\n--- FETCHING POSITIONS ---")
    positions = await bot.exchange.fetch_positions()
    active_pos = [p for p in positions if float(p.get('contracts', 0) or 0) != 0]
    
    for p in active_pos:
        print(f"Symbol: {p['symbol']}")
        print(f"  Side: {p['side']}")
        print(f"  Contracts: {p['contracts']}")
        print(f"  EntryPrice: {p['entryPrice']}")
        print(f"  MarkPrice: {p['markPrice']}")
        print(f"  UnrealizedPnL (CCXT): {p.get('unrealizedPnl')}")
        
        # Check Binance raw info
        if 'info' in p:
            print(f"  UnrealizedProfit (Raw): {p['info'].get('unrealizedProfit')}")
            
    await bot.exchange.close()

if __name__ == "__main__":
    asyncio.run(debug_data())
