import asyncio
import os
from bot import CryptoBot
import json

async def debug_data():
    bot = CryptoBot()
    await bot.initialize()
    
    print("--- FETCHING BALANCE ---")
    balances = await bot.exchange.fetch_balance()
    
    if 'info' in balances:
        print("Binance Info keys:", list(balances['info'].keys()))
        info = balances['info']
        print(f"totalWalletBalance: {info.get('totalWalletBalance')}")
        print(f"totalMarginBalance: {info.get('totalMarginBalance')}")
        print(f"totalUnrealizedProfit: {info.get('totalUnrealizedProfit')}")
        
        # Check assets list in info
        if 'assets' in info:
            for asset in info['assets']:
                if float(asset.get('walletBalance', 0)) != 0:
                    print(f"Asset: {asset['asset']}, Wallet: {asset['walletBalance']}, Margin: {asset.get('marginBalance')}")

    # Check CCXT parsed balances
    print("\nCCXT Parsed Balances (non-zero):")
    for currency, data in balances.items():
        if isinstance(data, dict) and 'total' in data and data['total'] != 0:
            print(f"{currency}: {data}")

    await bot.exchange.close()

if __name__ == "__main__":
    asyncio.run(debug_data())
