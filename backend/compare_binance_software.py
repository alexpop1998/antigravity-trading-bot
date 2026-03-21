import asyncio
import os
import json
import logging
from bot import CryptoBot
from dotenv import load_dotenv

# Configure logging to be less verbose for the report
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger("CompareTool")

async def run_comparison():
    print("\n" + "="*50)
    print("      BINANCE VS. SOFTWARE DATA COMPARISON")
    print("="*50 + "\n")

    # Initialize Bot
    bot = CryptoBot()
    
    # Force an account update to get the latest data from Binance
    print("Fetching live data from Binance...")
    # We call account_update_loop but since it's an infinite loop, 
    # we'll just manually trigger the sync parts or run it for one iteration.
    
    # Fetch Balance
    balance_data = await asyncio.to_thread(bot.exchange.fetch_balance)
    total_usdt = balance_data.get('total', {}).get('USDT', 0)
    wallet_balance = total_usdt
    margin_balance = total_usdt
    
    if 'info' in balance_data:
        info = balance_data['info']
        wallet_balance = float(info.get('totalWalletBalance', wallet_balance))
        margin_balance = float(info.get('totalMarginBalance', margin_balance))
        unrealized_pnl = margin_balance - wallet_balance
    else:
        unrealized_pnl = 0.0

    # Fetch Positions
    positions = await asyncio.to_thread(bot.exchange.fetch_positions)
    binance_positions = [p for p in positions if float(p.get('contracts', 0) or 0) != 0]
    
    # Software's Cached State (before sync)
    print("\n--- ACCOUNT SUMMARY ---")
    print(f"{'Metric':<20} | {'Binance Live':<15} | {'Software State':<15}")
    print("-" * 55)
    print(f"{'Wallet Balance':<20} | {wallet_balance:>15.4f} | {bot.latest_account_data['balance']:>15.4f}")
    print(f"{'Equity':<20} | {margin_balance:>15.4f} | {bot.latest_account_data['equity']:>15.4f}")
    print(f"{'Unrealized PnL':<20} | {unrealized_pnl:>15.4f} | {bot.latest_account_data['unrealized_pnl']:>15.4f}")

    print("\n--- POSITIONS COMPARISON ---")
    binance_symbols = set(p['symbol'] for p in binance_positions)
    software_symbols = set(p['symbol'] for p in bot.latest_account_data['positions'])
    
    all_symbols = binance_symbols.union(software_symbols)
    
    print(f"{'Symbol':<15} | {'Binance (Side/Size)':<25} | {'Software (Side/Size)':<25}")
    print("-" * 71)
    
    for symbol in sorted(all_symbols):
        b_pos = next((p for p in binance_positions if p['symbol'] == symbol), None)
        s_pos = next((p for p in bot.latest_account_data['positions'] if p['symbol'] == symbol), None)
        
        b_str = f"{b_pos['side']} {b_pos['contracts']}" if b_pos else "NONE"
        s_str = f"{s_pos['side']} {s_pos['contracts']}" if s_pos else "NONE"
        
        status = "✅" if b_str == s_str else "❌"
        print(f"{symbol:<15} | {b_str:<25} | {s_str:<25} {status}")

    print("\n" + "="*50)
    print("      ANALYSIS COMPLETE")
    print("="*50 + "\n")

if __name__ == "__main__":
    asyncio.run(run_comparison())
