import asyncio
import os
import logging
from dotenv import load_dotenv
from exchange_gateway import ExchangeGateway

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Diagnostic")

async def check_nom():
    load_dotenv(override=True)
    gateway = ExchangeGateway("bitget")
    await gateway.load_markets()
    
    print("\n--- 🔍 [DIAGNOSTIC] Checking NOM/USDT Truth ---")
    pos = await gateway.fetch_atomic_position("NOM/USDT:USDT")
    if pos:
        print(f"✅ FOUND NOM POSITION: {pos}")
    else:
        print("❌ NO NOM POSITION FOUND (Maybe it's already closed or symbol mismatch)")
        
    print("\n--- 🕵️ [DIAGNOSTIC] Raw Bitget V2 Response (USDT-FUTURES) ---")
    res_usdt = await gateway.exchange.private_mix_get_v2_mix_position_all_position({'productType': 'USDT-FUTURES', 'marginCoin': 'USDT'})
    import json
    print(json.dumps(res_usdt, indent=2))
    
    print("\n--- 🕵️ [DIAGNOSTIC] Raw Bitget V2 Response (COIN-FUTURES) ---")
    res_coin = await gateway.exchange.private_mix_get_v2_mix_position_all_position({'productType': 'COIN-FUTURES'})
    print(json.dumps(res_coin, indent=2))
    
    print("\n--- 🧬 [DIAGNOSTIC] Checking All Positions ---")
    all_pos = await gateway.fetch_positions_robustly()
    for p in all_pos:
        print(f"📍 {p['symbol']}: {p['side'].upper()} | Amount: {p['contracts']} | Entry: {p['entry_price']}")
    
    await gateway.close()

if __name__ == "__main__":
    asyncio.run(check_nom())
