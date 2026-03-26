import websockets
import asyncio
import json
import logging

logger = logging.getLogger("SqueezeHunter")
logger.setLevel(logging.INFO)

class LiquidationHunter:
    def __init__(self, bot_instance, threshold_usd=500000):
        self.bot = bot_instance
        self.threshold_usd = threshold_usd
        self.ws_url = "wss://fstream.binance.com/ws/!forceOrder@arr"
        # --- NEW: Cascade Detection (DeepSeek Refinement) ---
        self.recent_liquidations = [] # List of (timestamp, side, symbol)
        
    async def monitor_liquidations(self):
        logger.info(f"Starting HFT Cascade Hunter... Monitoring for 3+ liquidations > $200k in 10s")
        while True:
            try:
                async with websockets.connect(self.ws_url) as ws:
                    logger.info("✅ Connected to Binance Liquidations Stream.")
                    while True:
                        msg = await ws.recv()
                        data = json.loads(msg)
                        
                        if 'o' in data:
                            order = data['o']
                            symbol = order['s']
                            side = order['S'] 
                            price = float(order['p'])
                            qty = float(order['q'])
                            value_usd = price * qty
                            
                            now = asyncio.get_event_loop().time()
                            
                            # Lower threshold for cascade components ($200k)
                            if value_usd >= 200000:
                                self.recent_liquidations.append((now, side, symbol))
                                
                                # Clean old events (>10s)
                                self.recent_liquidations = [l for l in self.recent_liquidations if now - l[0] <= 10]
                                
                                # Check for Cascade (3+ events of the same side)
                                side_liquidations = [l for l in self.recent_liquidations if l[1] == side]
                                
                                if len(side_liquidations) >= 3:
                                    cascade_type = "🩸 CASCADE DUMP" if side == 'SELL' else "🚀 CASCADE PUMP"
                                    logger.critical(f"💥 CASCADE DETECTED: {cascade_type} ({len(side_liquidations)} events in 10s). Current: {symbol} @ ${price}")
                                    
                                    formatted_symbol = f"{symbol[:-4]}/{symbol[-4:]}" if symbol.endswith("USDT") else symbol
                                    if self.bot and (formatted_symbol in self.bot.symbols or symbol in self.bot.symbols):
                                        target = formatted_symbol if formatted_symbol in self.bot.symbols else symbol
                                        self.bot.add_alert("CASCADE", f"{cascade_type} {symbol}", f"${price}")
                                        side_signal = 'buy' if side == 'SELL' else 'sell'
                                        # Bypass Consensus (Path A) - handle_signal will handle the lock
                                        asyncio.create_task(self.bot.handle_signal(target, "LIQUIDATION", side_signal, is_black_swan=True, current_price=price))
                                        # Clear list to avoid double-triggering the same cascade
                                        self.recent_liquidations.clear()

            except Exception as e:
                logger.error(f"SqueezeHunter disconnected: {e}. Reconnecting in 5s...")
                await asyncio.sleep(5)
