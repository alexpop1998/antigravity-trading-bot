import websockets
import asyncio
import json
import logging

logger = logging.getLogger("SqueezeHunter")
logger.setLevel(logging.INFO)

class LiquidationHunter:
    def __init__(self, bot_instance, threshold_usd=500000):
        self.bot = bot_instance
        # Alert when a single liquidation is over $500k
        self.threshold_usd = threshold_usd
        # Binance Raw WebSocket for all market liquidations
        self.ws_url = "wss://fstream.binance.com/ws/!forceOrder@arr"
        
    async def monitor_liquidations(self):
        logger.info(f"Starting HFT Squeeze Hunter... Listening for liquidations > ${self.threshold_usd}")
        while True:
            try:
                async with websockets.connect(self.ws_url) as ws:
                    logger.info("✅ Connected to Binance Force Orders Stream (Liquidations).")
                    while True:
                        msg = await ws.recv()
                        data = json.loads(msg)
                        
                        if 'o' in data:
                            order = data['o']
                            symbol = order['s']
                            # "SELL" means a Long got liquidated (bearish cascade)
                            # "BUY" means a Short got liquidated (bullish squeeze)
                            side = order['S'] 
                            price = float(order['p'])
                            qty = float(order['q'])
                            value_usd = price * qty
                            
                            # Ensure the symbol matches ccxt format e.g. BTCUSDT -> BTC/USDT
                            formatted_symbol = f"{symbol[:-4]}/{symbol[-4:]}" if symbol.endswith("USDT") else symbol
                            
                            if value_usd >= self.threshold_usd:
                                cascade_type = "🩸 LONG LIQUIDATION (DUMP)" if side == 'SELL' else "🚀 SHORT SQUEEZE (PUMP)"
                                logger.critical(f"💥 MASSIVE LIQUIDATION DETECTED: {cascade_type} {qty} {symbol} @ ${price} (Value: ${value_usd:,.0f})")
                                
                                # HFT Squeeze Logic: Ride the mechanical cascade
                                if self.bot and formatted_symbol in self.bot.symbols:
                                    self.bot.add_alert("LIQUIDATION", f"{cascade_type} {symbol}", f"${price}")
                                    side_signal = 'sell' if side == 'SELL' else 'buy'
                                    asyncio.create_task(self.bot.handle_signal(formatted_symbol, "LIQUIDATION", side_signal, is_black_swan=True))

            except Exception as e:
                logger.error(f"SqueezeHunter disconnected: {e}. Reconnecting in 5s...")
                await asyncio.sleep(5)
