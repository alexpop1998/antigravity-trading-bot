import websockets
import asyncio
import json
import logging
import time

logger = logging.getLogger("WhaleTracker")
logger.setLevel(logging.INFO)

class WhaleTracker:
    def __init__(self, bot_instance, threshold_btc=500):
        self.bot = bot_instance
        # We look for unconfirmed txs larger than 500 BTC (roughly $35M+)
        self.threshold_satoshis = threshold_btc * 100000000 
        self.ws_url = "wss://ws.blockchain.info/inv"
        
    async def monitor_whales(self):
        logger.info(f"Starting On-Chain Whale Tracker... Alert threshold: {self.threshold_satoshis/100000000} BTC")
        while True:
            try:
                # Need to run in an async context, ensuring websockets library is properly used
                async with websockets.connect(self.ws_url) as ws:
                    await ws.send(json.dumps({"op": "unconfirmed_sub"}))
                    logger.info("✅ Subscribed to Bitcoin Mempool.")
                    
                    while True:
                        msg = await ws.recv()
                        data = json.loads(msg)
                        
                        if data.get('op') == 'utx':
                            tx = data.get('x', {})
                            
                            # Calculate total output value of this transaction in Satoshis
                            total_value = sum([out.get('value', 0) for out in tx.get('out', [])])
                            
                            if total_value >= self.threshold_satoshis:
                                btc_value = total_value / 100000000
                                logger.critical(f"🚨 WHALE ALERT: Massive On-Chain Transfer Detected! {btc_value:.2f} BTC moved. Hash: {tx.get('hash')}")
                                
                                # Send signal to trading bot for extreme defensive SHORT (Anticipating a dump)
                                if self.bot:
                                    symbol = 'BTC/USDT:USDT'
                                    current_price = self.bot.latest_data.get(symbol, {}).get('price', 0)
                                    logger.warning(f"Whale Tracker triggering emergency SHORT on {symbol} @ ${current_price}")
                                    self.bot.add_alert("WHALE", f"Large Movement Detected: {btc_value:.2f} BTC", f"${current_price}")
                                    # Usa handle_signal invece di execute_order diretto
                                    asyncio.create_task(self.bot.handle_signal(symbol, "WHALE", "sell", is_black_swan=True))
                                
            except Exception as e:
                logger.error(f"WhaleTracker disconnect: {e}. Reconnecting in 5s...")
                await asyncio.sleep(5)
