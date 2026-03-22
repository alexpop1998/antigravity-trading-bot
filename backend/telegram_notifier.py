import httpx
import os
import logging
import asyncio

logger = logging.getLogger("TelegramNotifier")

class TelegramNotifier:
    def __init__(self):
        self.token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID")
        self.enabled = bool(self.token and self.chat_id and self.token != "YOUR_TELEGRAM_TOKEN")
        
        if not self.enabled:
            logger.warning("Telegram Notifier disabilitato: TELEGRAM_BOT_TOKEN o TELEGRAM_CHAT_ID mancanti nel .env")

    async def send_message(self, message):
        if not self.enabled:
            return

        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": message,
            "parse_mode": "Markdown"
        }
        
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(url, json=payload, timeout=10.0)
                if resp.status_code != 200:
                    logger.error(f"Errore invio Telegram: {resp.text}")
        except Exception as e:
            logger.error(f"Errore connessione Telegram: {e}")

    def notify_trade(self, symbol, side, price, amount, reason="", pnl=None):
        if side.upper() == "CLOSE":
            if pnl is not None:
                emoji = "💰" if pnl >= 0 else "💀"
                pnl_str = f"PnL: `{pnl:+.2f} USDT`"
            else:
                emoji = "🏁"
                pnl_str = ""
        else:
            emoji = "🚀" if side.lower() in ['buy', 'long'] else "🩸"
            pnl_str = ""

        msg = f"{emoji} *TRADE {'EXECUTED' if side.upper() != 'CLOSE' else 'CLOSED'}*\n\n" \
              f"Instrument: `{symbol}`\n" \
              f"Side: `{side.upper()}`\n" \
              f"Price: `{price}`\n" \
              f"Amount: `{amount}`\n"
        
        if pnl_str:
            msg += f"{pnl_str}\n"
            
        if reason:
            msg += f"Reason: `{reason}`"
            
        asyncio.create_task(self.send_message(msg))

    def notify_alert(self, type, title, value=""):
        msg = f"⚠️ *ALERT: {type}*\n\n" \
              f"Title: {title}\n" \
              f"Value: `{value}`"
        asyncio.create_task(self.send_message(msg))
