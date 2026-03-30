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
        self.active_exchange = os.getenv("ACTIVE_EXCHANGE", "unknown").upper()
        
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

    def notify_trade(self, symbol, side, price, amount, reason="", pnl=None, pnl_pct=None):
        # Mappa delle ragioni in italiano
        reasons_it = {
            "EXIT": "Chiusura Posizione",
            "PARTIAL_EXIT": "Chiusura Parziale TP1",
            "STOP-LOSS": "Stop-Loss Colpito",
            "TAKE-PROFIT": "Take-Profit Colpito",
            "STOP-LOSS (TRAILING)": "Trailing Stop Colpito",
            "TAKE-PROFIT 2 (FINAL)": "Take-Profit 2 (Finale)",
            "INIT_CONSENSUS": "Segnale Approvato",
            "CONSENSUS_HIT": "Obiettivo Consenso Raggiunto",
            "CASCADE": "Cascata Liquidazioni",
            "VELOCITY EXIT": "Uscita d'Emergenza (Volatilità)",
            "RECOVERY": "Recupero Rapido",
            "REJECTION": "Rifiuto ad Alta Volatilità",
            "VELOCITY MOMENTUM": "Breakout di Velocità",
            "MANUAL": "Chiusura Manuale",
            "CIRCUIT BREAKER": "Intervento Circuit Breaker",
            "DEX_ARBITRAGE": "Arbitraggio DEX",
            "LIQUIDATION": "Liquidazione Rilevata"
        }
        
        reason_it = reasons_it.get(reason, reason)

        if side.upper() == "CLOSE" or side.upper() == "CLOSE_PARTIAL":
            if pnl is not None:
                emoji = "💰" if pnl >= 0 else "💀"
                pnl_str = f"PnL: `{pnl:+.2f} USDT`"
                if pnl_pct is not None:
                    pnl_str += f" (`{pnl_pct:+.2f}% ROE`)"
            else:
                emoji = "🏁"
                pnl_str = ""
            status = "CHIUSO" if side.upper() == "CLOSE" else "PARZIALMENTE CHIUSO"
        else:
            emoji = "🚀" if side.lower() in ['buy', 'long'] else "🩸"
            pnl_str = ""
            status = "ESEGUITO"
 
        exchange_prefix = f" [{self.active_exchange}]" if self.active_exchange != "UNKNOWN" else ""
        msg = f"{emoji} *ORDINE {status}{exchange_prefix}*\n\n" \
              f"Strumento: `{symbol}`\n" \
              f"Tipo: `{side.upper()}`\n" \
              f"Prezzo: `{price}`\n" \
              f"Quantità: `{amount}`\n"
        
        if pnl_str:
            msg += f"{pnl_str}\n"
            
        if reason_it:
            msg += f"Motivazione: `{reason_it}`"
            
        asyncio.create_task(self.send_message(msg))

    def notify_alert(self, type, title, value=""):
        # Fail-safe: Silenzio totale per GATEKEEPER richiesto dall'utente (v10.3)
        if type == "GATEKEEPER":
            return
            
        # Traduzione dei tipi di alert
        types_it = {
            "WHALE": "🐳 MOVIMENTO WHALE",
            "CASCADE": "🩸 CASCATA LIQUIDAZIONI",
            "GATEKEEPER": "🛡️ GATEKEEPER AI",
            "CRITICAL": "🆘 CRITICO",
            "ERROR": "❌ ERRORE",
            "WARNING": "⚠️ AVVISO"
        }
        
        type_it = types_it.get(type, type)
        
        exchange_prefix = f" [{self.active_exchange}]" if self.active_exchange != "UNKNOWN" else ""
        msg = f"⚠️ *ALLERTA{exchange_prefix}: {type_it}*\n\n" \
              f"Dettaglio: {title}\n" \
              f"Valore: `{value}`"
        asyncio.create_task(self.send_message(msg))
