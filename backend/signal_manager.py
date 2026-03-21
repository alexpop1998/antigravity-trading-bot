import logging
import asyncio
import time

logger = logging.getLogger("SignalManager")

class SignalManager:
    def __init__(self, bot_instance):
        self.bot = bot_instance
        self.signals = {} # symbol -> list of current signals
        self.min_conviction = 1.5 # Soglia minima per eseguire un ordine
        
        # Pesi per i diversi strumenti istituzionali
        self.weights = {
            "NEWS": 1.0,
            "WHALE": 1.5,
            "LIQUIDATION": 0.8,
            "DEX_ARBITRAGE": 2.0,
            "GATEKEEPER": 1.2
        }

    async def add_signal(self, symbol, type, side, weight_modifier=1.0):
        weight = self.weights.get(type, 0.5) * weight_modifier
        score = weight if side.lower() in ['buy', 'long'] else -weight
        
        logger.info(f"🚦 [Signal] {type} per {symbol}: {side.upper()} (Peso: {weight})")
        
        # In una versione Pro, qui aggreghiamo segnali multipli in una finestra temporale
        # Per ora, se il segnale è molto forte (Whale o DEX), eseguiamo subito.
        if abs(score) >= self.min_conviction:
            logger.warning(f"🔥 CONVIZIONE ALTA raggiunta per {symbol} via {type}. Esecuzione immediata.")
            return True
        return False
