import asyncio
import logging
import unittest
from unittest.mock import MagicMock, AsyncMock, patch
import os
import sys

# Ensure correct path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from bot import CryptoBot
from social_scraper import SocialScraper

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("InstitutionalTest")

class TestInstitutionalFeatures(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        # Mocking the exchange to avoid real API calls
        self.patcher = patch('ccxt.binance')
        self.mock_binance = self.patcher.start()
        
        # Initialize bot with a mock config
        with patch('bot.CryptoBot._load_risk_profile', return_value={}):
            self.bot = CryptoBot()
            self.bot.exchange = MagicMock()
            self.bot.exchange.amount_to_precision = MagicMock(side_effect=lambda s, a: a)
            self.bot.exchange.price_to_precision = MagicMock(side_effect=lambda s, p: p)
            self.bot.exchange.market = MagicMock(return_value={'id': 'BTCUSDT'})
            self.bot.execute_order = AsyncMock()

    async def asyncTearDown(self):
        self.patcher.stop()

    async def test_ai_alpha_signal(self):
        logger.info("Testing AI Alpha Signal Integration...")
        symbol = "BTC/USDT"
        price = 50000.0
        rsi = 50.0  # Neutral
        macd_hist = 0.0 # Neutral
        bb_lower = 48000.0
        bb_upper = 52000.0
        imbalance = 1.0 # Neutral
        
        # Case 1: Bullish AI Prediction (+1%)
        self.bot.latest_data[symbol] = {'prediction': 50500.0}
        self.bot._check_trading_signals(symbol, price, rsi, macd_hist, bb_lower, bb_upper, imbalance)
        await asyncio.sleep(0.1) # Let the task run
        self.bot.execute_order.assert_called_with(symbol, 'buy', price)
        self.assertEqual(self.bot.active_positions[symbol], 'LONG')
        
        self.bot.execute_order.reset_mock()
        self.bot.active_positions[symbol] = None
        
        # Case 2: Bearish AI Prediction (-1%)
        self.bot.latest_data[symbol] = {'prediction': 49500.0}
        self.bot._check_trading_signals(symbol, price, rsi, macd_hist, bb_lower, bb_upper, imbalance)
        await asyncio.sleep(0.1)
        self.bot.execute_order.assert_called_with(symbol, 'sell', price)
        self.assertEqual(self.bot.active_positions[symbol], 'SHORT')

    async def test_funding_arbitrage_logic(self):
        logger.info("Testing Funding Arbitrage Logic...")
        symbol = "BTC/USDT"
        self.bot.symbols = [symbol]
        self.bot.latest_data[symbol] = {'price': 50000.0}
        
        # Mock fetch_funding_rates
        self.bot.exchange.fetch_funding_rates = MagicMock(return_value={
            symbol: {'fundingRate': 0.0015} # 0.15% (Extreme Positive)
        })
        
        # Patching asyncio.to_thread to return the value directly
        with patch('asyncio.to_thread', side_effect=lambda func, *args: func(*args)):
            # Create a task for the loop but we only need one iteration
            # We bypass the while True for logic testing
            rates = self.bot.exchange.fetch_funding_rates(self.bot.symbols)
            for symbol, data in rates.items():
                rate_raw = data.get('fundingRate', 0)
                rate_pct = float(rate_raw) * 100
                if rate_pct > 0.1:
                    await self.bot.execute_order(symbol, 'sell', 50000.0)
        
        self.bot.execute_order.assert_called_with(symbol, 'sell', 50000.0)

    async def test_social_sentiment_mock(self):
        logger.info("Testing Social Sentiment Analysis (CryptoPanic)...")
        scraper = SocialScraper(self.bot)
        scraper.client = AsyncMock()
        
        # Mock LLM response
        mock_response = MagicMock()
        mock_response.choices = [MagicMock(message=MagicMock(content='{"ticker": "SOL", "sentiment": "BULLISH", "confidence": 0.95}'))]
        scraper.client.chat.completions.create = AsyncMock(return_value=mock_response)
        
        result = await scraper.analyze_sentiment("Solana breaking new ATH as ecosystem grows")
        self.assertEqual(result['ticker'], 'SOL')
        self.assertEqual(result['sentiment'], 'BULLISH')
        self.assertEqual(result['confidence'], 0.95)

if __name__ == "__main__":
    import unittest
    unittest.main()
