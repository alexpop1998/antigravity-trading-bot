import unittest
from unittest.mock import MagicMock, AsyncMock, patch
import os
import sys

# Ensure correct path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from bot import CryptoBot

class TestProfitabilityOptimization(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        # Mocking the exchange
        with patch('bot.CryptoBot._load_risk_profile', return_value={}):
            self.bot = CryptoBot()
            self.bot.exchange = MagicMock()
            self.bot.exchange.amount_to_precision = MagicMock(side_effect=lambda s, a: str(round(float(a), 4)))
            self.bot.exchange.price_to_precision = MagicMock(side_effect=lambda s, p: str(round(float(p), 4)))
            self.bot.exchange.market = MagicMock(return_value={'id': 'BTCUSDT'})
            self.bot.close_position = AsyncMock()

    async def test_fee_aware_break_even_long(self):
        symbol = "BTC/USDT"
        entry_price = 50000.0
        current_price = 51000.0 # Way above TP1
        
        # Scenario: TP1 hit for a LONG position
        self.bot.trade_levels[symbol] = {
            'side': 'buy',
            'entry_price': entry_price,
            'sl': 49000.0,
            'tp1': 50500.0,
            'tp2': 52000.0,
            'amount': 1.0,
            'tp1_hit': False,
            'signal_type': 'TECH'
        }
        self.bot.latest_data[symbol] = {'price': current_price, 'rsi': 50}
        
        # Trigger check_soft_stop_loss
        self.bot._check_soft_stop_loss(symbol, current_price)
        
        trade = self.bot.trade_levels[symbol]
        self.assertTrue(trade['tp1_hit'])
        
        # Expected SL = entry_price * (1 + 0.0015) = 50000 * 1.0015 = 50075.0
        expected_sl = 50000.0 * 1.0015
        self.assertAlmostEqual(float(trade['sl']), expected_sl)
        self.assertGreater(float(trade['sl']), entry_price)

    async def test_fee_aware_break_even_short(self):
        symbol = "BTC/USDT"
        entry_price = 50000.0
        current_price = 49000.0 # Below TP1
        
        # Scenario: TP1 hit for a SHORT position
        self.bot.trade_levels[symbol] = {
            'side': 'sell',
            'entry_price': entry_price,
            'sl': 51000.0,
            'tp1': 49500.0,
            'tp2': 48000.0,
            'amount': 1.0,
            'tp1_hit': False,
            'signal_type': 'TECH'
        }
        self.bot.latest_data[symbol] = {'price': current_price, 'rsi': 50, 'lowest_price': 50000.0}
        
        # Trigger check_soft_stop_loss
        self.bot._check_soft_stop_loss(symbol, current_price)
        
        trade = self.bot.trade_levels[symbol]
        self.assertTrue(trade['tp1_hit'])
        
        # Expected SL = entry_price * (1 - 0.0015) = 50000 * 0.9985 = 49925.0
        expected_sl = 50000.0 * 0.9985
        self.assertAlmostEqual(float(trade['sl']), expected_sl)
        self.assertLess(float(trade['sl']), entry_price)

    async def test_rsi_exit_guard_long(self):
        symbol = "BTC/USDT"
        entry_price = 50000.0
        
        # Scenario: RECOVERY LONG, RSI hits 50 but PnL is negative (slippage/spread)
        current_price = 49950.0 # Small loss
        rsi = 50.0 # Original exit point
        
        self.bot.trade_levels[symbol] = {
            'side': 'buy',
            'entry_price': entry_price,
            'sl': 48000.0,
            'tp1': 51000.0,
            'tp2': 52000.0,
            'amount': 1.0,
            'tp1_hit': False,
            'signal_type': 'RECOVERY',
            'highest_price': 50000.0,
            'lowest_price': 0
        }
        self.bot.latest_data[symbol] = {'price': current_price, 'rsi': rsi}
        
        self.bot._check_soft_stop_loss(symbol, current_price)
        
        # Should NOT have closed yet because RSI < 55 and PnL < 0.2%
        self.bot.close_position.assert_not_called()
        self.assertIsNotNone(self.bot.trade_levels[symbol])
        
        # Now simulate RSI 56 and PnL 0.6% (meets new 0.5% requirement)
        current_price = 50300.0 # +0.6%
        rsi = 56.0
        self.bot.latest_data[symbol] = {'price': current_price, 'rsi': rsi}
        self.bot._check_soft_stop_loss(symbol, current_price)
        
        # Should now close
        self.bot.close_position.assert_called()

if __name__ == "__main__":
    unittest.main()
