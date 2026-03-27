import pandas as pd
import numpy as np
import logging

logger = logging.getLogger("RegimeDetector")

class RegimeDetector:
    def __init__(self, adx_threshold=25, donchian_period=20):
        self.adx_threshold = adx_threshold
        self.donchian_period = donchian_period

    def calculate_indicators(self, df):
        """
        Manually calculates ADX and Donchian Channels using Pandas.
        This avoids extra dependencies while keeping calculation logic transparent.
        """
        # --- Donchian Channels ---
        df['donchian_high'] = df['high'].rolling(window=self.donchian_period).max()
        df['donchian_low'] = df['low'].rolling(window=self.donchian_period).min()
        df['donchian_mid'] = (df['donchian_high'] + df['donchian_low']) / 2
        
        # --- ADX (Average Directional Index) ---
        # 1. Calculate True Range (TR)
        df['h-l'] = df['high'] - df['low']
        df['h-pc'] = abs(df['high'] - df['close'].shift(1))
        df['l-pc'] = abs(df['low'] - df['close'].shift(1))
        df['tr'] = df[['h-l', 'h-pc', 'l-pc']].max(axis=1)
        
        # 2. Calculate Directional Movements (+DM, -DM)
        df['plus_dm'] = np.where((df['high'] - df['high'].shift(1)) > (df['low'].shift(1) - df['low']), 
                                  np.maximum(df['high'] - df['high'].shift(1), 0), 0)
        df['minus_dm'] = np.where((df['low'].shift(1) - df['low']) > (df['high'] - df['high'].shift(1)), 
                                   np.maximum(df['low'].shift(1) - df['low'], 0), 0)
        
        # 3. Smooth TR and DMs (Wilder's Smoothing)
        # Using simple moving average as an approximation or Wilder's specifically
        period = 14
        df['tr_smooth'] = df['tr'].rolling(window=period).mean() # In practice, Wilder's is preferred but SMA is common approximation
        df['plus_dm_smooth'] = df['plus_dm'].rolling(window=period).mean()
        df['minus_dm_smooth'] = df['minus_dm'].rolling(window=period).mean()
        
        # 4. Calculate DI+, DI-
        df['plus_di'] = 100 * (df['plus_dm_smooth'] / df['tr_smooth'])
        df['minus_di'] = 100 * (df['minus_dm_smooth'] / df['tr_smooth'])
        
        # 5. Calculate DX and ADX
        df['dx'] = 100 * (abs(df['plus_di'] - df['minus_di']) / (df['plus_di'] + df['minus_di']))
        df['adx'] = df['dx'].rolling(window=period).mean()
        
        return df

    def detect_regime(self, df):
        """
        Determines the market regime and returns a size multiplier.
        """
        if len(df) < self.donchian_period + 14:
            return "UNKNOWN", 1.0

        df = self.calculate_indicators(df)
        last_row = df.iloc[-1]
        
        adx = last_row['adx']
        close = last_row['close']
        d_high = last_row['donchian_high']
        d_low = last_row['donchian_low']
        d_mid = last_row['donchian_mid']
        
        # Regime Detection Logic
        if adx < self.adx_threshold:
            # RANGE REGIME (CHOP)
            regime = "RANGING"
            # Reduce size by 80% as suggested by DeepSeek
            multiplier = 0.2
        else:
            # TRENDING REGIME
            if close > d_mid:
                regime = "TRENDING_UP"
            elif close < d_mid:
                regime = "TRENDING_DOWN"
            else:
                regime = "TRENDING_NEUTRAL"
            
            # Full size for clear trends
            multiplier = 1.0

        logger.info(f"📍 REGIME DETECTED: {regime} (ADX: {adx:.1f}, Close: {close:.4f}, multiplier: {multiplier})")
        return regime, multiplier
