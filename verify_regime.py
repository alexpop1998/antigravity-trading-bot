import pandas as pd
import numpy as np
import sys
import os

# Aggiunge il path per importare i moduli backend
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'backend')))

from regime_detector import RegimeDetector

def create_mock_data(regime="ranging"):
    """Crea dati finti per simulare i regimi di mercato."""
    df = pd.DataFrame({
        'timestamp': pd.date_range(start='2026-01-01', periods=100, freq='15min'),
        'open': np.nan, 'high': np.nan, 'low': np.nan, 'close': np.nan, 'volume': 1000
    })
    
    if regime == "ranging":
        # Prezzo che oscilla tra 100 e 105 (ADX dovrebbe essere basso)
        closes = [100 + (5 * np.sin(i * 0.5)) for i in range(100)]
        df['close'] = closes
        df['high'] = df['close'] + 0.5
        df['low'] = df['close'] - 0.5
    else:
        # Prezzo in trend forte (ADX alto)
        closes = [100 + (i * 2.0) for i in range(100)]
        df['close'] = closes
        df['high'] = df['close'] + 1.0
        df['low'] = df['close'] - 1.0
    
    return df

def test_regime_logic():
    detector = RegimeDetector()
    
    print("--- TESTING RANGING REGIME ---")
    df_ranging = create_mock_data("ranging")
    regime, multiplier = detector.detect_regime(df_ranging)
    print(f"Outcome: {regime}, Multiplier: {multiplier}")
    assert regime == "RANGING", f"Expected RANGING, got {regime}"
    assert multiplier == 0.2, f"Expected 0.2, got {multiplier}"
    
    print("\n--- TESTING TRENDING REGIME ---")
    df_trending = create_mock_data("trending")
    regime, multiplier = detector.detect_regime(df_trending)
    print(f"Outcome: {regime}, Multiplier: {multiplier}")
    assert "TRENDING" in regime, f"Expected TRENDING, got {regime}"
    assert multiplier == 1.0, f"Expected 1.0, got {multiplier}"
    
    print("\n✅ REGIME DETECTOR TEST PASSED!")

if __name__ == "__main__":
    try:
        test_regime_logic()
    except Exception as e:
        print(f"❌ TEST FAILED: {e}")
        sys.exit(1)
