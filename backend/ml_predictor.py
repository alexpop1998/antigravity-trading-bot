import logging
import threading

# Use sklearn's high-speed random forest to avoid huge memory footprint of deep learning libs for now
from sklearn.ensemble import RandomForestClassifier

logger = logging.getLogger("MLPredictor")

class MLPredictor:
    def __init__(self):
        # Cache one trained model per active symbol
        self.models = {}
        # Global lock to prevent concurrent CPU-heavy training across threads
        self._training_lock = threading.Lock()

    def train_and_predict(self, symbol, df):
        """
        Takes the freshly downloaded DataFrame for the symbol, extracts technical features,
        trains a Random Forest Classifier on the spot, and predicts the direction of the NEXT candle.
        """
        with self._training_lock:
            try:
                # We need sufficient data points
                if len(df) < 50:
                    logger.warning(f"[{symbol}] Insufficient data for AI training: {len(df)} candles.")
                    return None
                    
                logger.info(f"🧠 [AI ENGINE] Training and predicting for {symbol}...")
                    
                # Copy to avoid modifying the original running bot dataframe
                data = df.copy()
                
                # THE TARGET: Predicting the direction of the next period (1 = UP, 0 = DOWN)
                data['target'] = (data['close'].shift(-1) > data['close']).astype(int)
                
                # The features we feed the AI matrix: 
                # We use the mathematical formulas already extracted by the bot (avoiding re-computations)
                features = ['close', 'rsi', 'macd_hist', 'bb_upper', 'bb_lower', 'volume', 'atr']
                
                # Drop NaN rows (created by rolling averages and shifts)
                df_clean = data.dropna(subset=features + ['target'])
                
                if len(df_clean) < 30:
                    logger.warning(f"[{symbol}] Insufficient clean data for AI training.")
                    return None
                    
                X = df_clean[features].values
                y = df_clean['target'].values
                
                # Initialize a stable Random Forest Classifier (50 trees, single job to prevent CPU saturation)
                model = RandomForestClassifier(n_estimators=50, random_state=42, n_jobs=1)
                
                # Execute Neural Engine Training Memory
                model.fit(X, y)
                
                # Store trained model logically (for future fast-predict extensions without re-training every tick)
                self.models[symbol] = model
                
                # Predict the future (Target for the current unclosed/ongoing candle)
                # Use the very last row of original df (which has a NaN target, but we have its present features)
                last_row = data.iloc[-1]
                
                if last_row[features].isnull().any():
                    return None
                    
                # Ask the AI to predict direction probability based on the absolute current state
                current_state = last_row[features].values.reshape(1, -1)
                
                # Predict direction and confidence
                predicted_class = int(model.predict(current_state)[0])
                confidence = float(model.predict_proba(current_state)[0][predicted_class])
                
                return {
                    'direction': predicted_class,
                    'confidence': confidence
                }
                
            except Exception as e:
                logger.error(f"Failed ML prediction for {symbol}: {e}")
                return None
