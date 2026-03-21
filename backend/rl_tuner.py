import asyncio
import json
import os
import logging
import random
from datetime import datetime

logger = logging.getLogger("RLTuner")

class RLTuner:
    def __init__(self, config_path="client_config.json", interval_hours=24):
        self.config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), config_path)
        self.interval_hours = interval_hours
        self.learning_rate = 0.05
    
    def _evaluate_current_policy(self, params):
        # In a real scenario, this queries the backtester or live PnL over the last 24h
        # Here we mock a reward signal between -1.0 and 1.0
        return random.uniform(-0.5, 1.0)
        
    def _update_hyperparameters(self, params, reward):
        # Q-Learning / PPO mock logic: Shift parameters towards profitability
        # If reward was highly positive, we might stick or tighten variants.
        # If negative, we mutate parameters slightly (exploration).
        
        rsi_period = params.get("rsi_period", 14)
        sl_pct = params.get("stop_loss_pct", 0.02)
        tp_pct = params.get("take_profit_pct", 0.06)
        
        if reward < 0:
            # Explore: shift RSI period by +/- 1 or 2
            rsi_period = max(7, min(21, rsi_period + random.choice([-1, 1])))
            # Explore: widen stop loss slightly to avoid getting chopped
            sl_pct = min(0.05, sl_pct * 1.1)
        else:
            # Exploit: tighten the take profit to secure gains faster
            tp_pct = max(0.02, tp_pct * 0.95)
            
        params["rsi_period"] = rsi_period
        params["stop_loss_pct"] = round(sl_pct, 4)
        params["take_profit_pct"] = round(tp_pct, 4)
        
        return params

    async def run_optimization_loop(self):
        logger.info("🧠 Auto-Tuner initialized. Running dynamic Q-Network updates...")
        while True:
            try:
                if os.path.exists(self.config_path):
                    with open(self.config_path, 'r') as f:
                        config = json.load(f)
                        
                    params = config.get("trading_parameters", {})
                    
                    # 1. Backtest/Evaluate current parameters over the last 24h
                    reward = self._evaluate_current_policy(params)
                    
                    # 2. Update params via RL logic
                    old_rsi = params.get("rsi_period")
                    new_params = self._update_hyperparameters(params, reward)
                    config["trading_parameters"] = new_params
                    
                    # 3. Write back to disk
                    with open(self.config_path, 'w') as f:
                        json.dump(config, f, indent=4)
                        
                    logger.info(f"🧠 RL Update Complete | Reward: {reward:.2f} | RSI Period: {old_rsi} -> {new_params['rsi_period']}")
                else:
                    logger.error("client_config.json not found for RL Tuner")
            except Exception as e:
                logger.error(f"Error in RL loop: {e}")
                
            # Sleep for the interval (simulate 24h, but we can do shorter for testing)
            # In production this would be interval_hours * 3600
            await asyncio.sleep(self.interval_hours * 3600)
