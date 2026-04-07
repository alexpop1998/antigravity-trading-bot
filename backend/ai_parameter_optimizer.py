import os
import json
import logging
import asyncio
import time
from typing import Dict, Any
from openai import AsyncOpenAI

logger = logging.getLogger("AIParameterOptimizer")
logger.setLevel(logging.INFO)

class AIParameterOptimizer:
    def __init__(self, bot_instance):
        self.bot = bot_instance
        self.api_key = os.getenv("LLM_API_KEY")
        self.base_url = os.getenv("LLM_BASE_URL", "https://api.deepseek.com")
        self.model_name = os.getenv("LLM_MODEL_NAME", "deepseek-chat")
        
        # --- v45.0.0 DEEPSEEK (OpenAI-Compatible) ---
        self.ai_client = AsyncOpenAI(
            api_key=self.api_key,
            base_url=f"{self.base_url}/v1"
        ) if self.api_key else None
        
        self.config_path = os.path.join(os.path.dirname(__file__), self.bot.config_file)
        self.last_run = 0
        self.run_interval = 3600 # 1 hour

    async def run_optimization_cycle(self):
        """Main loop for AI-driven parameter optimization."""
        if not self.ai_client:
            return

        now = time.time()
        if now - self.last_run < self.run_interval:
            return

        logger.info("🧠 [AI OPTIMIZER] Starting global risk optimization cycle...")
        
        try:
            # 1. Collect Global Market Context
            # We aggregate stats from all monitored symbols
            total_volatility = 0
            avg_funding = 0
            count = 0
            
            for symbol, data in self.bot.latest_data.items():
                price = data.get('price', 0)
                atr = data.get('atr', 0)
                if price > 0 and isinstance(atr, (int, float)):
                    total_volatility += (atr / price)
                    avg_funding += data.get('funding_rate', 0)
                    count += 1
            
            market_vol = (total_volatility / count) if count > 0 else 0.01
            market_funding = (avg_funding / count) if count > 0 else 0
            
            # 2. Collect Portfolio Context
            equity = self.bot.latest_account_data.get('equity', 1000)
            margin_ratio = self.bot.current_margin_ratio
            active_positions = len([p for p in self.bot.trade_levels.values() if p])
            
            # 3. Collect Recent Performance (24h)
            perf_stats = self.bot.db.get_ai_performance_stats(hours=24)
            
            # 4. Construct Prompt
            prompt = f"""
            Sei il Chief Risk Officer di un Hedge Fund AI. Il tuo compito è ottimizzare i parametri globali del bot per l'ora successiva.
            
            STATO ATTUALE MERCATO:
            - Volatilità Media (ATR/Price): {market_vol:.4f}
            - Funding Rate Medio: {market_funding:.4f}
            
            STATO PORTAFOGLIO:
            - Equity: {equity:.2f} USDT
            - Margin Ratio Attuale: {margin_ratio*100:.1f}%
            - Posizioni Aperte: {active_positions}
            
            PERFORMANCE ULTIME 24H:
            {json.dumps(perf_stats, indent=2)}
            
            CONFIGURAZIONE ATTUALE:
            {json.dumps(self.bot.risk_profile.get('trading_parameters', {}), indent=2)}
            
            MISSIONE:
            Ottimizza i parametri per massimizzare il profitto proteggendo il capitale. 
            - Se la volatilità è alta (>0.02), sii più conservativo (alza la soglia consenso, abbassa la size).
            - Se l'accuracy recente è bassa (<45%), riduci l'aggressività.
            - Se il margin ratio è alto (>50%), rallenta le nuove entrate.
            
            REGOLE DI SICUREZZA (GUARDRAILS):
            - consensus_threshold: min 1.2, max 4.0
            - percent_per_trade: min 0.5, max 10.0
            - leverage: min 5, max 25
            
            RISPONDI SOLO IN JSON:
            {{
                "market_analysis": "breve analisi del contesto",
                "risk_logic": "perché hai cambiato o mantenuto i parametri",
                "new_parameters": {{
                    "consensus_threshold": float,
                    "percent_per_trade": float,
                    "leverage": integer,
                    "max_concurrent_positions": integer
                }}
            }}
            """

            response = await self.ai_client.chat.completions.create(
                model=self.model_name,
                messages=[{"role": "user", "content": prompt}],
                response_format={ "type": "json_object" },
                temperature=0.2
            )
            
            content = json.loads(response.choices[0].message.content)
            new_params = content.get("new_parameters", {})
            
            if new_params:
                # Apply Guardrails & Sanitize
                final_params = self._apply_guardrails(new_params)
                self._update_config_file(final_params)
                logger.warning(f"🎯 [AI OPTIMIZER] Config updated by Gemini: {content['risk_logic']}")
            
            self.last_run = time.time()

        except Exception as e:
            logger.error(f"Error in parameter optimization cycle: {e}")

    def _apply_guardrails(self, params):
        """Enforces hard limits on AI-suggested parameters."""
        sanitized = {}
        # Consensus
        sanitized['consensus_threshold'] = max(1.2, min(4.0, float(params.get('consensus_threshold', 2.0))))
        # Size
        sanitized['percent_per_trade'] = max(0.5, min(10.0, float(params.get('percent_per_trade', 5.0))))
        # Leverage
        sanitized['leverage'] = max(5, min(25, int(params.get('leverage', 10))))
        # Max Positions
        sanitized['max_concurrent_positions'] = max(1, min(50, int(params.get('max_concurrent_positions', 15))))
        
        return sanitized

    def _update_config_file(self, new_params):
        """Writes the new parameters to client_config.json to trigger bot hot-reload."""
        try:
            with open(self.config_path, 'r') as f:
                config = json.load(f)
            
            # Update only the specific section
            if 'trading_parameters' not in config:
                config['trading_parameters'] = {}
                
            for k, v in new_params.items():
                config['trading_parameters'][k] = v
                
            with open(self.config_path, 'w') as f:
                json.dump(config, f, indent=4)
                
            logger.info("✅ client_config.json updated and saved.")
        except Exception as e:
            logger.error(f"Failed to update config file: {e}")
