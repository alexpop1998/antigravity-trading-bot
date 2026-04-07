import os
import logging
import asyncio
import json
import time
from typing import List, Dict, Any
import httpx

logger = logging.getLogger("LLMAnalyst")
logger.setLevel(logging.INFO)

class LLMAnalyst:
    def __init__(self, bot_instance):
        self.bot = bot_instance
        # --- v45.0.0 DEEPSEEK UPGRADE ---
        self.api_key = os.getenv("LLM_API_KEY")
        self.base_url = os.getenv("LLM_BASE_URL", "https://api.deepseek.com")
        self.model_name = os.getenv("LLM_MODEL_NAME", "deepseek-chat")
        self.api_endpoint = f"{self.base_url}/v1/chat/completions"
        self.semaphore = self.bot.ai_semaphore
        self.lessons_file = os.path.join(os.path.dirname(__file__), "ai_lessons.json")
        self.lessons_learned = self._load_lessons()
        self.cooldown_map = {} 
        self.cooldown_seconds = int(self.bot.config.get("strategic_params", {}).get("llm_cooldown_seconds", 60))
        self.cooldown_bypass = self.bot.config.get("strategic_params", {}).get("llm_cooldown_bypass", False)

    def _load_lessons(self):
        try:
            profile_type = getattr(self.bot, 'profile_type', 'aggressive').lower()
            if os.path.exists(self.lessons_file):
                with open(self.lessons_file, 'r') as f:
                    data = json.load(f)
                    if "history" in data and "profiles" not in data:
                        new_data = {"profiles": {"aggressive": {"history": data.get("history", []), "lessons": data.get("lessons", "")}}}
                        with open(self.lessons_file, 'w') as fw:
                            json.dump(new_data, fw)
                        return new_data["profiles"]["aggressive"]["lessons"]
                    profile_data = data.get("profiles", {}).get(profile_type, {})
                    return profile_data.get("lessons", "Focus on high-conviction technical alignment for this profile.")
        except Exception as e:
            logger.error(f"Error loading AI lessons: {e}")
        return "Focus on high-conviction technical alignment."

    async def _ask_deepseek(self, system_prompt: str, user_prompt: str, temperature: float = 0.5) -> Dict:
        """Centralized DeepSeek API Caller (v45.0.0 OpenAI-Compatible)"""
        if not self.api_key:
            raise ValueError("LLM_API_KEY_MISSING")

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        payload = {
            "model": self.model_name,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            "temperature": temperature,
            "response_format": {"type": "json_object"}
        }

        async with self.semaphore:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(self.api_endpoint, headers=headers, json=payload)
                if response.status_code == 429:
                    logger.warning("⚠️ [RATE LIMIT] DeepSeek 429. Cooling down.")
                    raise Exception("RATE_LIMIT_429")
                response.raise_for_status()
                data = response.json()
                content = data['choices'][0]['message']['content']
                return json.loads(content)

    async def decide_strategy(self, symbol, side, signal_type, indicators, mtf_context="N/A", news_context="N/A"):
        """Performs a strategic AI review via DeepSeek V3."""
        try:
            # Refresh AI config
            self.api_key = os.getenv("LLM_API_KEY")
            
            stats = self.bot.db.get_ai_performance_stats(hours=24)
            memory_context = "\nPERFORMANCE RECENTE (24H):\n"
            if stats:
                for s_key, data in stats.items():
                    memory_context += f"- {s_key}: Accuracy {data['accuracy']:.1f}%, Avg PnL {data['avg_pnl']:.4f}\n"
            else:
                memory_context += "Nessun trade registrato.\n"

            profile_type = getattr(self.bot, 'profile_type', 'aggressive').lower()
            ai_prompts = self.bot.config.get('ai_prompts', {})
            philosophy = ai_prompts.get("llm_philosophy", "Focus alignment.")
            
            bias_note = ""
            if profile_type == 'blitz':
                bias_note = "\n🔥 MODALITÀ BLITZ: Sii estremamente aggressivo. Dai priorità alla VELOCITÀ di entrata. Se vedi momentum, APPROVA ora."
            
            system_prompt = "Sei un Analista Strategico di un Hedge Fund AI specializzato in HFT e Crypto Blitz trading."
            user_prompt = f"""
            {philosophy}
            Asset: {symbol} | Direzione: {side.upper()} | Tipo: {signal_type}
            Metrics: {indicators}
            Macro: {mtf_context} | News: {news_context}
            Memory: {memory_context}
            Audit: {self.lessons_learned}
            ---
            {bias_note}
            Rispondi in JSON:
            {{
                "macro_analysis": "breve sintesi",
                "technical_analysis": "analisi",
                "risk_assessment": "spread/volatilità",
                "verdict": "APPROVE" o "REJECT",
                "side": "BUY" o "SELL", 
                "confidence": 0.0-1.0, 
                "suggested_leverage": intero,
                "position_strength": 1.0-50.0,
                "sl_multiplier": 0.5-3.0,
                "tp_multiplier": 0.5-5.0,
                "tp_price": numero o null,
                "reasoning": "max 20 parole"
            }}
            """

            active_temp = float(ai_prompts.get("llm_temperature", 0.5))
            res = await self._ask_deepseek(system_prompt, user_prompt, active_temp)
            
            verdict = res.get("verdict", "REJECT")
            confidence = float(res.get("confidence", 0.0))
            if confidence > 1.0: confidence /= 100.0
            
            ai_side = res.get("side", side).lower()
            if ai_side not in ['buy', 'sell']: ai_side = side.lower()

            min_conf = self.bot.config.get('trading_parameters', {}).get('min_ai_confidence', 0.40)
            if verdict == "APPROVE" and confidence < min_conf:
                logger.warning(f"🛡️ [GUARD] DeepSeek approved with {confidence:.2f}, below {min_conf}. REJECTED.")
                verdict = "REJECT"
                reasoning = f"Low confidence ({confidence:.2f})"
            else:
                reasoning = res.get("reasoning", "N/A")

            leverage = int(res.get("suggested_leverage", self.bot.leverage))
            strength = float(res.get("position_strength", 1.0))
            sl_mult = float(res.get("sl_multiplier", 1.0))
            tp_mult = float(res.get("tp_multiplier", 1.0))
            tp_price = res.get("tp_price")

            logger.info(f"🧠 [DEEPSEEK] {symbol} {ai_side.upper()} -> {verdict} (Conf: {confidence:.2f}) | {reasoning}")
            return (verdict == "APPROVE"), confidence, strength, leverage, sl_mult, tp_mult, tp_price, reasoning, ai_side

        except Exception as e:
            logger.error(f"Errore DeepSeek per {symbol}: {e}")
            return False, 0.0, 1.0, self.bot.leverage, 1.0, 1.0, None, str(e), side

    async def evaluate_active_position(self, symbol, side, indicators, current_pnl_pct):
        """[V29 DYNAMIC TP2] via DeepSeek"""
        if not self.api_key: return "HOLD", 0, "No Key"
        try:
            prompt = f"Asset: {symbol} | PnL: {current_pnl_pct:.2f}% | Indicators: {indicators}"
            sys_p = "Analista di Rischio. Rispondi JSON: { 'action': 'RUN/TIGHTEN/SCALE_OUT/PIVOT/CLOSE', 'confidence': 0-1, 'reasoning': 'max 15 parole' }"
            res = await self._ask_deepseek(sys_p, prompt, 0.2)
            return res.get("action", "RUN").upper(), float(res.get("confidence", 0)), res.get("reasoning", "")
        except Exception as e:
            logger.error(f"Error monitoring {symbol}: {e}")
            return "HOLD", 0.0, str(e)

    async def perform_self_audit(self, trades_history: List[Dict[str, Any]]):
        """[V29 DAILY AUDIT] via DeepSeek"""
        if not self.api_key or not trades_history: return
        try:
            profile_type = getattr(self.bot, 'profile_type', 'aggressive').lower()
            history = json.dumps([{'s': t['symbol'], 'pnl': t.get('pnl', 0), 'r': t.get('reason', 'N/A')} for t in trades_history[-20:]])
            sys_p = f"CRO Manager. Profilo: {profile_type}. Analizza trade e dai 3 regole d'oro in JSON: {{ 'lessons': '...' }}"
            res = await self._ask_deepseek(sys_p, history, 0.3)
            new_lessons = res.get("lessons", self.lessons_learned)
            
            # Save logic
            all_data = {"profiles": {}}
            if os.path.exists(self.lessons_file):
                with open(self.lessons_file, 'r') as f: all_data = json.load(f)
            p_data = all_data.get("profiles", {}).get(profile_type, {"history": [], "lessons": ""})
            p_data["lessons"] = new_lessons
            all_data.setdefault("profiles", {})[profile_type] = p_data
            self.lessons_learned = new_lessons
            with open(self.lessons_file, 'w') as f: json.dump(all_data, f)
            logger.warning(f"🧠 [AUDIT - {profile_type.upper()}] New Lessons: {self.lessons_learned}")
        except Exception as e: logger.error(f"Audit error: {e}")

    async def refine_market_selection(self, candidates: List[Dict[str, Any]], limit: int = 60) -> List[str]:
        """[V33.0 DYNAMIC SCANNER] via DeepSeek"""
        if not self.api_key: return [c['symbol'] for c in candidates[:limit]]
        try:
            data = "\n".join([f"{c['symbol']}: Vol: {c['volume']/1e6:.1f}M, Chg: {c['change']:.1f}%" for c in candidates[:150]])
            sys_p = f"Quant Strategist. Seleziona {limit} asset promettenti in JSON: {{'symbols': ['...', '...']}}"
            res = await self._ask_deepseek(sys_p, data, 0.2)
            final_list = res.get("symbols", [])
            return final_list if final_list else [c['symbol'] for c in candidates[:limit]]
        except Exception as e:
            logger.error(f"Scan refinement error: {e}")
    def is_in_cooldown(self, symbol: str) -> bool:
        """[V11.8 COOLDOWN ENGINE] Checks if a symbol is in AI cooldown."""
        if self.cooldown_bypass: return False
        last_time = self.cooldown_map.get(symbol, 0)
        return (time.time() - last_time) < self.cooldown_seconds

    def update_cooldown(self, symbol: str):
        """Sets the cooldown timestamp for a symbol."""
        self.cooldown_map[symbol] = time.time()

    async def decide_listing_strategy(self, symbol, current_price, spread_pct):
        """Specialized 'Flash Audit' for new coin listings (v11.5)."""
        if not self.api_key: return True, 1.0, self.bot.leverage, 1.0, 1.5, None, "API_KEY_MISSING"
        try:
            sys_p = "Listing Sniper. Rispondi JSON: { 'verdict': 'APPROVE/REJECT', 'confidence': 0-1, 'reasoning': 'max 5 parole' }"
            user_p = f"Listing: {symbol} | Price: {current_price} | Spread: {spread_pct:.2f}%"
            res = await self._ask_deepseek(sys_p, user_p, 0.0)
            verdict = res.get("verdict", "REJECT")
            return (verdict == "APPROVE"), 1.0, self.bot.leverage, 1.0, 1.5, None, res.get("reasoning")
        except Exception as e:
            logger.warning(f"Flash Audit error for {symbol}: {e}")
            return False, 0.0, 0, 0, 0, None, "Flash Error"
