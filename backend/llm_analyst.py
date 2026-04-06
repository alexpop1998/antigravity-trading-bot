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
        # Gemini Configuration (Dynamic via .env/config)
        self.api_key = os.getenv("LLM_API_KEY")
        self.model_name = os.getenv("LLM_MODEL_NAME", "gemini-1.5-flash")
        self.gemini_url = f"https://generativelanguage.googleapis.com/v1beta/models/{self.model_name}:generateContent?key={self.api_key}"
        self.semaphore = self.bot.ai_semaphore
        self.lessons_file = os.path.join(os.path.dirname(__file__), "ai_lessons.json")
        self.lessons_learned = self._load_lessons()
        self.cooldown_map = {} # v29 cooldown tracking
        self.cooldown_seconds = int(self.bot.config.get("strategic_params", {}).get("llm_cooldown_seconds", 60))
        self.cooldown_bypass = self.bot.config.get("strategic_params", {}).get("llm_cooldown_bypass", False)

    def _load_lessons(self):
        try:
            profile_type = getattr(self.bot, 'profile_type', 'aggressive').lower()
            if os.path.exists(self.lessons_file):
                with open(self.lessons_file, 'r') as f:
                    data = json.load(f)
                    
                    # --- MIGRATION LOGIC (v15.0) ---
                    # If legacy format, move history to 'aggressive' bucket
                    if "history" in data and "profiles" not in data:
                        logger.warning("🔄 [AI MEMORY] Migrating legacy lessons to 'AGGRESSIVE' partition.")
                        new_data = {"profiles": {"aggressive": {"history": data.get("history", []), "lessons": data.get("lessons", "")}}}
                        with open(self.lessons_file, 'w') as fw:
                            json.dump(new_data, fw)
                        return new_data["profiles"]["aggressive"]["lessons"]
                    
                    # Partitioned load
                    profile_data = data.get("profiles", {}).get(profile_type, {})
                    return profile_data.get("lessons", "Focus on high-conviction technical alignment for this profile.")
        except Exception as e:
            logger.error(f"Error loading AI lessons: {e}")
        return "Focus on high-conviction technical alignment."

    async def decide_strategy(self, symbol, side, signal_type, indicators, mtf_context="N/A", news_context="N/A"):
        """
        Performs a strategic AI review by analyzing current market data against 
        past trade results (RAG).
        """
        if not self.api_key:
            logger.warning("LLM_API_KEY non configurata. Decisione automatica: REJECT.")
            return False, 0.0, 1.0, self.bot.leverage, 1.0, 1.0, None, "API_KEY_MISSING", side

        # --- V32.4 COST AUDIT ---
        if not hasattr(self, 'session_calls'): self.session_calls = 0
        self.session_calls += 1
        start_time = time.time()

        try:
            # 🚀 FULL SPEED (v31.12)
            await asyncio.sleep(0.1) 
            
            # 1. Recupera la memoria statistica (ultime 24 ore)
            stats = self.bot.db.get_ai_performance_stats(hours=24)
            memory_context = "\nPERFORMANCE RECENTE (STATISTICHE AGGREGATE - 24H):\n"
            if stats:
                for signal_key, data in stats.items():
                    memory_context += f"- Segnale {signal_key}: Accuracy {data['accuracy']:.1f}%, Avg PnL {data['avg_pnl']:.4f}\n"
            else:
                memory_context += "Nessun trade registrato nelle ultime 24 ore.\n"

            # --- DYNAMIC PROFILE PROMPTS (v17.0) ---
            profile_type = getattr(self.bot, 'profile_type', 'aggressive').lower()
            ai_prompts = self.bot.config.get('ai_prompts', {})
            active_profile_prompt = ai_prompts.get("llm_philosophy", "Focus on high-conviction technical alignment.")

            # --- ANTI-BIAS NOTE (Dynamic) ---
            bias_note = ""
            if profile_type in ["aggressive", "extreme", "blitz"]:
                bias_note = "❗ NOTA CRITICA (ANTI-BIAS): Ignora la 'prudenza' derivante dai trade passati se vedi un nuovo momentum in atto."
            
            if profile_type == 'blitz':
                bias_note += f"\n🔥 MODALITÀ BLITZ: Sii estremamente aggressivo. Se non vedi il segnale nella direzione suggerita ({side.upper()}), ma vedi un trend chiaro nella direzione OPPOSTA, devi CAMBIARE il campo 'side' nella risposta JSON e APPROVARE il trade invece di rifiutarlo. Vogliamo trading attivo ora!"
            
            prompt = f"""
            Sei l'Analista Strategico di un Hedge Fund AI.
            {active_profile_prompt}
            Asset: {symbol} | Direzione: {side.upper()} | Tipo: {signal_type}
            Metrics: {indicators}
            Macro: {mtf_context} | News: {news_context}
            Memory: {memory_context}
            Audit: {self.lessons_learned}
            ---
            {bias_note}
            Rispondi in JSON:
            {{
                "macro_analysis": "breve sintesi macro",
                "technical_analysis": "analisi degli indicatori",
                "risk_assessment": "valutazione spread/funding/volatilità",
                "verdict": "APPROVE" o "REJECT",
                "side": "BUY" o "SELL", 
                "confidence": 0.0 a 1.0 (o 0 a 100), 
                "suggested_leverage": intero nel range del profilo,
                "position_strength": 1.0 a 50.0,  // Rappresenta la % di equity da impegnare come margine
                "sl_multiplier": 0.5 a 3.0,
                "tp_multiplier": 0.5 a 5.0,
                "tp_price": numero o null,
                "reasoning": "Sintesi finale della decisione (max 20 parole)"
            }}
            """

            # --- DYNAMIC TEMPERATURE (v17.0) ---
            active_temp = float(ai_prompts.get("llm_temperature", 0.5))
            
            # Check for API key presence
            if not self.api_key:
                logger.error("❌ LLM_API_KEY NOT FOUND. Defaulting to Neutral.")
                return False, 0.0, 1.0, self.bot.leverage, 1.0, 1.0, None, "API_KEY_MISSING", side

            async with self.semaphore:
                async with httpx.AsyncClient(timeout=30.0) as client:
                    response = await client.post(
                        self.gemini_url,
                        json={
                            "contents": [{"parts": [{"text": prompt}]}],
                            "generationConfig": {
                                "temperature": active_temp,
                                "responseMimeType": "application/json"
                            }
                        }
                    )
                    
                    if response.status_code == 429:
                        logger.warning(f"⚠️ [RATE LIMIT] Gemini API 429. Skipping AI review for {symbol}.")
                        # v37.1 SAFETY: Reject on rate-limit
                        return False, 0.0, 1.0, self.bot.leverage, 1.0, 1.0, None, "RATE_LIMIT_429", side
                    
                    response.raise_for_status()
                    data = response.json()
                    
                    # Log response for debugging non-standard outputs
                    # logger.debug(f"AI Response for {symbol}: {data}")

                    text = data['candidates'][0]['content']['parts'][0]['text']
                    response_json = json.loads(text)
            
            # Verdetto basato sulla soglia dinamica della configurazione
            verdict = response_json.get("verdict", "REJECT")
            # --- ROBUST SCALING (v33.5) ---
            confidence = response_json.get("confidence", 0.0)
            if float(confidence) > 1.0:
                confidence = float(confidence) / 100.0
            
            ai_side = response_json.get("side", side).lower()
            if ai_side not in ['buy', 'sell']:
                ai_side = side.lower()

            min_confidence = self.bot.config.get('trading_parameters', {}).get('min_ai_confidence', 0.40)
            if verdict == "APPROVE" and confidence < min_confidence:
                logger.warning(f"🛡️ [LLM GUARD] AI approved with {confidence:.2f}, below threshold {min_confidence}. REJECTING.")
                verdict = "REJECT"
                reasoning = f"Confidence threshold not met ({confidence:.2f} < {min_confidence})"
            else:
                reasoning = response_json.get("reasoning", "No reason provided.")

            leverage = int(response_json.get("suggested_leverage", self.bot.leverage))
            strength = float(response_json.get("position_strength", 1.0))
            sl_mult = float(response_json.get("sl_multiplier", 1.0))
            tp_mult = float(response_json.get("tp_multiplier", 1.0))
            tp_price = response_json.get("tp_price")

            logger.info(f"🧠 [LLM ANALYST] {symbol} {ai_side.upper()} -> {verdict} (Conf: {confidence:.2f}, Lev: {leverage}x) | Reason: {reasoning}")
            
            # v34.0 Standardized Return: (verdict, confidence, strength, leverage, sl_mult, tp_mult, tp_p, reason, side)
            return (verdict == "APPROVE"), confidence, strength, leverage, sl_mult, tp_mult, tp_price, reasoning, ai_side

        except Exception as e:
            logger.error(f"Errore durante la decisione LLM per {symbol}: {e}")
            # v37.1 Robust Return: (approved, confidence, strength, leverage, sl_mult, tp_mult, tp_p, reason, side)
            return False, 0.0, 1.0, self.bot.leverage, 1.0, 1.0, None, f"ERROR: {str(e)}", side

    async def decide_listing_strategy(self, symbol, current_price, spread_pct):
        """
        Specialized 'Flash Audit' for new coin listings (v11.5).
        Designed for maximum speed (sub-2s) to capture listing pumps safely.
        """
        if not self.ai_client:
            return True, 1.0, self.bot.leverage, 1.0, 1.0, "API_KEY_MISSING"

        try:
            prompt = f"""
            ANALISI FLASH LISTING: {symbol}
            Prezzo: {current_price}
            Spread Rilevato: {spread_pct:.2f}%
            
            REGOLE SNIPER:
            1. Se lo Spread > 2.5%, REJECT (Troppo rischioso/Slippage).
            2. Se lo Spread < 1.0%, APPROVE (Opportunità eccellente).
            3. Altrimenti valuta la volatilità iniziale.
            
            RISPONDI JSON:
            {{ "verdict": "APPROVE" o "REJECT", "confidence": 0.0-1.0, "reasoning": "max 5 parole" }}
            """
            
            response = await self.ai_client.chat.completions.create(
                model=self.model_name,
                messages=[{"role": "user", "content": prompt}],
                response_format={ "type": "json_object" },
                temperature=0.0,
                timeout=1.5 # Ultra-strict timeout
            )
            
            res = json.loads(response.choices[0].message.content)
            verdict = res.get("verdict", "REJECT")
            
            # Use fixed multipliers for listings to minimize latency
            return (verdict == "APPROVE"), 1.0, self.bot.leverage, 1.0, 1.5, None, res.get("reasoning")

        except Exception as e:
            logger.warning(f"⚠️ Flash Audit fallito per {symbol}: {e}. Eseguo REJECT per sicurezza.")
            return False, 0.0, 0, 0, 0, None, "Flash Audit Timeout/Error"

    async def evaluate_active_position(self, symbol, side, indicators, current_pnl_pct):
        """
        [V29 DYNAMIC TP2]
        Evaluates the health of an active position and suggests Scaling/Pivot.
        Uses the Gemini implementation (fixed from legacy ai_client).
        """
        if not self.api_key:
            return "HOLD", 0, "No API Key"

        try:
            profile_type = getattr(self.bot, 'profile_type', 'aggressive').lower()
            ai_prompts = self.bot.config.get('ai_prompts', {})
            philosophy = ai_prompts.get("llm_philosophy", "Monitor standard indicators.")

            prompt = f"""
            Sei l'Analista di Rischio di un Hedge Fund AI. Modalità operativa: {profile_type.upper()}.
            Filosofia: {philosophy}
            
            Valuta se mantenere, scalare o chiudere questa posizione aperta.
            
            POSIZIONE ATTUALE:
            Asset: {symbol} | Direzione: {side.upper()} | PnL Attuale: {current_pnl_pct:.2f}%
            
            CRITICAL METRICS:
            RSI: {indicators.get('rsi', 'N/D')}, MACD_Hist: {indicators.get('macd_hist', 'N/D')}, Trend: {indicators.get('trend_adx', 'N/D')}
            
            MISSIONE:
            1. RUN: Momentum forte, lascia correre verso TP2/DTS.
            2. TIGHTEN: Trend in indebolimento, stringi lo Stop Loss (Proactive SL/Trailing).
            3. SCALE_OUT: Chiudi il 50% se vedi incertezza o esaurimento. (v31.x)
            4. PIVOT: Chiudi subito tutto e gira la posizione (solo su cambio trend violento).
            5. CLOSE: Chiudi tutto ora. Momentum finito.
            
            RISPONDI JSON: {{ "technical_status": "sintesi", "action": "RUN/TIGHTEN/SCALE_OUT/PIVOT/CLOSE", "confidence": 0-1, "reasoning": "max 15 parole" }}
            """

            payload = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {
                    "temperature": 0.2,
                    "responseMimeType": "application/json"
                }
            }

            async with self.semaphore:
                async with httpx.AsyncClient(timeout=20.0) as client:
                    resp = await client.post(self.gemini_url, json=payload)
                    resp.raise_for_status()
                    raw = resp.json()
                    text = raw["candidates"][0]["content"]["parts"][0]["text"]
                    result = json.loads(text)
            
            action = result.get("action", "RUN").upper()
            confidence = float(result.get("confidence", 0.0))
            reasoning = result.get("reasoning", "Monitoraggio standard.")
            
            if confidence < 0.65: # Threshold for active monitoring
                return "RUN", confidence, "Low confidence"
                
            return action, confidence, reasoning

        except Exception as e:
            logger.error(f"Errore durante monitoraggio posizione {symbol}: {e}")
            return "HOLD", 0.0, f"ERROR: {str(e)}"

    def is_in_cooldown(self, symbol: str) -> bool:
        """
        [V11.8 COOLDOWN ENGINE]
        Checks if a symbol is in AI cooldown.
        Bypassable via config for Blitz/Extreme.
        """
        if self.cooldown_bypass:
            return False
            
        last_time = self.cooldown_map.get(symbol, 0)
        elapsed = time.time() - last_time
        return elapsed < self.cooldown_seconds

    def update_cooldown(self, symbol: str):
        """Sets the cooldown timestamp for a symbol."""
        self.cooldown_map[symbol] = time.time()

    async def perform_self_audit(self, trades_history: List[Dict[str, Any]]):
        """
        [V29 DAILY AUDIT]
        Analizza i trade passati per estrarre lezioni e migliorare la strategia.
        """
        if not self.api_key or not trades_history:
            return

        try:
            profile_type = getattr(self.bot, 'profile_type', 'aggressive').lower()
            history_str = json.dumps([{
                'symbol': t['symbol'], 'side': t['side'], 'pnl': t.get('pnl', 0), 
                'outcome': 'WIN' if float(t.get('pnl', 0)) > 0 else 'LOSS',
                'reason': t.get('reason', 'N/A')
            } for t in trades_history[-20:]], indent=2)

            prompt = f"""
            Sei il Chief Risk Officer di un Hedge Fund. Profilo Attivo: {profile_type.upper()}.
            Analizza questi ultimi 20 trade e identifica 3 REGOLE d'oro per evitare perdite future.
            
            STORICO TRADE:
            {history_str}
            
            RISPONDI JSON: {{ "lessons": "Una stringa concisa con le 3 regole identificate." }}
            """

            payload = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": { "temperature": 0.3, "responseMimeType": "application/json" }
            }

            async with self.semaphore:
                async with httpx.AsyncClient(timeout=30.0) as client:
                    resp = await client.post(self.gemini_url, json=payload)
                    resp.raise_for_status()
                    res_json = resp.json()
                    text = res_json['candidates'][0]['content']['parts'][0]['text']
                    result = json.loads(text)
                    new_lessons = result.get("lessons", self.lessons_learned)
                
                # --- PROFILE-PARTITIONED SAVE ---
                all_data = {"profiles": {}}
                if os.path.exists(self.lessons_file):
                    with open(self.lessons_file, 'r') as f: all_data = json.load(f)
                
                p_data = all_data.get("profiles", {}).get(profile_type, {"history": [], "lessons": ""})
                p_data["lessons"] = new_lessons
                all_data.setdefault("profiles", {})[profile_type] = p_data
                self.lessons_learned = new_lessons

                with open(self.lessons_file, 'w') as f: json.dump(all_data, f)
                logger.warning(f"🧠 [DAILY AUDIT - {profile_type.upper()}] New Lessons: {self.lessons_learned}")

        except Exception as e:
            logger.error(f"Errore durante self-audit AI: {e}")

    async def perform_post_mortem(self, symbol: str, trade: Dict, pnl: float):
        """
        [V14.6 POST-MORTEM]
        Analizza l'esito di un trade appena chiuso.
        """
        if not self.api_key: return
        try:
            if abs(pnl) < 0.003: return # Skip noise
            
            outcome = "WIN" if pnl > 0.005 else "LOSS"
            profile_type = getattr(self.bot, 'profile_type', 'aggressive').lower()
            
            prompt = f"""
            Analizza questo trade appena chiuso (Profilo {profile_type.upper()}).
            Asset: {symbol} | Esito: {outcome} | PnL: {pnl:.2%}
            Reasoning: {trade.get('reasoning', 'N/D')}
            
            Definisci una 'Golden Rule' (max 12 parole) per il futuro.
            RISPONDI JSON: {{ "golden_rule": "regola" }}
            """

            payload = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": { "temperature": 0.4, "responseMimeType": "application/json" }
            }

            async with self.semaphore:
                async with httpx.AsyncClient(timeout=20.0) as client:
                    resp = await client.post(self.gemini_url, json=payload)
                    resp.raise_for_status()
                    res_json = resp.json()
                    text = res_json['candidates'][0]['content']['parts'][0]['text']
                    result = json.loads(text)
            
            new_rule = result.get("golden_rule", "")
            if new_rule:
                logger.warning(f"🎓 [AI MENTOR] New trade rule for {symbol}: {new_rule}")
                # Log to lessons history (simplified for now)
                self.lessons_learned = f"{new_rule} | {self.lessons_learned[:200]}"
                # Save would go here...
        except Exception as e:
            logger.error(f"Error in post-mortem: {e}")

    async def refine_market_selection(self, candidates: List[Dict[str, Any]], limit: int = 60) -> List[str]:
        """
        [V33.0 DYNAMIC SCANNER]
        Asks Gemini to pick the best trading candidates from a list of high-volume assets.
        Analyzes 150 candidates to select the top 60.
        """
        if not self.api_key:
            return [c['symbol'] for c in candidates[:limit]]

        try:
            # Format candidate data for Gemini (Top 150)
            data_string = "\n".join([f"{c['symbol']}: Vol: {c['volume']/1e6:.1f}M, Change: {c['change']:.1f}%" for c in candidates[:150]])
            
            prompt = f"""
            Sei lo Strategista di un Hedge Fund Quantitativo. Seleziona i {limit} asset più promettenti tra questi 150 candidati ad alto volume.
            Prediligi asset con:
            1. Alta volatilità recente (percentuali di cambiamento alte).
            2. Volume consistente (>2M USDT).
            3. Evita stablecoin se presenti.
            
            CANDIDATI:
            {data_string}
            
            RISPONDI ESATTAMENTE CON UN OGGETTO JSON: {{"symbols": ["BTC/USDT:USDT", "SOL/USDT:USDT", ...]}}
            Massimo {limit} simboli. Sii preciso nei nomi dei simboli.
            """

            payload = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {
                    "temperature": 0.2,
                    "responseMimeType": "application/json"
                }
            }

            async with self.semaphore:
                async with httpx.AsyncClient(timeout=30.0) as client:
                    response = await client.post(self.gemini_url, json=payload)
                    response.raise_for_status()
                    data = response.json()
                    
                    text = data['candidates'][0]['content']['parts'][0]['text']
                    
                    # --- ROBUST JSON CLEANING (v33.2) ---
                    if "```json" in text:
                        text = text.split("```json")[1].split("```")[0]
                    elif "```" in text:
                        text = text.split("```")[1].split("```")[0]
                    text = text.strip()
                    
                    result = json.loads(text)
                    final_list = result.get("symbols", [])
                    
                    if not final_list:
                        logger.warning("⚠️ [AI SCANNER] Gemini returned empty list or invalid JSON. Falling back to volume-based.")
                        final_list = [c['symbol'] for c in candidates[:limit]]
                    
                    logger.info(f"🧠 [AI SCANNER] Gemini refined selection. Selected {len(final_list)} symbols from {len(candidates)} candidates.")
                    return final_list

        except Exception as e:
            logger.error(f"❌ Errore critico raffinamento mercati AI: {str(e)}")
            import traceback
            logger.debug(traceback.format_exc())
            return [c['symbol'] for c in candidates[:limit]]

