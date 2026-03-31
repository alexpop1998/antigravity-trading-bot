import os
import logging
import asyncio
import json
import time
from typing import List, Dict, Any
from openai import AsyncOpenAI

logger = logging.getLogger("LLMAnalyst")
logger.setLevel(logging.INFO)

class LLMAnalyst:
    def __init__(self, bot_instance):
        self.bot = bot_instance
        api_key = os.getenv("LLM_API_KEY")
        base_url = os.getenv("LLM_BASE_URL", "https://generativelanguage.googleapis.com/v1beta/openai/")
        self.ai_client = AsyncOpenAI(
            api_key=api_key,
            base_url=base_url
        ) if api_key else None
        
        self.model_name = os.getenv("LLM_MODEL_NAME", "gemini-2.5-flash-lite")
        self.semaphore = asyncio.Semaphore(2) # Limit concurrent decisions to avoid rate limits
        self.lessons_file = os.path.join(os.path.dirname(__file__), "ai_lessons.json")
        self.lessons_learned = self._load_lessons()

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
        if not self.ai_client:
            logger.warning("LLM_API_KEY non configurata. Decisione automatica: APPROVE.")
            return True, 1.0, self.bot.leverage, 1.0, 1.0, "API_KEY_MISSING"

        try:
            # 1. Recupera la memoria statistica (ultime 24 ore)
            stats = self.bot.db.get_ai_performance_stats(hours=24)
            memory_context = "\nPERFORMANCE RECENTE (STATISTICHE AGGREGATE - 24H):\n"
            if stats:
                for signal, data in stats.items():
                    memory_context += f"- Segnale {signal}: Accuracy {data['accuracy']:.1f}%, Avg PnL {data['avg_pnl']:.4f} (Sample size: {data['sample_size']})\n"
            else:
                memory_context += "Nessun trade registrato nelle ultime 24 ore.\n"

            # --- DYNAMIC PROFILE PROMPTS (v14.5) ---
            profile_type = getattr(self.bot, 'profile_type', 'aggressive').lower()
            
            prompts = {
                "institutional": """--- MISSIONE INSTITUTIONAL : LA FORTEZZA ---
                    Agisci come un Risk Manager di un Hedge Fund Istituzionale. Il tuo obiettivo primario è la conservazione del capitale. Devi essere estremamente selettivo. 
                    REGOLE:
                    1. Trend (EMA200): Il prezzo deve essere chiaramente sopra (Long) o sotto (Short) con una pendenza consolidata. Niente ranging.
                    2. Volatilità (ATR): Rifiuta segnali con ATR in forte espansione (>15% nelle ultime 4h). La stabilità è prioritaria.
                    3. Consenso: Il punteggio di consenso è alto (>6/10). Assicurati che ci sia convergenza tra Macro e Tecnico.
                    4. Decisione: Approva solo se il setup è 'perfetto'. Se hai anche il minimo dubbio, RIFIUTA. 
                    5. Leva: Tendi sempre verso il minimo consentito (1x-2x) a meno che il trend non sia eccezionalmente forte.""",
                
                "conservative": """--- MISSIONE CONSERVATIVE : CRESCITA STABILE ---
                    Sei un Trader Professionista con un focus sulla gestione del rischio. Cerchi opportunità con un buon rapporto rischio/rendimento.
                    REGOLE:
                    1. Momentum: Il MACD è incrociato e istogramma in espansione? Il volume supporta il movimento?
                    2. Contesto: Il prezzo rispetta i livelli chiave (supporto/resistenza)?
                    3. AI Predictor: Qual è la probabilità di direzione del ML? Se è >60%, dai più peso.
                    4. Leva: Scegli la leva in base alla forza del trend. Usa leva più alta (6x-8x) solo se il momentum è forte e i volumi sono sopra la media del 20%.""",
                
                "aggressive": """--- MISSIONE AGGRESSIVE : PREDATORE BLITZ ---
                    Sei un Predatore di Momentum. Il tuo unico obiettivo è cavalcare le onde di volatilità per generare profitti rapidi. 
                    Ignora metriche conservative come RSI > 70 – per te, RSI alto è sinonimo di FORZA e momentum consolidato.
                    REGOLE:
                    1. Volume: Il volume delle ultime 3 candele è superiore alla media del 20% o chiaramente in aumento? Non esigere esplosioni 2x se il trend è chiaro.
                    2. Velocità Prezzo: Il prezzo si sta muovendo con forza in una direzione? 
                    3. AI Predictor: Se il ML Predictor dà alta confidenza, è un GREEN LIGHT.
                    4. Esecuzione: Prioritizza la cattura dell'opportunità rispetto alla precisione millimetrica. Se il trend è forte, APPROVA. Usa leva e strength audaci per capitalizzare.""",
                
                "extreme": """--- MISSIONE EXTREME : HIGH STAKES ---
                    Sei un Trader d'Assalto con massima tolleranza al rischio. Hai un conto piccolo e cerchi il salto di capitale immediato.
                    Il tuo obiettivo è la frequenza di esecuzione alta: non scartare trade per dubbi minori.
                    REGOLE:
                    1. Catalizzatore: C'è volatilità, una rottura di livello o volume interessante?
                    2. Struttura: Ogni breakout è una potenziale entrata. 
                    3. Decisione: Se vedi un movimento in atto, APPROVA immediatamente senza esitazione. Meglio rischiare un'entrata rapida che perdere il treno.
                    4. Leva & Forza: Massimizza i parametri. Ogni trade deve essere un colpo secco alla giugulare del mercato."""
            }
            
            active_profile_prompt = prompts.get(profile_type, prompts["aggressive"])

            prompt = f"""
            Sei l'Analista Strategico di un Hedge Fund AI. Devi decidere se approvare un'operazione di trading ad alta frequenza.
            
            {active_profile_prompt}

            OPERAZIONE PROPOSTA:
            Asset: {symbol} | Direzione: {side.upper()} | Tipo Segnale: {signal_type}
            
            CRITICAL METRICS:
            RSI: {indicators.get('rsi', 'N/D')}, MACD_Hist: {indicators.get('macd_hist', 'N/D')}, 
            Vol_Change: {indicators.get('volume_change', 'N/D')}, ATR_Volatility: {indicators.get('atr', 'N/D')},
            Price_vs_EMA200: {indicators.get('price_vs_ema200', 'N/D')}
            
            --- MACRO & MEMORY ---
            Trend Context: {mtf_context}
            News: {news_context}
            Past Performance: {memory_context}
            Audit Lessons: {self.lessons_learned}
            
            ❗ NOTA CRITICA (ANTI-BIAS): Se il profilo è AGGRESSIVE o EXTREME, ignora la 'prudenza' derivante dai trade passati se vedi un nuovo momentum in atto. Non farti frenare dalle perdite precedenti; il tuo compito è catturare il prossimo movimento.
            
            RISPONDI ESATTAMENTE IN QUESTO FORMATO JSON:
            {{
                "macro_analysis": "breve sintesi macro",
                "technical_analysis": "analisi degli indicatori",
                "risk_assessment": "valutazione spread/funding/volatilità",
                "verdict": "APPROVE" o "REJECT",
                "confidence": 0.0 a 1.0, 
                "suggested_leverage": intero nel range del profilo,
                "position_strength": 1.0 a 50.0,  // Rappresenta la % di equity da impegnare come margine
                "sl_multiplier": 0.5 a 3.0,
                "tp_multiplier": 0.5 a 5.0,
                "tp_price": numero o null,
                "reasoning": "Sintesi finale della decisione (max 20 parole)"
            }}
            """

            async with self.semaphore:
                response = await self.ai_client.chat.completions.create(
                    model=self.model_name,
                    messages=[{"role": "user", "content": prompt}],
                    response_format={ "type": "json_object" },
                    temperature=0.2
                )
                
                response_json = json.loads(response.choices[0].message.content)
            
            # Verdetto basato sulla soglia dinamica della configurazione
            verdict = response_json.get("verdict", "REJECT")
            confidence = response_json.get("confidence", 0.0)
            
            if verdict == "APPROVE" and confidence < self.bot.gemini_min_confidence:
                logger.warning(f"🛡️ [LLM GUARD] AI approved with {confidence}, but config requires {self.bot.gemini_min_confidence}. REJECTING.")
                verdict = "REJECT"
                reasoning = f"Confidence threshold not met ({confidence} < {self.bot.gemini_min_confidence})"
            else:
                reasoning = response_json.get("reasoning", "No reason provided.")

            leverage = int(response_json.get("suggested_leverage", self.bot.leverage))
            strength = float(response_json.get("position_strength", 1.0))
            sl_mult = float(response_json.get("sl_multiplier", 1.0))
            tp_mult = float(response_json.get("tp_multiplier", 1.0))
            tp_price = response_json.get("tp_price")

            logger.info(f"🧠 [LLM ANALYST] {symbol} {side.upper()} -> {verdict} (Lev: {leverage}x, Strength: {strength:.2f}, SL_m: {sl_mult:.2f}, TP_m: {tp_mult:.2f}, TP_p: {tp_price}) | Reason: {reasoning}")
            
            return (verdict == "APPROVE"), strength, leverage, sl_mult, tp_mult, tp_price, reasoning

        except Exception as e:
            logger.error(f"Errore durante la decisione LLM per {symbol}: {e}")
            return True, 1.0, self.bot.leverage, 1.0, 1.0, None, f"ERROR: {str(e)}"

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
        Evaluates the health of an active position and suggests Scaling/Pivot.
        """
        if not self.ai_client:
            return "HOLD", 0, "No AI Client"

        try:
            # --- DYNAMIC MONITORING PHILOSOPHY (v14.6) ---
            profile_type = getattr(self.bot, 'profile_type', 'aggressive').lower()
            philosophies = {
                "institutional": "Sei ultra-prudente. Se vedi debolezza o perdita di trend EMA200, chiudi subito. Non inseguire profitti incerti.",
                "conservative": "Punta alla stabilità. Se siamo in profitto (TP1 preso), cerca motivi per chiudere se la volatilità sale.",
                "aggressive": "Sii un predatore. Mantieni finché il momentum è a favore. Accetta ritracciamenti tecnici.",
                "extreme": "All-in o niente. Mantieni per il colpo grosso (TP2) a meno di inversione strutturale violenta."
            }
            philosophy = philosophies.get(profile_type, philosophies["aggressive"])

            prompt = f"""
            Sei l'Analista di Rischio di un Hedge Fund AI. Modalità operativa: {profile_type.upper()}.
            Filosofia: {philosophy}
            
            Valuta se mantenere, scalare o chiudere questa posizione aperta.
            
            POSIZIONE ATTUALE:
            Asset: {symbol} | Direzione: {side.upper()} | PnL Attuale: {current_pnl_pct:.2f}%
            
            CRITICAL METRICS:
            RSI: {indicators.get('rsi', 'N/D')}, MACD_Hist: {indicators.get('macd_hist', 'N/D')}, Trend: {indicators.get('trend_adx', 'N/D')}
            
            MISSIONE:
            1. HOLD: Mantieni tutto invariato. Punta al TP2.
            2. SCALE_OUT: Chiudi il 50% (TP1) se vedi incertezza o esaurimento.
            3. PIVOT: Chiudi subito tutto e gira la posizione (solo su cambio trend violento).
            4. CLOSE: Chiudi tutto ora.
            
            REGOLE SPECIFICHE {profile_type.upper()}:
            - {philosophy}
            - Abbiamo uno Stop Loss e un Trailing che ci proteggono, ma il tuo intervento 'manuale' è richiesto se i segnali tecnici deteriorano.
            
            RISPONDI JSON: {{ "technical_status": "sintesi", "action": "HOLD/SCALE_OUT/PIVOT/CLOSE", "confidence": 0-1, "reasoning": "max 15 parole" }}
            """

            async with self.semaphore:
                response = await self.ai_client.chat.completions.create(
                    model=self.model_name,
                    messages=[{"role": "user", "content": prompt}],
                    response_format={ "type": "json_object" },
                    temperature=0.1
                )
                
                result = json.loads(response.choices[0].message.content)
                action = result.get("action", "HOLD").upper()
                confidence = float(result.get("confidence", 0.0))
                reasoning = result.get("reasoning", "Monitoraggio standard.")
                
                if confidence < 0.70: # Ignora azioni a bassa confidenza
                    return "HOLD", confidence, "Low confidence"
                    
                return action, confidence, reasoning

        except Exception as e:
            logger.error(f"Errore durante monitoraggio posizione {symbol}: {e}")
            return "HOLD", 0.0, f"ERROR: {str(e)}"

    async def refine_market_selection(self, candidates: List[Dict[str, Any]], limit: int = 50) -> List[str]:
        """
        Asks Gemini to pick the best trading candidates from a list of high-volume assets.
        """
        if not self.ai_client:
            return [c['symbol'] for c in candidates[:limit]]

        try:
            # Format candidate data for Gemini (Top 100)
            data_string = "\n".join([f"{c['symbol']}: Vol: {c['volume']/1e6:.1f}M, Change: {c['change']:.1f}%" for c in candidates[:100]])
            
            prompt = f"""
            Sei lo Strategista di un Hedge Fund Quantitativo. Seleziona i 50 asset più promettenti tra questi 100 candidati ad alto volume.
            Prediligi asset con alta volatilità (percentuali alte) ma volume consistente (>30M).
            
            CANDIDATI:
            {data_string}
            
            RISPONDI ESATTAMENTE CON UNA LISTA DI STRINGHE JSON (SOLO I SIMBOLI), esempio: ["BTC/USDT:USDT", "SOL/USDT:USDT", ...]
            Massimo {limit} simboli.
            """

            async with self.semaphore:
                response = await self.ai_client.chat.completions.create(
                    model=self.model_name,
                    messages=[{"role": "user", "content": prompt}],
                    response_format={ "type": "json_object" if "flash" in self.model_name else "text" }, # Some models might not support json_object for arrays
                    temperature=0.2
                )
                
                content = response.choices[0].message.content
                # If the model doesn't support json_object for bare arrays, it might wrap it or we just parse it
                try:
                    result = json.loads(content)
                    if isinstance(result, dict) and "symbols" in result:
                        final_list = result["symbols"]
                    elif isinstance(result, list):
                        final_list = result
                    else:
                        final_list = [c['symbol'] for c in candidates[:limit]]
                except:
                    # Fallback to volume-based if JSON fails
                    final_list = [c['symbol'] for c in candidates[:limit]]
                
                logger.info(f"🧠 [AI SCANNER] Gemini refined selection. Selected {len(final_list)} symbols.")
                return final_list

        except Exception as e:
            logger.error(f"Errore durante raffinamento mercati AI: {e}")
            return [c['symbol'] for c in candidates[:limit]]

    async def perform_self_audit(self, trades_history: List[Dict[str, Any]]):
        """
        Analizza i trade passati per estrarre lezioni e migliorare la strategia.
        """
        if not self.ai_client or not trades_history:
            return

        try:
            profile_type = getattr(self.bot, 'profile_type', 'aggressive').lower()
            # Filter history to roughly align with current profile if possible, 
            # for now we take the last 20 but evaluate them via the profile philosophy.
            history_str = json.dumps([{
                'symbol': t['symbol'], 'side': t['side'], 'pnl': t.get('pnl', 0), 
                'outcome': 'WIN' if t.get('pnl', 0) > 0 else 'LOSS',
                'reason': t.get('reason', 'N/A')
            } for t in trades_history[-20:]], indent=2)

            prompt = f"""
            Sei il Chief Risk Officer di un Hedge Fund. Profilo Attivo: {profile_type.upper()}.
            Analizza questi ultimi 20 trade e identifica 3 REGOLE d'oro coerenti con la strategia {profile_type.upper()}.
            
            STORICO TRADE:
            {history_str}
            
            RISPONDI ESATTAMENTE IN JSON:
            {{
                "lessons": "Una stringa concisa con le 3 regole identificate."
            }}
            """

            async with self.semaphore:
                response = await self.ai_client.chat.completions.create(
                    model=self.model_name,
                    messages=[{"role": "user", "content": prompt}],
                    response_format={ "type": "json_object" },
                    temperature=0.3
                )
                
                content = response.choices[0].message.content
                result = json.loads(content)
                new_lessons = result.get("lessons", self.lessons_learned)
                
                # --- PROFILE-PARTITIONED SAVE ---
                all_data = {"profiles": {}}
                if os.path.exists(self.lessons_file):
                    try:
                        with open(self.lessons_file, 'r') as f:
                            all_data = json.load(f)
                    except: pass
                
                p_data = all_data.get("profiles", {}).get(profile_type, {"history": [], "lessons": ""})
                p_data["lessons"] = new_lessons
                all_data.setdefault("profiles", {})[profile_type] = p_data
                self.lessons_learned = new_lessons

                with open(self.lessons_file, 'w') as f:
                    json.dump(all_data, f)
                
                logger.warning(f"🧠 [AI AUDIT - {profile_type.upper()}] Nuove lezioni apprese: {self.lessons_learned}")

        except Exception as e:
            logger.error(f"Errore durante self-audit AI: {e}")

    async def perform_post_mortem(self, symbol: str, trade: Dict, close_price: float, pnl: float):
        """Analizza l'esito di un trade per estrarre lezioni strategiche e gestisce lo sliding window."""
        if not self.ai_client:
            return

        try:
            # Skip minor noise trades to save tokens (Threshold: |0.3%|)
            if abs(pnl) < 0.003:
                return
                
            outcome = "WIN" if pnl > 0.005 else ("LOSS" if pnl < -0.005 else "NEUTRAL/PARTIAL")
            
            # --- Capture indicators context from bot ---
            indicators = self.bot.latest_data.get(symbol, {})
            
            # --- PROFILE-AWARE MENTOR (v14.6) ---
            profile_type = getattr(self.bot, 'profile_type', 'aggressive').lower()
            
            prompt = f"""
            Sei un Senior Trading Mentor specializzato in strategie {profile_type.upper()}. 
            Analizza questo trade appena chiuso e ricava una lezione.
            
            DATI TRADE:
            - Simbolo: {symbol} | Esito: {outcome} | PnL: {pnl:.2%}
            - Reasoning Entrata: {trade.get('reasoning', 'N/D')}
            
            MISSIONE:
            1. Valuta il trade secondo la filosofia {profile_type.upper()}.
            2. Identifica se l'errore è stato strategico (rischio troppo alto/basso per il profilo) o tecnico.
            3. Definisci una 'Golden Rule' (max 12 parole).
            
            RISPONDI SOLO IN JSON: {{ "analysis": "analisi", "lesson": "lezione", "golden_rule": "regola" }}
            """

            async with self.semaphore:
                response = await self.ai_client.chat.completions.create(
                    model=self.model_name,
                    messages=[{"role": "user", "content": prompt}],
                    response_format={ "type": "json_object" },
                    temperature=0.3
                )
            
            content = json.loads(response.choices[0].message.content)
            new_rule = content.get("golden_rule", "Be careful.")
            
            # --- PROFILE-PARTITIONED SLIDING WINDOW (v15.0) ---
            profile_type = getattr(self.bot, 'profile_type', 'aggressive').lower()
            all_data = {"profiles": {}}
            if os.path.exists(self.lessons_file):
                try:
                    with open(self.lessons_file, 'r') as f:
                        all_data = json.load(f)
                        if "profiles" not in all_data:
                            # Migrate legacy or handle empty
                            all_data = {"profiles": {"aggressive": {"history": all_data.get("history", []), "lessons": all_data.get("lessons", "")}}}
                except: pass
            
            p_data = all_data.get("profiles", {}).get(profile_type, {"history": [], "lessons": ""})
            lessons_db = p_data.get("history", [])

            # Aggiungiamo la nuova lezione in testa
            lessons_db.insert(0, {
                "timestamp": time.time(),
                "symbol": symbol,
                "outcome": outcome,
                "rule": new_rule,
                "pnl": pnl
            })

            # Cleanup: Teniamo solo le ultime 2500 lezioni per questo profilo
            cutoff = time.time() - (14 * 86400)
            lessons_db = [l for l in lessons_db if l['timestamp'] > cutoff][:2500]

            # Generiamo il prompt context specifico per questo profilo
            rules_summary = " | ".join([l['rule'] for l in lessons_db[:15]])
            self.lessons_learned = rules_summary
            
            # Update all_data
            all_data.setdefault("profiles", {})[profile_type] = {
                "history": lessons_db,
                "lessons": self.lessons_learned
            }

            # Save per persistence
            with open(self.lessons_file, 'w') as f:
                json.dump(all_data, f)
                
            logger.warning(f"🎓 [AI MENTOR] New lesson learned for {symbol}: {new_rule}")

        except Exception as e:
            logger.error(f"Error in post-mortem: {e}")
