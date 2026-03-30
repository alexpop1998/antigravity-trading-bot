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
            if os.path.exists(self.lessons_file):
                with open(self.lessons_file, 'r') as f:
                    return json.load(f).get("lessons", "Focus on high-conviction technical alignment.")
        except:
            pass
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

            # 2. Prepara il prompt
            prompt = f"""
            Sei l'Analista Strategico di un Hedge Fund AI. Devi decidere se approvare un'operazione di trading ad alta frequenza.
            
            OPERAZIONE PROPOSTA:
            Asset: {symbol}
            Direzione: {side.upper()}
            Tipo Segnale: {signal_type}
            
            CRITICAL METRICS:
            RSI: {indicators.get('rsi', 'N/D')}, MACD_Hist: {indicators.get('macd_hist', 'N/D')}, 
            Vol_Change: {indicators.get('volume_change', 'N/D')}, ATR_Volatility: {indicators.get('atr', 'N/D')},
            Price_vs_EMA200: {indicators.get('price_vs_ema200', 'N/D')}
            
            --- MULTI-TIMEFRAME ANALYSIS (MACRO TREND) ---
            Trend Context: {mtf_context}
            
            --- LATEST MARKET NEWS ---
            {news_context}
            
            --- AI MEMORY (PAST PERFORMANCE) ---
            {memory_context}
            
            --- LESSONS FROM PREVIOUS AUDITS ---
            {self.lessons_learned}
            
            --- MISSIONE SNIPER ---
            Sei un Cecchino Istituzionale. Non aver paura delle monete meno capitalizzate, ma esigi prove schiaccianti:
            1. MODALITÀ CAPITAL BLITZ ATTIVA: Stiamo crescendo un capitale piccolo ($16). Sii AUDACE.
            2. Per monete a bassa capitalizzazione: Richiedi CONVINCENZA > 0.70.
            3. Per le Major (BTC, ETH): Richiedi CONVINCENZA > 0.70.
            4. Se vedi Momentum esplosivo, APPROVA anche se RSI è border-line. Siamo in modalità Blitz.
            3. ANALISI OVEREXTENSION: Non entrare mai LONG se il prezzo è troppo lontano dalla EMA200 o se l'RSI è già in ipercomprato estremo (>75).
            
            DEVI SEGUIRE QUESTO PROCESSO LOGICO (Chain-of-Thought):
            1. ANALISI MACRO: Valuta le news e il trend dominante.
            2. ANALISI TECNICA: Verifica RSI, MACD e Bollinger rispetto ai livelli chiave.
            3. VALUTAZIONE RISCHIO: Analizza lo spread, la liquidità e il funding.
            
            RISPONDI ESATTAMENTE IN QUESTO FORMATO JSON:
            {{
                "macro_analysis": "breve sintesi macro",
                "technical_analysis": "analisi degli indicatori",
                "risk_assessment": "valutazione spread/funding/volatilità",
                "verdict": "APPROVE" o "REJECT",
                "confidence": 0.0 a 1.0, 
                "suggested_leverage": intero tra 8 e 25,  // Sii dinamico ma prudente su monete volatili
                "position_strength": 1.0 a 25.0,  // 1.0 = Floor 1% Equity. 10.0 = 10% Equity. 25.0 = 25% Equity. 
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
            prompt = f"""
            Sei l'Analista di Rischio di un Hedge Fund AI. Valuta se mantenere, scalare o chiudere questa posizione aperta.
            
            POSIZIONE ATTUALE:
            Asset: {symbol}
            Direzione: {side.upper()}
            PnL Attuale: {current_pnl_pct:.2f}%
            
            CRITICAL METRICS:
            PnL: {current_pnl_pct:.2f}%, RSI: {indicators.get('rsi', 'N/D')}, 
            MACD_Hist: {indicators.get('macd_hist', 'N/D')}, Trend: {indicators.get('trend_adx', 'N/D')}
            
            MISSIONE:
            1. HOLD: Mantieni tutto invariato. Punta al TP2 (grande profitto). 
               - [!] IGNORA I PULLBACK: Se siamo in rosso (PnL negativo) ma il trend primario è intatto, scegli HOLD.
            2. SCALE_OUT: Suggerisci di chiudere il 50% (TP1) se siamo sopra lo 0.75% di profitto e vedi incertezza, esaurimento volumetrico o un'inversione imminente.
            3. PIVOT: Chiudi subito tutto e apri in direzione OPPOSTA (solo se il trend è girato violentemente su timeframe H1/H4).
            4. CLOSE: Chiudi tutto ora. 
               - [!] TRIGGER REVERSE: Scegli CLOSE solo se rilevi una rottura dei livelli di supporto/resistenza MACRO.
            
            REGOLE D'ORO:
            - Accettiamo un drawdown (rosso) fino al 2% di prezzo. Non avere paura se non vedi inversione reale.
            - Favorisci il HOLD per far correre i profitti (TP2).
            - Abbiamo un Trailing Stop LARGO che ci protegge.
            
            RISPONDI ESATTAMENTE IN QUESTO FORMATO JSON:
            {{
                "technical_status": "sintesi stato tecnico",
                "risk_status": "livello di rischio attuale",
                "action": "HOLD" o "SCALE_OUT" o "PIVOT" o "CLOSE",
                "confidence": 0.0 a 1.0,
                "reasoning": "Breve spiegazione tecnica (max 15 parole)"
            }}
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
            # Format last 20 trades for analysis
            history_str = json.dumps([{
                'symbol': t['symbol'], 'side': t['side'], 'pnl': t.get('pnl', 0), 
                'outcome': 'WIN' if t.get('pnl', 0) > 0 else 'LOSS',
                'reason': t.get('reason', 'N/A')
            } for t in trades_history[-20:]], indent=2)

            prompt = f"""
            Sei il Chief Risk Officer di un Hedge Fund. Analizza questi ultimi 20 trade e identifica 3 REGOLE d'oro per evitare perdite future basandoti sui pattern di errore riscontrati (LOSS).
            
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
                self.lessons_learned = result.get("lessons", self.lessons_learned)
                
                # Save to file for persistence
                with open(self.lessons_file, 'w') as f:
                    json.dump({"lessons": self.lessons_learned}, f)
                
                logger.warning(f"🧠 [AI AUDIT] Nuove lezioni apprese: {self.lessons_learned}")

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
            
            prompt = f"""
            Sei un Senior Trading Mentor. Analizza questo trade appena chiuso e ricava una lezione per il futuro.
            
            DATI TRADE:
            - Simbolo: {symbol}
            - Entry: {trade.get('entry_price')}
            - Close: {close_price}
            - PnL: {pnl:.2%}
            - Esito: {outcome}
            - Reasoning Entrata: {trade.get('reasoning', 'N/D')}
            
            CONTESTO INDICATORI:
            RSI: {indicators.get('rsi')}, MACD: {indicators.get('macd')}, Vol: {indicators.get('volume_change')}
            
            MISSIONE:
            1. Identifica il 'Fatal Flaw' (errore fatale) o il 'Success Pattern'.
            2. Definisci una 'Golden Rule' (max 12 parole) che il bot DEVE seguire per non ripetere errori.
            
            RISPONDI SOLO IN JSON:
            {{
                "analysis": "analisi tecnica dettagliata",
                "lesson": "cosa abbiamo imparato",
                "golden_rule": "regola sintetica"
            }}
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
            
            # --- SLIDING WINDOW MEMORY (v6.0) ---
            lessons_db = []
            if os.path.exists(self.lessons_file):
                try:
                    with open(self.lessons_file, 'r') as f:
                        data = json.load(f)
                        lessons_db = data.get("history", [])
                except: pass

            # Aggiungiamo la nuova lezione in testa
            lessons_db.insert(0, {
                "timestamp": time.time(),
                "symbol": symbol,
                "outcome": outcome,
                "rule": new_rule,
                "pnl": pnl
            })

            # Cleanup: Teniamo solo le ultime 2500 lezioni (circa 14 giorni @ 180 trade/day)
            cutoff = time.time() - (14 * 86400)
            lessons_db = [l for l in lessons_db if l['timestamp'] > cutoff][:2500]

            # Generiamo il prompt context riassuntivo per le prossime decisioni
            # Prendiamo le ultime 15 regole d'oro per dare più contesto senza saturare i token
            rules_summary = " | ".join([l['rule'] for l in lessons_db[:15]])
            self.lessons_learned = rules_summary

            # Save per persistence
            with open(self.lessons_file, 'w') as f:
                json.dump({"history": lessons_db, "lessons": self.lessons_learned}, f)
                
            logger.warning(f"🎓 [AI MENTOR] New lesson learned for {symbol}: {new_rule}")

        except Exception as e:
            logger.error(f"Error in post-mortem: {e}")
