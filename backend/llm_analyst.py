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
            
            DATI TECNICI ATTUALI:
            {json.dumps(indicators, indent=2)}
            
            --- MULTI-TIMEFRAME ANALYSIS (MACRO TREND) ---
            Trend Context: {mtf_context}
            
            --- LATEST MARKET NEWS ---
            {news_context}
            
            --- AI MEMORY (PAST PERFORMANCE) ---
            {memory_context}
            
            --- LESSONS FROM PREVIOUS AUDITS ---
            {self.lessons_learned}
            
            MISSIONE:
            Basandoti sui dati tecnici e sullo storico dei risultati (se presenti), decidi se questa operazione ha un'alta probabilità di successo. 
            
            DEVI SEGUIRE QUESTO PROCESSO LOGICO (Chain-of-Thought):
            1. ANALISI MACRO: Valuta le news e il trend dominante.
            2. ANALISI TECNICA: Verifica RSI, MACD e Bollinger rispetto ai livelli chiave.
            3. VALUTAZIONE RISCHIO: Analizza lo spread, la liquidità e il funding.
            
            RISPONDI ESATTAMENTE IN QUESTO FORMATO JSON:
            {
                "macro_analysis": "breve sintesi macro",
                "technical_analysis": "analisi degli indicatori",
                "risk_assessment": "valutazione spread/funding/volatilità",
                "verdict": "APPROVE" o "REJECT",
                "confidence": 0.0 a 1.0,
                "suggested_leverage": intero da 5 a 25,
                "position_strength": 0.5 a 2.5,  // Moltiplicatore di capitale: 0.5=prudente, 1.0=standard, 2.5=massima convinzione
                "sl_multiplier": 0.5 a 2.0,
                "tp_multiplier": 0.5 a 2.0,
                "tp_price": numero o null,
                "reasoning": "Sintesi finale della decisione (max 20 parole)"
            }
            """

            async with self.semaphore:
                response = await self.ai_client.chat.completions.create(
                    model=self.model_name,
                    messages=[{"role": "user", "content": prompt}],
                    response_format={ "type": "json_object" },
                    temperature=0.2
                )
                
                result = json.loads(response.choices[0].message.content)
                verdict = result.get("verdict", "APPROVE").upper()
                confidence = float(result.get("confidence", 0.8))
                leverage = int(result.get("suggested_leverage", self.bot.leverage))
                strength = float(result.get("position_strength", 1.0))
                sl_mult = float(result.get("sl_multiplier", 1.0))
                tp_mult = float(result.get("tp_multiplier", 1.0))
                tp_price = result.get("tp_price")
                reasoning = result.get("reasoning", "Analisi standard.")

                logger.info(f"🧠 [LLM ANALYST] {symbol} {side.upper()} -> {verdict} (Lev: {leverage}x, Strength: {strength:.2f}, SL_m: {sl_mult:.2f}, TP_m: {tp_mult:.2f}, TP_p: {tp_price}) | Reason: {reasoning}")
                
                return (verdict == "APPROVE"), strength, leverage, sl_mult, tp_mult, tp_price, reasoning

        except Exception as e:
            logger.error(f"Errore durante la decisione LLM per {symbol}: {e}")
            return True, 1.0, self.bot.leverage, 1.0, 1.0, None, f"ERROR: {str(e)}"

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
            
            DATI TECNICI ATTUALI:
            {json.dumps(indicators, indent=2)}
            
            MISSIONE:
            1. HOLD: Mantieni tutto invariato.
            2. SCALE_OUT: Chiudi il 50% per proteggere il profitto o ridurre il rischio.
            3. PIVOT: Chiudi subito tutto e apri in direzione OPPOSTA (solo se il trend è girato violentemente).
            4. CLOSE: Chiudi tutto ora.
            
            DEVI SEGUIRE QUESTO PROCESSO LOGICO:
            - Analizza se il trend tecnico a supporto dell'entry è ancora valido.
            - Valuta se il PnL attuale giustifica una chiusura parziale per "de-risking".
            
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
        """Analizza l'esito di un trade per estrarre lezioni strategiche."""
        if not self.ai_client:
            return

        try:
            outcome = "WIN" if pnl > 0.005 else ("LOSS" if pnl < -0.005 else "NEUTRAL/PARTIAL")
            
            prompt = f"""
            Sei un Senior Trading Mentor. Analizza questo trade appena chiuso e ricava una lezione per il futuro.
            
            DATI TRADE:
            - Simbolo: {symbol}
            - Entry: {trade.get('entry_price')}
            - Close: {close_price}
            - PnL: {pnl:.2%}
            - Esito: {outcome}
            - Ragione entrata originale: {trade.get('reasoning', 'N/D')}
            
            MISSIONE:
            - Cosa abbiamo imparato? (es. "RSI era troppo alto", "Trend News era fasullo").
            - Definisci una regola "Golden Rule" di massimo 10 parole.
            
            RISPONDI SOLO IN JSON:
            {{
                "analysis": "analisi tecnica del perché è andata così",
                "lesson": "la lezione imparata",
                "golden_rule": "regola sintetica da ricordare"
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
            new_lesson = content.get("golden_rule", "Be careful.")
            
            # Update lessons_learned memory (simple concatenation or summary)
            self.lessons_learned = f"{new_lesson} | {self.lessons_learned}"[:500]
            
            # Save to file
            with open(self.lessons_file, 'w') as f:
                json.dump({"lessons": self.lessons_learned}, f)
                
            logger.warning(f"🎓 [AI MENTOR] New lesson learned for {symbol}: {new_lesson}")

        except Exception as e:
            logger.error(f"Error in post-mortem: {e}")
