import asyncio
import feedparser
import logging
import httpx
import os
import json
from typing import Dict, List, Any
from bs4 import BeautifulSoup
import httpx

logger = logging.getLogger("NewsRadarV2")
logger.setLevel(logging.INFO)

class NewsRadar:
    def __init__(self, bot_instance, http_client=None):
        self.bot = bot_instance
        self.http_client = http_client # Shared client from main.py
        self.rss_urls = [
            "https://cointelegraph.com/rss",
            "https://www.coindesk.com/arc/outboundfeeds/rss/",
            "https://cryptoslate.com/feed/"
        ]
        # Broader set of keywords for alpha
        self.keywords = ["sec", "lawsuit", "hack", "bankrupt", "etf", "banned", "crash", 
                         "partnership", "mainnet", "upgrade", "acquired", "inflation", "rates"]
        self.seen_guids = set()
        
        # Gemini 1.5 Flash (v33.8 Cost Safe)
        self.api_key = os.getenv("LLM_API_KEY")
        model_name = os.getenv("LLM_MODEL_NAME", "gemini-1.5-flash")
        self.gemini_url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={self.api_key}"
        
        # Using Centralized AI Gatekeeper (v31.07)
        self.semaphore = self.bot.ai_semaphore
        
        # v38.1 Gemini Cost Capping
        self.hourly_ai_limit = 10
        self.hourly_calls = 0
        self.last_reset_time = 0

    async def analyze_article(self, article: Dict[str, str]):
        if not self.api_key: return

        async with self.semaphore:
            # 🚀 FULL SPEED (v31.12)
            await asyncio.sleep(0.5)
            
            prompt = f"""
            Analyze this crypto news for immediate market impact (ALPHA). 
            Title: {article['title']}
            Summary: {article['summary'][:500]}
            
            Return JSON only: {{"impact": "high|medium|low", "sentiment": "bullish|bearish|neutral", "reason": "short explanation"}}
            """
            
            payload = {
                "contents": [{
                    "parts": [{"text": prompt}]
                }],
                "generationConfig": {
                    "response_mime_type": "application/json",
                }
            }

            try:
                headers = {'Content-Type': 'application/json'}
                response = await self.http_client.post(self.gemini_url, json=payload, headers=headers)
                
                if response.status_code != 200:
                    logger.error(f"Gemini API Error: {response.status_code} - {response.text}")
                    return

                res_json = response.json()
                generation = res_json['candidates'][0]['content']['parts'][0]['text']
                analysis = json.loads(generation)
                
                if analysis.get('impact') == 'high':
                    logger.critical(f"🚀 HIGH IMPACT NEWS! {article['title']} - Sentiment: {analysis['sentiment'].upper()}")
                    # Route to bot orchestrator
                    await self.bot.handle_signal(
                        symbol="GLOBAL", 
                        source="NEWS_RADAR", 
                        side="buy" if analysis['sentiment'] == 'bullish' else "sell",
                        confidence=0.85,
                        metadata=analysis
                    )
            except Exception as e:
                logger.error(f"Error in Gemini analysis: {e}")

    async def poll_news(self):
        logger.info("Starting News Radar V2 (Multi-Source & Gatekeeper)...")
        while True:
            for url in self.rss_urls:
                try:
                    # Parse feed in thread to avoid blocking the event loop
                    feed = await asyncio.to_thread(feedparser.parse, url)
                    for entry in feed.entries:
                        if entry.guid not in self.seen_guids:
                            self.seen_guids.add(entry.guid)
                            
                            title = entry.title.lower()
                            if any(kw in title for kw in self.keywords):
                                logger.info(f"🔍 Keyword matched in title: {entry.title}")
                                # Spawn a task to handle this article asynchronously
                                asyncio.create_task(self.process_article(entry.title, entry.link))
                except Exception as e:
                    logger.error(f"Error polling {url}: {e}")
                    
            # Poll all feeds every 120 seconds for cost optimization (reduced from 20s)
            await asyncio.sleep(120)

    async def process_article(self, title, link):
        try:
            # 🛡️ Anti-429 Burst Protection
            await asyncio.sleep(10) 
            
            # 0. Local Filter (Pre-Gatekeeper)
            # v43.4.0 [COST OPTIMIZATION] Only evaluate news for active or top potential symbols
            high_priority_kws = ["sec", "fed", "hack", "etf", "lawsuit", "bnb", "binance", "cz", "usdt"]
            
            # Add dynamic symbols from bot scanner
            relevant_symbols = [s.split('/')[0].lower() for s in self.bot.dynamic_symbols[:10]]
            active_symbols = [s.split('/')[0].lower() for s in self.bot.active_positions.keys()]
            watch_list = list(set(high_priority_kws + relevant_symbols + active_symbols))
            
            if not any(kw in title.lower() for kw in watch_list):
                logger.info(f"⏭️ Skipping Gatekeeper (Irrelevant to current focus): {title}")
                return

            logger.info(f"🕷️ Scraping article text from: {link}")
            # ... rest of the function ...
            # 1. Scrape the article text
            # Use the shared client if available, otherwise fallback (to avoid total failure)
            client = self.http_client if self.http_client else httpx.AsyncClient(timeout=15.0)
            
            response = await client.get(link)
            soup = BeautifulSoup(response.text, 'lxml')
            # Extract text from paragraphs
            paragraphs = soup.find_all('p')
            full_text = " ".join([p.get_text() for p in paragraphs])
            
            # Limit text to roughly 4000 chars to cover the main body without huge payloads
            article_content = full_text[:4000].strip()
            
            if len(article_content) < 100:
                logger.warning(f"Could not extract sufficient text from {link}. Using title only.")
                article_content = title

            if not self.api_key:
                logger.warning("Nessuna LLM_API_KEY. Salto Gatekeeper.")
                return

            # --- v38.1 Hourly Capping Logic ---
            import time
            now = time.time()
            if now - self.last_reset_time > 3600:
                self.hourly_calls = 0
                self.last_reset_time = now
            
            if self.hourly_calls >= self.hourly_ai_limit:
                logger.warning(f"⚠️ [NEWS CAP] Hourly Gemini limit ({self.hourly_ai_limit}) reached. Skipping AI audit for: {title}")
                return
            
            self.hourly_calls += 1

            async with self.semaphore:
                logger.info(f"🛡️ Gatekeeper evaluating: {title}")
                prompt = f"""
                Sei un Hedge Fund Manager istituzionale. Analizza questo articolo.
                
                TITOLO: {title}
                TESTO: {article_content}
                
                DOMANDA: Questo articolo contiene informazioni fondamentali di altissimo impatto (es. crolli o regolamentazioni sistemiche, enormi hack, approvazioni di ETF istituzionali, clamorose partnership mondiali) che causeranno un panico o un'euforia irrazionale per le criptovalute nelle prossime 24 ore?
                
                RISPONDI ESATTAMENTE SOLO CON "YES" O "NO". Nessuna spiegazione.
                """
                
                payload = {
                    "contents": [{"parts": [{"text": prompt}]}],
                    "generationConfig": {
                        "temperature": 0.1,
                        "responseMimeType": "text/plain"
                    }
                }

                async with httpx.AsyncClient(timeout=30.0) as client:
                    resp = await client.post(self.gemini_url, json=payload)
                    resp.raise_for_status()
                    res_json = resp.json()
                    verdict = res_json['candidates'][0]['content']['parts'][0]['text'].strip().upper()
                
                if "YES" in verdict:
                    logger.info(f"🟢 Gatekeeper APPROVED: {title}. Executing high-priority signal analysis.")
                    self.bot.add_alert("GATEKEEPER", f"Approved: {title[:30]}...", "High Impact")
                    
                    # High-priority signal analysis handled directly for speed/simplicity
                    sentiment_score = 50
                    if any(kw in title.lower() for kw in ["hack", "bankrupt", "sec", "crash"]):
                        sentiment_score = 20
                    elif any(kw in title.lower() for kw in ["etf", "partnership", "upgrade"]):
                        sentiment_score = 80
                    
                    if sentiment_score < 30:
                        logger.error("🛑 SENTIMENT IS DEADLY. EXECUTING EMERGENCY SHORT ON ALL ASSETS.")
                        for symbol in self.bot.latest_data.keys():
                            # --- PRICE SPIKE CHECK (DeepSeek v3) ---
                            # If it's a SHORT signal but price already dumped > 2% in 2m, skip to avoid "selling bottom"
                            recent_change = self.bot.latest_data.get(symbol, {}).get('changePercent', 0)
                            if recent_change < -2.0:
                                logger.info(f"⏭️ Skipping SHORT on {symbol}: Price already dumped {recent_change:.2%}")
                                continue
                            asyncio.create_task(self.bot.handle_signal(symbol, "GATEKEEPER", "sell", is_black_swan=True))
                    elif sentiment_score > 70:
                        logger.info("🚀 SENTIMENT IS EUPHORIC. EXECUTING EMERGENCY LONG ON ALL ASSETS.")
                        for symbol in self.bot.latest_data.keys():
                            # --- PRICE SPIKE CHECK (DeepSeek v3) ---
                            # If it's a LONG signal but price already pumped > 2% in 2m, skip to avoid "buying top"
                            recent_change = self.bot.latest_data.get(symbol, {}).get('changePercent', 0)
                            if recent_change > 2.0:
                                logger.info(f"⏭️ Skipping LONG on {symbol}: Price already pumped {recent_change:.2%}")
                                continue
                            asyncio.create_task(self.bot.handle_signal(symbol, "GATEKEEPER", "buy", is_black_swan=True))
                else:
                    logger.info(f"🔴 Gatekeeper REJECTED (Not market-moving): {title}")

        except Exception as e:
            logger.error(f"Failed to process article {link}: {e}")
