import asyncio
import feedparser
import logging
import httpx
import os
import json
import time
from typing import Dict, List, Any
from bs4 import BeautifulSoup

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
        self.keywords = ["sec", "lawsuit", "hack", "bankrupt", "etf", "banned", "crash", 
                         "partnership", "mainnet", "upgrade", "acquired", "inflation", "rates"]
        self.seen_guids = set()
        
        # --- v51.3.1 [DYNAMIC REFRESH] ---
        self._refresh_api_config()
        
        self.semaphore = self.bot.ai_semaphore
        self.hourly_ai_limit = 20 # Increased for DeepSeek efficiency
        self.hourly_calls = 0
        self.last_reset_time = 0

    def _refresh_api_config(self):
        """[V51.6.1] [GWEN PURGA] Detect and kill Google domain remnants."""
        self.api_key = os.getenv("LLM_API_KEY")
        self.base_url = os.getenv("LLM_BASE_URL", "https://api.deepseek.com").rstrip('/')
        if "google" in self.base_url.lower():
            self.base_url = "https://api.deepseek.com"
            logger.warning("☣️ [NEWS] Detected Google Poison in Environment. Overriding to absolute DeepSeek.")
        
        self.api_endpoint = f"{self.base_url}/v1/chat/completions"
        self.model_name = "deepseek-chat" # [v52.1.1] Force consistency

    async def _ask_deepseek(self, system_prompt: str, user_prompt: str, temperature: float = 0.1) -> str:
        """Centralized DeepSeek API Caller for News (v51.3.1 Instrumented)"""
        self._refresh_api_config()
        if not self.api_key: return "NO"
        
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
            "temperature": temperature
        }

        async with self.semaphore:
            async with httpx.AsyncClient(timeout=30.0) as client:
                logger.info(f"📍 [NEWS AI] Calling DeepSeek at {self.api_endpoint}...")
                resp = await client.post(self.api_endpoint, headers=headers, json=payload)
                resp.raise_for_status()
                data = resp.json()
                return data['choices'][0]['message']['content'].strip().upper()

    async def poll_news(self):
        logger.info("Starting News Radar V2 (DeepSeek Powered)...")
        while True:
            for url in self.rss_urls:
                try:
                    feed = await asyncio.to_thread(feedparser.parse, url)
                    for entry in feed.entries:
                        if entry.guid not in self.seen_guids:
                            self.seen_guids.add(entry.guid)
                            title = entry.title.lower()
                            if any(kw in title for kw in self.keywords):
                                logger.info(f"🔍 Keyword matched in title: {entry.title}")
                                asyncio.create_task(self.process_article(entry.title, entry.link))
                except Exception as e:
                    logger.error(f"Error polling {url}: {e}")
            await asyncio.sleep(120)

    async def process_article(self, title, link):
        try:
            await asyncio.sleep(5) 
            high_priority_kws = ["sec", "fed", "hack", "etf", "lawsuit", "bnb", "binance", "cz", "usdt"]
            relevant_symbols = [s.split('/')[0].lower() for s in self.bot.dynamic_symbols[:20]]
            active_symbols = [s.split('/')[0].lower() for s in self.bot.active_positions.keys()]
            watch_list = list(set(high_priority_kws + relevant_symbols + active_symbols))
            
            if not any(kw in title.lower() for kw in watch_list):
                return

            logger.info(f"🕷️ Scraping article: {link}")
            client = self.http_client if self.http_client else httpx.AsyncClient(timeout=15.0)
            response = await client.get(link)
            soup = BeautifulSoup(response.text, 'lxml')
            paragraphs = soup.find_all('p')
            article_content = " ".join([p.get_text() for p in paragraphs])[:3000].strip()
            
            if len(article_content) < 100: article_content = title

            now = time.time()
            if now - self.last_reset_time > 3600:
                self.hourly_calls = 0
                self.last_reset_time = now
            if self.hourly_calls >= self.hourly_ai_limit: return
            self.hourly_calls += 1

            logger.info(f"🛡️ Gatekeeper evaluating: {title}")
            sys_p = "Sei un Hedge Fund Manager. Rispondi ESATTAMENTE solo 'YES' o 'NO'."
            user_p = f"Articolo: {title}\nTesto: {article_content}\n\nDOMANDA: Questo articolo causerà panico/euforia irrazionale nelle prossime 24 ore?"
            
            self.api_key = os.getenv("LLM_API_KEY")
            verdict = await self._ask_deepseek(sys_p, user_p)
            
            if "YES" in verdict:
                logger.info(f"🟢 Gatekeeper APPROVED: {title}")
                self.bot.add_alert("GATEKEEPER", f"Approved: {title[:30]}...", "High Impact")
                sentiment_score = 50
                if any(kw in title.lower() for kw in ["hack", "bankrupt", "sec", "crash"]): sentiment_score = 20
                elif any(kw in title.lower() for kw in ["etf", "partnership", "upgrade"]): sentiment_score = 80
                
                if sentiment_score < 30:
                    logger.error("🛑 EMERGENCY SHORT ON ALL ASSETS initiated by Gatekeeper.")
                    for symbol in self.bot.latest_data.keys():
                        asyncio.create_task(self.bot.handle_signal(symbol, "GATEKEEPER", "sell", is_black_swan=True))
                elif sentiment_score > 70:
                    logger.info("🚀 EMERGENCY LONG ON ALL ASSETS initiated by Gatekeeper.")
                    for symbol in self.bot.latest_data.keys():
                        asyncio.create_task(self.bot.handle_signal(symbol, "GATEKEEPER", "buy", is_black_swan=True))
            else:
                logger.info(f"🔴 Gatekeeper REJECTED: {title}")
        except Exception as e:
            logger.error(f"Failed to process {link}: {e}")
