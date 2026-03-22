import asyncio
import feedparser
import logging
import httpx
import os
from bs4 import BeautifulSoup
from openai import AsyncOpenAI

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
        self.mirofish_url = "http://127.0.0.1:5001/api/simulation/quick_run" 
        
        # Gemini setup for the Gatekeeper
        api_key = os.getenv("LLM_API_KEY")
        self.ai_client = AsyncOpenAI(
            api_key=api_key,
            base_url="https://generativelanguage.googleapis.com/v1beta/openai/"
        ) if api_key else None
        
        # Concurrency control for AI Gatekeeper to avoid resource exhaustion
        self.semaphore = asyncio.Semaphore(3)

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
                    
            # Poll all feeds every 60 seconds
            await asyncio.sleep(60)

    async def process_article(self, title, link):
        try:
            logger.info(f"🕷️ Scraping article text from: {link}")
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
                    
            # 2. The Gatekeeper (AI Filter)
            if not self.ai_client:
                logger.warning("No LLM_API_KEY. Skipping Gatekeeper. Sending directly to MiroFish.")
                await self.trigger_mirofish(title, article_content)
                return

            async with self.semaphore:
                logger.info(f"🛡️ Gatekeeper evaluating: {title}")
                prompt = f"""
                Sei un Hedge Fund Manager istituzionale. Analizza questo articolo.
                
                TITOLO: {title}
                TESTO: {article_content}
                
                DOMANDA: Questo articolo contiene informazioni fondamentali di altissimo impatto (es. crolli o regolamentazioni sistemiche, enormi hack, approvazioni di ETF istituzionali, clamorose partnership mondiali) che causeranno un panico o un'euforia irrazionale per le criptovalute nelle prossime 24 ore?
                
                RISPONDI ESATTAMENTE SOLO CON "YES" O "NO". Nessuna spiegazione.
                """
                
                ai_response = await self.ai_client.chat.completions.create(
                    model=os.getenv("LLM_MODEL_NAME", "gemini-1.5-flash"),
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.1
                )
                
                verdict = ai_response.choices[0].message.content.strip().upper()
                
                if "YES" in verdict:
                    logger.info(f"🟢 Gatekeeper APPROVED: {title}. Forwarding to MiroFish...")
                    self.bot.add_alert("GATEKEEPER", f"Approved: {title[:30]}...", "High Impact")
                    await self.trigger_mirofish(title, article_content)
                else:
                    logger.info(f"🔴 Gatekeeper REJECTED (Not market-moving): {title}")

        except Exception as e:
            logger.error(f"Failed to process article {link}: {e}")

    async def trigger_mirofish(self, title, article_content):
        logger.info(f"Deep Seed sent to MiroFish: {title}")
        try:
            # Use the shared client
            client = self.http_client if self.http_client else httpx.AsyncClient()
            
            payload = {
                "seed": title,
                "content": article_content,
                "agents": 5, 
                "timeout": 30
            }
            
            # Mocking MiroFish logic assuming the real API processes `content` as reality seed
            # In a real scenario we'd do a POST to MiroFish and await the score
            sentiment_score = 50
            if "hack" in title.lower() or "bankrupt" in title.lower() or "sec" in title.lower() or "crash" in title.lower():
                sentiment_score = 20
            elif "etf" in title.lower() or "partnership" in title.lower() or "upgrade" in title.lower():
                sentiment_score = 80
                
            logger.info(f"MiroFish Verdict: Sentiment Score = {sentiment_score}")
            
            if sentiment_score < 30:
                logger.error("🛑 SENTIMENT IS DEADLY. EXECUTING EMERGENCY SHORT ON ALL ASSETS.")
                for symbol in self.bot.symbols:
                    asyncio.create_task(self.bot.handle_signal(symbol, "GATEKEEPER", "sell", is_black_swan=True))
                            
            elif sentiment_score > 70:
                logger.info("🚀 SENTIMENT IS EUPHORIC. EXECUTING EMERGENCY LONG ON ALL ASSETS.")
                for symbol in self.bot.symbols:
                    asyncio.create_task(self.bot.handle_signal(symbol, "GATEKEEPER", "buy", is_black_swan=True))
                                
        except Exception as e:
            logger.error(f"Failed to reach MiroFish: {e}")
