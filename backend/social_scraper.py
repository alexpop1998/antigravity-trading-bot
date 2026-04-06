import asyncio
import logging
import os
import httpx
import json
import httpx
from dotenv import load_dotenv

load_dotenv(override=True)
logger = logging.getLogger("SocialScraper")
logger.setLevel(logging.INFO)

class SocialScraper:
    def __init__(self, bot_instance, http_client=None):
        self.bot = bot_instance
        self.http_client = http_client # Shared client from main.py
        self.api_key = os.getenv("CRYPTOPANIC_API_KEY")
        self.last_news_id = None
        
        # LLM Config (Gemini REST v33.8 Cost Safe)
        self.api_key_llm = os.getenv("LLM_API_KEY")
        model_name = os.getenv("LLM_MODEL_NAME", "gemini-1.5-flash")
        self.gemini_url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={self.api_key_llm}"

    async def fetch_cryptopanic_news(self):
        if not self.api_key or self.api_key == "YOUR_CRYPTOPANIC_KEY":
            # Fallback to simulated "hot" news for testing if no API key
            return [{"id": 9999, "title": "Bitcoin breaking above $100k according to major analysts", "metadata": {"ticker": "BTC"}}]
            
        url = f"https://cryptopanic.com/api/v1/posts/?auth_token={self.api_key}&public=true"
        try:
            client = self.http_client if self.http_client else httpx.AsyncClient(timeout=15.0)
            response = await client.get(url)
            if response.status_code == 200:
                data = response.json()
                return data.get('results', [])
            else:
                logger.error(f"CryptoPanic API Error: {response.status_code}")
                return []
        except Exception as e:
            logger.error(f"Error fetching CryptoPanic news: {e}")
            return []

    async def analyze_sentiment(self, text):
        if not self.api_key_llm:
            return None
            
        prompt = f"""
        Analyze the following news headline for IMMEDIATE crypto market impact.
        NEWS: "{text}"
        
        Identify:
        1. Target Crypto Ticker (e.g. BTC, ETH, SOL).
        2. Sentiment (BULLISH, BEARISH, or NEUTRAL).
        3. Scalptime Impact (0 to 1.0) - How fast will the market react?
        
        Respond ONLY in JSON format: {{"ticker": "SYMBOL", "sentiment": "BULLISH/BEARISH/NEUTRAL", "confidence": 0.85}}
        """
        
        try:
            payload = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {
                    "temperature": 0.1,
                    "responseMimeType": "application/json"
                }
            }
            
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(self.gemini_url, json=payload)
                resp.raise_for_status()
                res_json = resp.json()
                text_response = res_json['candidates'][0]['content']['parts'][0]['text']
                return json.loads(text_response)
        except Exception as e:
            logger.error(f"Sentiment analysis error: {e}")
            return None

    async def monitor_socials(self):
        logger.info("🚀 Starting Institutional News Aggregator (CryptoPanic)...")
        while True:
            news_items = await self.fetch_cryptopanic_news()
            
            for item in news_items:
                item_id = item.get('id')
                if item_id == self.last_news_id:
                    break # Already processed the latest batch
                
                title = item.get('title', '')
                logger.info(f"📰 Analyzing News: {title[:50]}...")
                
                analysis = await self.analyze_sentiment(title)
                if analysis and analysis.get('confidence', 0) > 0.8:
                    symbol = f"{analysis['ticker']}/USDT:USDT"
                    # Check if symbol is in our bot's monitored symbols
                    if any(s.startswith(analysis['ticker']) for s in self.bot.latest_data.keys()):
                        side = 'buy' if analysis['sentiment'] == 'BULLISH' else 'sell'
                        current_price = self.bot.latest_data.get(symbol, {}).get('price', 0)
                        
                        if current_price > 0:
                            logger.critical(f"🎯 NEWS ALPHA: {side.upper()} {symbol} (Conf: {analysis['confidence']})")
                            self.bot.add_alert("NEWS", f"{analysis['sentiment']} Signal: {analysis['ticker']}", f"Conf: {analysis['confidence']}")
                            # Usa handle_signal con modificatore basato sulla confidenza
                            asyncio.create_task(self.bot.handle_signal(symbol, "NEWS", side, weight_modifier=analysis.get('confidence', 0.8)))
                
                if item_id:
                    self.last_news_id = item_id
            
            await asyncio.sleep(120) # Poll every 120 seconds (optimized from 20s for costs)
