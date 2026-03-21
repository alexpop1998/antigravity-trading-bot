import aiohttp
import asyncio
import logging
import os
from openai import AsyncOpenAI
from dotenv import load_dotenv

load_dotenv(override=True)
logger = logging.getLogger("SocialScraper")
logger.setLevel(logging.INFO)

class SocialScraper:
    def __init__(self, bot_instance):
        self.bot = bot_instance
        self.api_key = os.getenv("CRYPTOPANIC_API_KEY")
        self.last_news_id = None
        
        # LLM Config
        llm_api_key = os.getenv("LLM_API_KEY")
        llm_base_url = os.getenv("LLM_BASE_URL", "https://api.openai.com/v1")
        self.model_name = os.getenv("LLM_MODEL_NAME", "gpt-4-turbo")
        
        if llm_api_key and llm_api_key != "YOUR_LLM_KEY":
            self.client = AsyncOpenAI(
                api_key=llm_api_key,
                base_url=llm_base_url
            )
        else:
            self.client = None
            logger.warning("LLM_API_KEY not found. News analysis will be restricted.")

    async def fetch_cryptopanic_news(self):
        if not self.api_key or self.api_key == "YOUR_CRYPTOPANIC_KEY":
            # Fallback to simulated "hot" news for testing if no API key
            return [{"id": 9999, "title": "Bitcoin breaking above $100k according to major analysts", "metadata": {"ticker": "BTC"}}]
            
        url = f"https://cryptopanic.com/api/v1/posts/?auth_token={self.api_key}&public=true"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data.get('results', [])
                    else:
                        logger.error(f"CryptoPanic API Error: {response.status}")
                        return []
        except Exception as e:
            logger.error(f"Error fetching CryptoPanic news: {e}")
            return []

    async def analyze_sentiment(self, text):
        if not self.client:
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
            response = await self.client.chat.completions.create(
                model=self.model_name,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                response_format={ "type": "json_object" }
            )
            import json
            return json.loads(response.choices[0].message.content)
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
                    if any(s.startswith(analysis['ticker']) for s in self.bot.symbols):
                        side = 'buy' if analysis['sentiment'] == 'BULLISH' else 'sell'
                        current_price = self.bot.latest_data.get(symbol, {}).get('price', 0)
                        
                        if current_price > 0:
                            logger.critical(f"🎯 NEWS ALPHA: {side.upper()} {symbol} (Conf: {analysis['confidence']})")
                            self.bot.add_alert("NEWS", f"{analysis['sentiment']} Signal: {analysis['ticker']}", f"Conf: {analysis['confidence']}")
                            # Usa handle_signal con modificatore basato sulla confidenza
                            asyncio.create_task(self.bot.handle_signal(symbol, "NEWS", side, weight_modifier=analysis.get('confidence', 0.8)))
                
                if item_id:
                    self.last_news_id = item_id
                    break # Only process the single top news per loop to be safe in testing
            
            await asyncio.sleep(60) # Poll every minute
