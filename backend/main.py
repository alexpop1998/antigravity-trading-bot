import time
import uvicorn
import asyncio
import httpx
import logging
import os
import traceback
import sys
from typing import Optional
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel
from contextlib import asynccontextmanager

# Modular Imports (v30.0)
from bot import CryptoBot
from news_radar import NewsRadar
from whale_tracker import WhaleTracker
from social_scraper import SocialScraper
from liquidation_hunter import LiquidationHunter
from macro_calendar import MacroCalendar
from rl_tuner import RLTuner
from dex_sniper import DEXSniper
from listing_radar import ListingRadar
from dotenv import load_dotenv

load_dotenv(override=True)

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("TradingTerminal")

# Global instances (initialized in lifespan)
trading_bot = None
news_radar = None
whale_tracker = None
social_scraper = None
liquidation_hunter = None
macro_calendar = None
rl_tuner = None
dex_sniper = None
listing_radar = None
http_client = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """[v30.5] Modern Lifecycle Management."""
    global trading_bot, http_client, news_radar, whale_tracker, social_scraper, liquidation_hunter, macro_calendar, rl_tuner, dex_sniper, listing_radar
    
    async def initialize_all():
        global trading_bot, news_radar, whale_tracker, social_scraper, liquidation_hunter, macro_calendar, rl_tuner, dex_sniper, listing_radar
        try:
            # 1. Core Bot Sync
            trading_bot = CryptoBot()
            await trading_bot.initialize()
            
            # 2. Tool Initialization
            news_radar = NewsRadar(bot_instance=trading_bot, http_client=http_client)
            whale_tracker = WhaleTracker(bot_instance=trading_bot)
            social_scraper = SocialScraper(bot_instance=trading_bot, http_client=http_client)
            liquidation_hunter = LiquidationHunter(bot_instance=trading_bot)
            macro_calendar = MacroCalendar(bot_instance=trading_bot)
            rl_tuner = RLTuner(bot_instance=trading_bot)
            dex_sniper = DEXSniper(bot_instance=trading_bot)
            listing_radar = ListingRadar(bot_instance=trading_bot)

            # 3. Background Tasks
            def safe_run(coro, task_name):
                async def wrapper():
                    try:
                        await coro
                    except Exception as e:
                        logger.error(f"❌ FATAL ERROR in background task '{task_name}': {e}")
                return asyncio.create_task(wrapper())

            safe_run(news_radar.poll_news(), "NewsRadar")
            safe_run(whale_tracker.monitor_whales(), "WhaleTracker")
            safe_run(social_scraper.monitor_socials(), "SocialScraper")
            
            logger.info("✅ [LIFESPAN] All systems background-initialized.")
        except Exception as e:
            logger.error(f"❌ [BACKGROUND INIT FAILURE] {e}")
            traceback.print_exc()

    try:
        logger.info("🏁 [LIFESPAN] Initializing Trading Bot Cluster...")
        http_client = httpx.AsyncClient(timeout=30.0, follow_redirects=True)
        asyncio.create_task(initialize_all())
        yield
    except Exception as e:
        logger.error(f"❌ [LIFESPAN FAILURE] {e}")
        traceback.print_exc()
        raise e
    finally:
        logger.warning("🛑 [LIFESPAN] Shutdown initiated...")
        if trading_bot: await trading_bot.gateway.close()
        if http_client: await http_client.aclose()
        logger.info("👋 [LIFESPAN] Goodbye.")

app = FastAPI(title="Trading Terminal Backend", lifespan=lifespan)

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# WebSocket Logic
class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try: await connection.send_json(message)
            except: pass

manager = ConnectionManager()

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

# API Endpoints
@app.get("/api/status")
async def get_status():
    if not trading_bot: return {"status": "starting"}
    return {
        "status": "online",
        "exchange": getattr(trading_bot, 'active_exchange_name', trading_bot.config.get('strategic_params', {}).get('active_exchange', 'bitget')),
        "profile": trading_bot.profile_type,
        "active_trades": len([t for t in trading_bot.trade_levels.values() if t]),
        "heartbeat": time.time()
    }

@app.get("/api/manual-audit")
async def manual_audit():
    try:
        history = trading_bot.db.get_trades(limit=50)
        await trading_bot.strategy.analyst.perform_self_audit(history)
        return {"status": "success", "lessons": trading_bot.strategy.analyst.lessons_learned}
    except Exception as e:
        return {"status": "error", "message": str(e)}

frontend_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "frontend")
@app.get("/")
async def get_index():
    return FileResponse(os.path.join(frontend_path, "index.html"))

if __name__ == "__main__":
    import uvicorn
    import time
    uvicorn.run(app, host="0.0.0.0", port=8080)
