from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import asyncio
import httpx
import json
import os
from dotenv import load_dotenv
load_dotenv(override=True)

from bot import CryptoBot
from openai import AsyncOpenAI
from news_radar import NewsRadar
from whale_tracker import WhaleTracker
from social_scraper import SocialScraper
from liquidation_hunter import LiquidationHunter
from macro_calendar import MacroCalendar
from rl_tuner import RLTuner
from dex_sniper import DEXSniper
from fastapi.responses import FileResponse
import logging
print("🚀 DEBUG: LOADING MAIN.PY FROM " + __file__)

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("TradingTerminal")

app = FastAPI(title="Trading Terminal Backend")

# Enable CORS for the frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Active WebSocket connections
class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                pass

manager = ConnectionManager()

# Global instances (initialized in startup_event)
trading_bot = None
news_radar = None
whale_tracker = None
social_scraper = None
liquidation_hunter = None
macro_calendar = None
rl_tuner = None
dex_sniper = None
http_client = None # NEW: Global shared HTTP client

@app.on_event("startup")
async def startup_event():
    global trading_bot, news_radar, whale_tracker, social_scraper, liquidation_hunter, macro_calendar, rl_tuner, dex_sniper, http_client
    
    logger.info("Initializing Shared Resources...")
    # Initialize global shared HTTP client to prevent connection pool exhaustion
    http_client = httpx.AsyncClient(timeout=30.0, follow_redirects=True)
    
    logger.info("Initializing Bot and Tools...")
    trading_bot = CryptoBot()
    await trading_bot.initialize()
    
    # Pass the shared http_client to all tools that need it
    news_radar = NewsRadar(bot_instance=trading_bot, http_client=http_client)
    whale_tracker = WhaleTracker(bot_instance=trading_bot, threshold_btc=500)
    social_scraper = SocialScraper(bot_instance=trading_bot, http_client=http_client)
    liquidation_hunter = LiquidationHunter(bot_instance=trading_bot, threshold_usd=500000)
    macro_calendar = MacroCalendar(bot_instance=trading_bot, http_client=http_client)
    rl_tuner = RLTuner(interval_hours=24)
    dex_sniper = DEXSniper(bot_instance=trading_bot)

    # Helper to wrap background tasks with logging
    def safe_run(coro, task_name):
        async def wrapper():
            try:
                await coro
            except Exception as e:
                logger.error(f"❌ FATAL ERROR in background task '{task_name}': {e}")
                import traceback
                logger.error(traceback.format_exc())
        return asyncio.create_task(wrapper())

    # Start the bot's background update loop
    safe_run(trading_bot.update_loop(), "Bot Update Loop")
    safe_run(trading_bot.account_update_loop(), "Account Update Loop")
    safe_run(trading_bot.funding_rate_loop(), "Funding Rate Loop")
    
    # Start the websocket broadcaster
    safe_run(broadcast_market_data(), "Market Broadcaster")
    safe_run(broadcast_account_data(), "Account Broadcaster")
    
    # Start the news radar loop
    safe_run(news_radar.poll_news(), "News Radar")
    
    # Start the institutional edge tools
    safe_run(whale_tracker.monitor_whales(), "Whale Tracker")
    safe_run(social_scraper.monitor_socials(), "Social Scraper")
    safe_run(liquidation_hunter.monitor_liquidations(), "Liquidation Hunter")
    safe_run(macro_calendar.monitor_calendar(), "Macro Calendar")
    safe_run(rl_tuner.run_optimization_loop(), "RL Tuner")
    safe_run(dex_sniper.monitor_arbitrage(), "DEX Sniper")

@app.on_event("shutdown")
async def shutdown_event():
    global http_client, trading_bot
    logger.info("Shutting down and cleaning up resources...")
    if http_client:
        await http_client.aclose()
        logger.info("Shared HTTP client closed.")
    if trading_bot and trading_bot.exchange:
        await trading_bot.exchange.close()
        logger.info("Exchange connection closed.")

async def broadcast_market_data():
    while True:
        await asyncio.sleep(2) # Broadcast every 2 seconds
        data = trading_bot.get_dashboard_data()
        await manager.broadcast({"type": "MARKET_UPDATE", "data": data})

async def broadcast_account_data():
    while True:
        await asyncio.sleep(10) # Broadcast every 10 seconds
        data = trading_bot.get_account_data()
        await manager.broadcast({"type": "ACCOUNT_UPDATE", "data": data})

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    # Send immediate current state upon connection
    await websocket.send_json({"type": "MARKET_UPDATE", "data": trading_bot.get_dashboard_data()})
    await websocket.send_json({"type": "ACCOUNT_UPDATE", "data": trading_bot.get_account_data()})
    try:
        while True:
            # Keep connection alive, can handle client messages here if needed
            data = await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

class AnalyzeRequest(BaseModel):
    ticker: str
    price: float
    rsi: float

class TestNewsRequest(BaseModel):
    title: str
    content: str = "Test Content"

@app.post("/api/test-news")
async def test_news(req: TestNewsRequest):
    # Bypassing the scraper/gatekeeper for pure MiroFish mock
    await news_radar.trigger_mirofish(req.title, req.content)
    return {"status": "success", "message": f"Test news sent to Radar: {req.title}"}

class TestWhaleRequest(BaseModel):
    btc_amount: float

@app.post("/api/test-whale")
async def test_whale(req: TestWhaleRequest):
    logger.info(f"TEST: Simulating Whale Alert for {req.btc_amount} BTC")
    # Triggering the logic directly
    if trading_bot.active_positions.get('BTC/USDT:USDT') != 'SHORT':
        current_price = trading_bot.latest_data.get('BTC/USDT:USDT', {}).get('price', 0)
        if current_price > 0:
            trading_bot.active_positions['BTC/USDT:USDT'] = 'SHORT'
            asyncio.create_task(trading_bot.execute_order('BTC/USDT:USDT', 'sell', current_price, is_black_swan=True))
            return {"status": "success", "message": f"Whale signal triggered for {req.btc_amount} BTC"}
    return {"status": "ignored", "message": "Symbol not ready or already in SHORT"}

@app.post("/api/test-liquidation")
async def test_liquidation():
    logger.info("TEST: Simulating Liquidation Alert")
    # Simulating a signal from LiquidationHunter
    # For now we just log it as the bot doesn't have a direct liquidation trigger yet beyond logs
    return {"status": "success", "message": "Liquidation signal logged"}

@app.post("/api/analyze")
async def analyze_ticker(req: AnalyzeRequest):
    api_key = os.getenv("LLM_API_KEY")
    base_url = os.getenv("LLM_BASE_URL", "https://api.openai.com/v1")
    model_name = os.getenv("LLM_MODEL_NAME", "gpt-4-turbo")
    
    if not api_key or api_key == "YOUR_LLM_KEY":
        return {
            "status": "warning",
            "message": "LLM_API_KEY non configurata o non valida in .env.",
            "analysis": f"Simulazione Backend per {req.ticker}: Al prezzo di {req.price} e con RSI a {req.rsi}, il mercato è in attesa di una tua direttiva reale."
        }
    
    logger.info(f"LLM Call: model={model_name}, base_url={base_url}")
    client = AsyncOpenAI(
        api_key=api_key,
        base_url=base_url
    )
    
    prompt = f"""
    Sei un analista finanziario esperto in criptovalute. 
    Analizza brevemente la situazione attuale per il ticker {req.ticker}.
    
    DATI ATTUALI:
    - Prezzo Snapshot: ${req.price}
    - RSI (14 periodi su candele da 15m): {req.rsi}
    
    REGOLE:
    1. Sii conciso e diretto (massimo 4-5 frasi).
    2. Spiega se l'RSI indica una zona di ipercomprato (>70), ipervenduto (<30) o neutrale.
    3. Fornisci una "Strategia Consigliata" tra: LONG, SHORT, o ATTESA (Wait).
    4. Usa un tono professionale da terminale finanziario.
    """
    
    try:
        response = await client.chat.completions.create(
            model=model_name,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7,
            max_tokens=250
        )
        
        analysis_text = response.choices[0].message.content
        
        return {
            "status": "success",
            "message": "Analisi generata da Gemini",
            "analysis": analysis_text
        }
    except Exception as e:
        return {
            "status": "error",
            "message": "Errore durante la chiamata Gemini",
            "analysis": f"Si è verificato un errore: {str(e)}"
        }

from typing import Optional

@app.get("/api/ping")
async def ping():
    return {"status": "ok", "message": "pong"}

@app.get("/api/history")
async def get_history(start: Optional[str] = None, end: Optional[str] = None):
    if not trading_bot or not trading_bot.db:
        return {"status": "error", "message": "Bot non inizializzato"}
    
    trades = trading_bot.db.get_trades(start_date=start, end_date=end)
    
    total_pnl = sum(float(t.get('pnl', 0) or 0) for t in trades)
    
    return {
        "status": "success",
        "count": len(trades),
        "total_pnl": round(total_pnl, 2),
        "trades": trades
    }

frontend_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "frontend")

@app.get("/")
async def get_index():
    return FileResponse(os.path.join(frontend_path, "index.html"))

if __name__ == "__main__":
    import uvicorn
    import traceback
    
    try:
        # Final check: Testnet connectivity with clean keys
        # Multi-Reload is disabled to prevent infinite loops when config files are auto-tuned
        uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
    except Exception as e:
        # CRITICAL: Capture the ultimate crash that takes the server down
        with open("crash.log", "a") as f:
            f.write(f"\n--- CRASH AT {asyncio.get_event_loop().time()} ---\n")
            f.write(traceback.format_exc())
            f.write("\n")
        logger.critical(f"FATAL SERVER CRASH: {e}")
        raise e
