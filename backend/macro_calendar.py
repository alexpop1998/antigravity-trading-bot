import asyncio
import logging
import httpx
from bs4 import BeautifulSoup
from datetime import datetime
from dateutil import parser
import pytz

logger = logging.getLogger("MacroCalendar")

class MacroCalendar:
    def __init__(self, bot_instance, http_client=None, update_interval=3600):
        self.bot = bot_instance
        self.http_client = http_client # Shared client from main.py
        self.update_interval = update_interval
        # Using ForexFactory free XML feed for economic events
        self.calendar_url = "https://nfs.faireconomy.media/ff_calendar_thisweek.xml"
        self.high_impact_events = []
        self.next_event = None

    async def fetch_calendar(self):
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
            }
            client = self.http_client if self.http_client else httpx.AsyncClient(headers=headers)
            response = await client.get(self.calendar_url, timeout=15.0)
            if response.status_code == 429:
                logger.warning("ForexFactory 429: Too Many Requests. Retrying in 5 seconds...")
                await asyncio.sleep(5)
                response = await client.get(self.calendar_url, timeout=15.0)
                
                if response.status_code != 200:
                    logger.error(f"Failed to fetch macroeconomic calendar: {response.status_code}")
                    return

                # Parse XML
                soup = BeautifulSoup(response.text, "xml")
                events = soup.find_all("event")
                
                self.high_impact_events.clear()
                
                for event in events:
                    impact = event.impact.text.strip()
                    country = event.country.text.strip()
                    
                    # We only care about HIGH impact USD events (like FOMC, CPI, NFP)
                    if impact == "High" and country == "USD":
                        title = event.title.text.strip()
                        date_str = event.date.text.strip()
                        time_str = event.time.text.strip()
                        
                        # ForexFactory standard format: date = MM-DD-YYYY, time = 02:00pm
                        try:
                            # Parse the time in Eastern Time (US/Eastern) which FF usually provides, 
                            # or just treat it as EST/EDT. The XML feed usually outputs in the timezone 
                            # of the requester or Eastern. Let's assume EST.
                            dt_str = f"{date_str} {time_str}"
                            event_dt = parser.parse(dt_str)
                            
                            # Standardize to timezone-aware UTC. 
                            # FF XML defaults to Eastern Time.
                            eastern = pytz.timezone('US/Eastern')
                            loc_dt = eastern.localize(event_dt)
                            utc_dt = loc_dt.astimezone(pytz.utc)
                            
                            # Only keep future events
                            if utc_dt > datetime.now(pytz.utc):
                                self.high_impact_events.append({
                                    "title": title,
                                    "time": utc_dt
                                })
                        except Exception as e:
                            logger.error(f"Error parsing date {date_str} {time_str}: {e}")
                
                # Sort by upcoming time
                self.high_impact_events.sort(key=lambda x: x["time"])
                
                if self.high_impact_events:
                    self.next_event = self.high_impact_events[0]
                    logger.info(f"📅 Next High-Impact USD Event: '{self.next_event['title']}' at {self.next_event['time']} UTC")
                else:
                    self.next_event = None
                    logger.info("📅 No upcoming High-Impact USD Events this week.")
                    
        except Exception as e:
            logger.error(f"Exception in fetch_calendar: {e}")

    async def monitor_calendar(self):
        logger.info("Starting MacroEconomic Calendar monitor...")
        
        # Initial fetch
        await self.fetch_calendar()
        
        last_fetch = datetime.now(pytz.utc)
        
        while True:
            now = datetime.now(pytz.utc)
            
            # Re-fetch calendar periodically
            if (now - last_fetch).total_seconds() > self.update_interval:
                await self.fetch_calendar()
                last_fetch = now
                
            if self.next_event:
                time_till_event = (self.next_event["time"] - now).total_seconds()
                
                # Volatility Shield: 15 mins before to 5 mins after
                if -300 <= time_till_event <= 900:  # 900s = 15m before, -300s = 5m after
                    if not self.bot.is_macro_paused:
                        logger.warning(f"🚨 MACRO VOLATILITY SHIELD ACTIVE: '{self.next_event['title']}'. Reducing sizing (0.5%) and widening SL (5x ATR).")
                        self.bot.is_macro_paused = True # Still called is_macro_paused but logic in bot.py now handles it as a shield
                else:
                    if self.bot.is_macro_paused:
                        logger.info(f"✅ MACRO EVENT PASSED: '{self.next_event['title']}'. Resuming standard risk profile.")
                        self.bot.is_macro_paused = False
                        
                        # Pop the event so we look for the next one
                        self.high_impact_events.pop(0)
                        if self.high_impact_events:
                            self.next_event = self.high_impact_events[0]
                        else:
                            self.next_event = None
            
            await asyncio.sleep(60) # Check every minute
