import sqlite3
import json
import logging
import os

logger = logging.getLogger("Database")

class BotDatabase:
    def __init__(self, db_path="bot_data.db"):
        self.db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), db_path)
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            # Tabella per lo stato del bot (trade_levels)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS bot_state (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            ''')
            # Tabella per lo storico dei trade
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS trade_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    symbol TEXT,
                    side TEXT,
                    price REAL,
                    amount REAL,
                    pnl REAL,
                    reason TEXT
                )
            ''')
            conn.commit()

    def save_state(self, key, data):
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR REPLACE INTO bot_state (key, value) VALUES (?, ?)",
                    (key, json.dumps(data))
                )
                conn.commit()
        except Exception as e:
            logger.error(f"Errore salvataggio database: {e}")

    def load_state(self, key):
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT value FROM bot_state WHERE key = ?", (key,))
                row = cursor.fetchone()
                return json.loads(row[0]) if row else None
        except Exception as e:
            logger.error(f"Errore caricamento database: {e}")
            return None

    def log_trade(self, symbol, side, price, amount, pnl=0, reason=""):
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO trade_history (symbol, side, price, amount, pnl, reason)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (symbol, side, price, amount, pnl, reason))
                conn.commit()
        except Exception as e:
            logger.error(f"Errore log trade: {e}")
