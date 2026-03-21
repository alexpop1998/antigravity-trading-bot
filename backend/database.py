import sqlite3
import json
import logging
import os

logger = logging.getLogger("Database")

class BotDatabase:
    def __init__(self, db_path="bot_data.db"):
        self.db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), db_path)
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self):
        cursor = self.conn.cursor()
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
        self.conn.commit()

    def save_state(self, key, data):
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "INSERT OR REPLACE INTO bot_state (key, value) VALUES (?, ?)",
                (key, json.dumps(data))
            )
            self.conn.commit()
        except Exception as e:
            logger.error(f"Errore salvataggio database: {e}")

    def load_state(self, key):
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT value FROM bot_state WHERE key = ?", (key,))
            row = cursor.fetchone()
            return json.loads(row[0]) if row else None
        except Exception as e:
            logger.error(f"Errore caricamento database: {e}")
            return None

    def log_trade(self, symbol, side, price, amount, pnl=0, reason=""):
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
                INSERT INTO trade_history (symbol, side, price, amount, pnl, reason)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (symbol, side, price, amount, pnl, reason))
            self.conn.commit()
        except Exception as e:
            logger.error(f"Errore log trade: {e}")

    def get_trades(self, start_date=None, end_date=None):
        try:
            cursor = self.conn.cursor()
            query = "SELECT * FROM trade_history"
            params = []
            if start_date and end_date:
                query += " WHERE timestamp BETWEEN ? AND ?"
                params = [start_date + " 00:00:00", end_date + " 23:59:59"]
            query += " ORDER BY timestamp DESC"
            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Errore recupero trades: {e}")
            return []
    
    def close(self):
        """Cleanly close the database connection."""
        try:
            self.conn.close()
            logger.info("Database connection closed.")
        except Exception as e:
            logger.error(f"Error closing database: {e}")
