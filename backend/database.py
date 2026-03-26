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
                reason TEXT,
                exchange_trade_id TEXT UNIQUE
            )
        ''')

        # Nuova Tabella: AI Memory (per addestramento LLM in-context)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ai_memory (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                symbol TEXT,
                signal_type TEXT,
                side TEXT,
                indicators_json TEXT,
                entry_price REAL,
                exit_price REAL,
                pnl REAL,
                outcome TEXT, -- 'WIN', 'LOSS', 'PARTIAL'
                trade_id TEXT
            )
        ''')
        
        # Add exchange_trade_id column to existing table if it doesn't exist
        try:
            cursor.execute("ALTER TABLE trade_history ADD COLUMN exchange_trade_id TEXT UNIQUE")
        except sqlite3.OperationalError:
            pass # Column already exists
            
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

    def log_trade(self, symbol, side, price, amount, pnl=0, reason="", exchange_trade_id=None):
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
                INSERT INTO trade_history (symbol, side, price, amount, pnl, reason, exchange_trade_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (symbol, side, price, amount, pnl, reason, exchange_trade_id))
            self.conn.commit()
        except sqlite3.IntegrityError:
            pass # Ignoriamo duplicati basati su exchange_trade_id
        except Exception as e:
            logger.error(f"Errore log trade: {e}")

    def save_trade_snapshot(self, symbol, signal_type, side, entry_price, indicators):
        """Salva uno snapshot dello stato del mercato al momento dell'ingresso."""
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
                INSERT INTO ai_memory (symbol, signal_type, side, entry_price, indicators_json)
                VALUES (?, ?, ?, ?, ?)
            ''', (symbol, signal_type, side, entry_price, json.dumps(indicators)))
            snapshot_id = cursor.lastrowid
            self.conn.commit()
            return snapshot_id
        except Exception as e:
            logger.error(f"Errore salvataggio snapshot AI: {e}")
            return None

    def update_trade_outcome(self, snapshot_id, exit_price, pnl):
        """Aggiorna lo snapshot con l'esito finale del trade."""
        if snapshot_id is None: return
        try:
            outcome = 'WIN' if pnl > 0 else 'LOSS'
            cursor = self.conn.cursor()
            cursor.execute('''
                UPDATE ai_memory 
                SET exit_price = ?, pnl = ?, outcome = ?
                WHERE id = ?
            ''', (exit_price, pnl, outcome, snapshot_id))
            self.conn.commit()
        except Exception as e:
            logger.error(f"Errore aggiornamento esito AI: {e}")

    def get_similar_memories(self, symbol, limit=5):
        """Recupera gli ultimi trade simili per fornire contesto all'LLM."""
        try:
            cursor = self.conn.cursor()
            # Priorità ai trade dello stesso simbolo, poi agli altri
            cursor.execute('''
                SELECT * FROM ai_memory 
                WHERE outcome IS NOT NULL 
                ORDER BY CASE WHEN symbol = ? THEN 0 ELSE 1 END, timestamp DESC 
                LIMIT ?
            ''', (symbol, limit))
            return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Errore recupero memorie AI: {e}")
            return []
            
    def sync_binance_trades(self, trades_list):
        """Syncs an array of CCXT trade objects to the local SQLite database"""
        if not trades_list:
            return
            
        from datetime import datetime, timezone
        
        try:
            cursor = self.conn.cursor()
            inserted_count = 0
            
            for t in trades_list:
                # Basic CCXT trade properties
                exchange_trade_id = t.get('id')
                if not exchange_trade_id:
                    continue
                    
                symbol = t.get('symbol', 'UNKNOWN')
                side = t.get('side', 'unknown')
                price = float(t.get('price', 0) or 0)
                amount = float(t.get('amount', 0) or 0)
                
                # Realized PnL is usually inside trade.info on Binance Futures
                pnl = 0.0
                info = t.get('info', {})
                if 'realizedPnl' in info:
                    pnl = float(info['realizedPnl'])
                    
                timestamp_ms = t.get('timestamp')
                dt_str = None
                if timestamp_ms:
                    # Convert to YYYY-MM-DD HH:MM:SS string
                    dt = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
                    dt_str = dt.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    continue # Skip trades without time
                
                try:
                    # Usiamo INSERT OR REPLACE per assicurarci che i dati di Binance (il "fatto") 
                    # sovrascrivano eventuali log manuali del bot (l'"intento") per lo stesso trade_id.
                    cursor.execute('''
                        INSERT OR REPLACE INTO trade_history 
                        (timestamp, symbol, side, price, amount, pnl, reason, exchange_trade_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (dt_str, symbol, side, price, amount, pnl, "BINANCE", exchange_trade_id))
                    
                    if cursor.rowcount > 0:
                        inserted_count = inserted_count + 1
                except Exception as row_error:
                    logger.error(f"Error inserting individual trade {exchange_trade_id}: {row_error}")
                    
            self.conn.commit()
            if inserted_count > 0:
                logger.info(f"🔄 Sincronizzati {inserted_count} storici trade da Binance al database locale.")
        except Exception as e:
            logger.error(f"Errore durante sync_binance_trades: {e}")

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
