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
                pnl_pct REAL,
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
        
        try:
            cursor.execute("ALTER TABLE trade_history ADD COLUMN pnl_pct REAL")
        except sqlite3.OperationalError:
            pass # Colonna già esistente
            
        try:
            cursor.execute("ALTER TABLE trade_history ADD COLUMN real_price REAL")
        except sqlite3.OperationalError:
            pass # Colonna già esistente
            
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

    def log_trade(self, symbol, side, price, amount, pnl=0, pnl_pct=0, reason="", exchange_trade_id=None, real_price=None):
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
                INSERT INTO trade_history (symbol, side, price, amount, pnl, pnl_pct, reason, exchange_trade_id, real_price)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (symbol, side, price, amount, pnl, pnl_pct, reason, exchange_trade_id, real_price))
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
            
    def get_ai_performance_stats(self, hours=24):
        """
        Calcola statistiche aggregate per settore e segnale negli ultimi periodi.
        Restituisce un prompt strutturato per Gemini.
        """
        try:
            cursor = self.conn.cursor()
            # Statistiche per Segnale (TECH vs AI vs NEWS) nelle ultime 24 ore
            cursor.execute('''
                SELECT signal_type, 
                       COUNT(*) as total,
                       SUM(CASE WHEN outcome = 'WIN' THEN 1 ELSE 0 END) as wins,
                       AVG(pnl) as avg_pnl
                FROM ai_memory 
                WHERE timestamp >= datetime('now', '-' || ? || ' hours')
                AND outcome IS NOT NULL
                GROUP BY signal_type
            ''', (hours,))
            rows = cursor.fetchall()
            
            stats = {}
            for row in rows:
                sig = row['signal_type']
                total = row['total']
                stats[sig] = {
                    'accuracy': (row['wins'] / total) * 100 if total > 0 else 0,
                    'avg_pnl': row['avg_pnl'] or 0,
                    'sample_size': total
                }
            return stats
        except Exception as e:
            logger.error(f"Errore calcolo statistiche AI: {e}")
            return {}
            
    def sync_binance_trades(self, trades_list, min_timestamp_ms=0):
        """
        Syncs an array of CCXT trade objects to the local SQLite database.
        v10.4: Added min_timestamp_ms to prevent old data from sync-locking the session.
        """
        if not trades_list:
            return
            
        from datetime import datetime, timezone
        
        try:
            cursor = self.conn.cursor()
            inserted_count = 0
            aggregated_trades = {}
            for t in trades_list:
                exchange_trade_id = t.get('id')
                order_id = t.get('order') # v9.9.5: Usiamo OrderID per un raggruppamento infallibile
                if not exchange_trade_id:
                    continue
                    
                symbol = t.get('symbol', 'UNKNOWN')
                side = t.get('side', 'unknown').upper()
                price = float(t.get('price', 0) or 0)
                amount = float(t.get('amount', 0) or 0)
                timestamp_ms = t.get('timestamp')
                
                if not timestamp_ms or timestamp_ms < min_timestamp_ms:
                    # Skip trades before the current session start to avoid ghosting
                    continue
                
                dt_str = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                
                # --- [REFINED] Raggruppamento Fills (v10.5) ---
                # Usiamo OrderID per raggruppare i parziali. Se OrderID è mancante, 
                # usiamo il timestamp al secondo (dt_str) per evitare duplicazioni nello stesso istante.
                key = f"order_{order_id}" if order_id else f"time_{dt_str}_{symbol}_{side}"
                
                if key not in aggregated_trades:
                    aggregated_trades[key] = {
                        'timestamp': dt_str,
                        'symbol': symbol,
                        'side': side,
                        'total_price_volume': price * amount,
                        'total_amount': amount,
                        'total_pnl': 0,
                        'exchange_trade_id': exchange_trade_id, 
                        'order_id': order_id
                    }
                else:
                    aggregated_trades[key]['total_price_volume'] += (price * amount)
                    aggregated_trades[key]['total_amount'] += amount
                
                # Somma PnL
                info = t.get('info', {})
                pnl_fields = ['realizedPnl', 'pnl', 'profit', 'realized_pnl', 'income']
                for field in pnl_fields:
                    if field in info and info[field] is not None:
                        aggregated_trades[key]['total_pnl'] += float(info[field])
                        break

            for key, data in aggregated_trades.items():
                avg_price = data['total_price_volume'] / data['total_amount'] if data['total_amount'] > 0 else 0
                
                try:
                    # v10.2: Calculate PnL % (ROI) during sync
                    cost = avg_price * data['total_amount']
                    pnl_pct = (data['total_pnl'] / cost * 100) if cost > 0 else 0
                    
                    # Usiamo l'ID del primo fill come chiave unica nel DB
                    # --- [REFINED] Deduplicazione Preventiva (v10.5) ---
                    # Verifichiamo se esiste già un trade identico (stesso tempo, simbolo, side) 
                    # per evitare duplicati logici se Binance non restituisce OrderID o IDs coerenti.
                    cursor.execute('''
                        SELECT 1 FROM trade_history 
                        WHERE timestamp = ? AND symbol = ? AND side = ? AND amount = ?
                    ''', (data['timestamp'], data['symbol'], data['side'], data['total_amount']))
                    
                    if cursor.fetchone():
                        # Già presente, aggiorniamo solo il PnL se è diverso da 0
                        if data['total_pnl'] != 0:
                            cursor.execute('''
                                UPDATE trade_history SET pnl = ?, pnl_pct = ? 
                                WHERE timestamp = ? AND symbol = ? AND side = ? AND pnl = 0
                            ''', (data['total_pnl'], pnl_pct, data['timestamp'], data['symbol'], data['side']))
                    else:
                        cursor.execute('''
                            INSERT OR IGNORE INTO trade_history 
                            (timestamp, symbol, side, price, amount, pnl, pnl_pct, reason, exchange_trade_id)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (data['timestamp'], data['symbol'], data['side'], avg_price, data['total_amount'], data['total_pnl'], pnl_pct, "BINANCE_SYNC", data['exchange_trade_id']))
                        if cursor.rowcount > 0:
                            inserted_count += 1
                except Exception as row_error:
                    logger.error(f"Error syncing individual group {data['exchange_trade_id']}: {row_error}")
                    
            self.conn.commit()
            if inserted_count > 0:
                logger.info(f"🔄 Sincronizzati {inserted_count} storici trade da Binance al database locale.")
        except Exception as e:
            logger.error(f"Errore durante sync_binance_trades: {e}")

    def get_trades(self, start_date=None, end_date=None, limit=None):
        try:
            cursor = self.conn.cursor()
            query = "SELECT * FROM trade_history"
            params = []
            if start_date and end_date:
                query += " WHERE timestamp BETWEEN ? AND ?"
                params = [start_date + " 00:00:00", end_date + " 23:59:59"]
            query += " ORDER BY timestamp DESC"
            if limit:
                query += " LIMIT ?"
                params.append(limit)
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
