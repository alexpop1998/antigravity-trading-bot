import sqlite3
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DBCleanup")

def cleanup():
    db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bot_data.db")
    if not os.path.exists(db_path):
        logger.error(f"Database not found at {db_path}")
        return

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 1. Clear Trade History
        logger.info("🧹 Clearing trade_history...")
        cursor.execute("DELETE FROM trade_history")
        
        # 2. Reset Bot State (Initial balance, etc.)
        # We want to reset the circuit breaker and start fresh
        logger.info("🧹 Resetting bot_state keys...")
        cursor.execute("DELETE FROM bot_state WHERE key IN ('initial_wallet_balance', 'initial_equity')")
        
        conn.commit()
        conn.close()
        logger.info("✅ Database cleaned successfully. Only new strategy results will be recorded.")
    except Exception as e:
        logger.error(f"❌ Error during cleanup: {e}")

if __name__ == "__main__":
    cleanup()
