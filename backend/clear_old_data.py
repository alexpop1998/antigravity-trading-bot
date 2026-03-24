import sqlite3
import os

DB_PATH = "/Users/alex/.gemini/antigravity/scratch/trading-terminal/backend/bot_data.db"

def clear_old_history(cutoff_time="2026-03-23 21:00:00"):
    if not os.path.exists(DB_PATH):
        print(f"Database not found at {DB_PATH}")
        return
        
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Check what we are deleting first
    cursor.execute("SELECT COUNT(*), SUM(pnl) FROM trade_history WHERE timestamp < ?", (cutoff_time,))
    row = cursor.fetchone()
    count_before = row[0]
    pnl_before = row[1] or 0.0
    
    print(f"Found {count_before} records before {cutoff_time} with total PnL: {pnl_before:.2f}")
    
    if count_before > 0:
        cursor.execute("DELETE FROM trade_history WHERE timestamp < ?", (cutoff_time,))
        print(f"Successfully deleted {cursor.rowcount} records.")
    else:
        print("No old records found to delete.")
    
    conn.commit()
    
    # Check remaining records
    cursor.execute("SELECT COUNT(*), SUM(pnl) FROM trade_history")
    row = cursor.fetchone()
    print(f"Remaining records: {row[0]}, Current Total PnL: {row[1] or 0.0:.2f}")
    
    # Vacuum the database to reclaim space
    cursor.execute("VACUUM")
    conn.commit()
    conn.close()

if __name__ == "__main__":
    clear_old_history()
