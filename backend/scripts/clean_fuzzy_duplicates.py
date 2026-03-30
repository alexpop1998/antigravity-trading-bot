import sqlite3
import os

def clean_fuzzy_duplicates():
    db_path = os.path.join(os.path.dirname(__file__), "..", "bot_data.db")
    print(f"Connecting to database: {db_path}")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Step 1: Find fuzzy duplicates
    # We group by symbol, uppercase(side), date, rounded amount, and rounded PnL
    cursor.execute("""
        SELECT MIN(id) as keep_id, 
               symbol, UPPER(side) as u_side, 
               substr(timestamp, 1, 15) as time_prefix, 
               ROUND(pnl, 2) as r_pnl, 
               ROUND(amount, 2) as r_amount,
               COUNT(*) as c
        FROM trade_history
        GROUP BY symbol, u_side, time_prefix, r_pnl, r_amount
        HAVING c > 1
    """)
    duplicate_groups = cursor.fetchall()
    
    total_deleted = 0
    for group in duplicate_groups:
        keep_id = group[0]
        symbol = group[1]
        u_side = group[2]
        time_prefix = group[3]
        r_pnl = group[4]
        r_amount = group[5]
        
        # Delete all other rows in this group
        cursor.execute("""
            DELETE FROM trade_history 
            WHERE id != ? 
            AND symbol = ? 
            AND UPPER(side) = ? 
            AND substr(timestamp, 1, 15) = ? 
            AND ROUND(pnl, 2) = ? 
            AND ROUND(amount, 2) = ?
        """, (keep_id, symbol, u_side, time_prefix, r_pnl, r_amount))
        
        deleted = cursor.rowcount
        total_deleted += deleted
        print(f"Deleted {deleted} fuzzy duplicates for {symbol} ({u_side}) around {time_prefix}")

    conn.commit()
    conn.close()
    print(f"Cleanup complete. Total fuzzy duplicates deleted: {total_deleted}")

if __name__ == "__main__":
    clean_fuzzy_duplicates()
