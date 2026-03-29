-- Clean up logical duplicates from trade_history
-- This keeps only the first entry for each identical combination of timestamp, symbol, side, amount, and price.

BEGIN TRANSACTION;

-- Show some stats before cleanup
SELECT 'Rows before cleanup:' as info, COUNT(*) FROM trade_history;

-- Create temporary table with unique rows
CREATE TEMP TABLE unique_trades AS 
SELECT MIN(id) as keep_id 
FROM trade_history 
GROUP BY timestamp, symbol, side, amount, price;

-- Delete rows not in the unique list
DELETE FROM trade_history 
WHERE id NOT IN (SELECT keep_id FROM unique_trades);

-- Show some stats after cleanup
SELECT 'Rows after cleanup:' as info, COUNT(*) FROM trade_history;

COMMIT;

-- Vacuum to shrink the database
VACUUM;
