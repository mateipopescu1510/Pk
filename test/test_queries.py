import sqlite3
import pandas as pd

conn = sqlite3.connect('data/pk.db')

hits = pd.read_sql("""
    SELECT 
        run_id, missile_name, t0, range_lt, alt_l, mach_l, alt_t, mach_t, hit
    FROM shots
    WHERE hit = 1 AND missile_name IN ('AIM_120', 'AIM_120C')
    ORDER BY range_lt DESC
    LIMIT 6
""", conn)

missiles = pd.read_sql("""
    SELECT 
        COUNT(*) AS count, missile_name, guidance
    FROM shots
    GROUP BY missile_name
    ORDER BY count DESC                    
""", conn)

# print(hits.to_string(index=False))

print(missiles.to_string(index=False))


conn.close()
