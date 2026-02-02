#!/usr/bin/env python3
"""
Aggregations-Script für Smart Energy Pi
Läuft als Cronjob (empfohlen: jede Stunde)

Aufgaben:
1. Rohdaten älter als 48h zu Minuten-Werten aggregieren
2. Minuten-Daten älter als 7 Tage zu Stunden-Werten aggregieren  
3. Stunden-Daten älter als 90 Tage zu Tages-Werten aggregieren
4. Alte Daten löschen gemäß Retention Policy
"""
import sqlite3
from datetime import datetime, timedelta
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(SCRIPT_DIR, ".env"))
except ImportError:
    pass

_db_env = os.getenv("DB_PATH", "instance/energy.db")
DB_PATH = _db_env if os.path.isabs(_db_env) else os.path.join(SCRIPT_DIR, _db_env)

def aggregate():
    if not os.path.exists(DB_PATH):
        print(f"DB not found: {DB_PATH}")
        return
    
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    now = datetime.now()
    
    cur.execute("""CREATE TABLE IF NOT EXISTS measurement_minute (
        id INTEGER PRIMARY KEY, timestamp DATETIME, 
        power_avg FLOAT, power_max FLOAT, power_min FLOAT, total_kwh FLOAT)""")
    cur.execute("""CREATE TABLE IF NOT EXISTS measurement_hour (
        id INTEGER PRIMARY KEY, timestamp DATETIME, 
        power_avg FLOAT, power_max FLOAT, power_min FLOAT, kwh_used FLOAT)""")
    cur.execute("""CREATE TABLE IF NOT EXISTS measurement_day (
        id INTEGER PRIMARY KEY, timestamp DATETIME, 
        power_avg FLOAT, power_max FLOAT, power_min FLOAT, kwh_used FLOAT)""")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_mm_ts ON measurement_minute(timestamp)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_mh_ts ON measurement_hour(timestamp)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_md_ts ON measurement_day(timestamp)")

    
    cutoff_48h = now - timedelta(hours=48)
    cutoff_7d = now - timedelta(days=7)
    cutoff_90d = now - timedelta(days=90)
    
    # 1. Raw -> Minuten (älter als 48h)
    cur.execute("""
        INSERT INTO measurement_minute (timestamp, power_avg, power_max, power_min, total_kwh)
        SELECT 
            datetime(strftime('%Y-%m-%d %H:%M:00', timestamp)),
            AVG(power_watt), MAX(power_watt), MIN(power_watt), MAX(total_kwh)
        FROM measurement
        WHERE timestamp < ?
        AND datetime(strftime('%Y-%m-%d %H:%M:00', timestamp)) NOT IN 
            (SELECT timestamp FROM measurement_minute)
        GROUP BY strftime('%Y-%m-%d %H:%M', timestamp)
    """, (cutoff_48h.isoformat(),))
    rows_min = cur.rowcount
    
    # 2. Minuten -> Stunden (älter als 7 Tage)
    cur.execute("""
        INSERT INTO measurement_hour (timestamp, power_avg, power_max, power_min, kwh_used)
        SELECT 
            datetime(strftime('%Y-%m-%d %H:00:00', timestamp)),
            AVG(power_avg), MAX(power_max), MIN(power_min),
            MAX(total_kwh) - MIN(total_kwh)
        FROM measurement_minute
        WHERE timestamp < ?
        AND datetime(strftime('%Y-%m-%d %H:00:00', timestamp)) NOT IN 
            (SELECT timestamp FROM measurement_hour)
        GROUP BY strftime('%Y-%m-%d %H', timestamp)
    """, (cutoff_7d.isoformat(),))
    rows_hour = cur.rowcount
    
    # 3. Stunden -> Tage (älter als 90 Tage)
    cur.execute("""
        INSERT INTO measurement_day (timestamp, power_avg, power_max, power_min, kwh_used)
        SELECT 
            datetime(strftime('%Y-%m-%d 00:00:00', timestamp)),
            AVG(power_avg), MAX(power_max), MIN(power_min), SUM(kwh_used)
        FROM measurement_hour
        WHERE timestamp < ?
        AND datetime(strftime('%Y-%m-%d 00:00:00', timestamp)) NOT IN 
            (SELECT timestamp FROM measurement_day)
        GROUP BY strftime('%Y-%m-%d', timestamp)
    """, (cutoff_90d.isoformat(),))
    rows_day = cur.rowcount

    
    # 4. Cleanup: Alte Rohdaten löschen (älter als 48h)
    cur.execute("DELETE FROM measurement WHERE timestamp < ?", (cutoff_48h.isoformat(),))
    deleted_raw = cur.rowcount
    
    # 5. Cleanup: Alte Minuten-Daten löschen (älter als 7 Tage)
    cur.execute("DELETE FROM measurement_minute WHERE timestamp < ?", (cutoff_7d.isoformat(),))
    deleted_min = cur.rowcount
    
    # 6. Cleanup: Alte Stunden-Daten löschen (älter als 90 Tage)
    cur.execute("DELETE FROM measurement_hour WHERE timestamp < ?", (cutoff_90d.isoformat(),))
    deleted_hour = cur.rowcount
    
    conn.commit()
    conn.close()
    
    print(f"[{now.isoformat()}] Aggregation done:")
    print(f"  + Minuten-Einträge: {rows_min}")
    print(f"  + Stunden-Einträge: {rows_hour}")
    print(f"  + Tages-Einträge: {rows_day}")
    print(f"  - Rohdaten gelöscht: {deleted_raw}")
    print(f"  - Minuten gelöscht: {deleted_min}")
    print(f"  - Stunden gelöscht: {deleted_hour}")

if __name__ == "__main__":
    aggregate()
