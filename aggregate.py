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
        power_avg FLOAT, power_max FLOAT, power_min FLOAT, kwh_used FLOAT, total_kwh FLOAT)""")
    cur.execute("""CREATE TABLE IF NOT EXISTS measurement_day (
        id INTEGER PRIMARY KEY, timestamp DATETIME, 
        power_avg FLOAT, power_max FLOAT, power_min FLOAT, kwh_used FLOAT, total_kwh FLOAT)""")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_mm_ts ON measurement_minute(timestamp)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_mh_ts ON measurement_hour(timestamp)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_md_ts ON measurement_day(timestamp)")

    for tbl in ("measurement_hour", "measurement_day"):
        cols = [r[1] for r in cur.execute(f"PRAGMA table_info({tbl})").fetchall()]
        if "total_kwh" not in cols:
            cur.execute(f"ALTER TABLE {tbl} ADD COLUMN total_kwh FLOAT")

    
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
    # End-Registerstand der Stunde speichern; kwh_used = Differenz zum
    # vorherigen vorhandenen Registerstand (lückenlos, kein Grenzverlust).
    cur.execute("""
        INSERT INTO measurement_hour (timestamp, power_avg, power_max, power_min, kwh_used, total_kwh)
        SELECT 
            datetime(strftime('%Y-%m-%d %H:00:00', timestamp)),
            AVG(power_avg), MAX(power_max), MIN(power_min),
            NULL,
            MAX(total_kwh)
        FROM measurement_minute
        WHERE timestamp < ?
        AND datetime(strftime('%Y-%m-%d %H:00:00', timestamp)) NOT IN 
            (SELECT timestamp FROM measurement_hour)
        GROUP BY strftime('%Y-%m-%d %H', timestamp)
    """, (cutoff_7d.isoformat(),))
    rows_hour = cur.rowcount
    cur.execute("""
        UPDATE measurement_hour AS h SET kwh_used = h.total_kwh - (
            SELECT p.total_kwh FROM measurement_hour AS p
            WHERE p.timestamp < h.timestamp AND p.total_kwh IS NOT NULL
            ORDER BY p.timestamp DESC LIMIT 1)
        WHERE h.kwh_used IS NULL AND h.total_kwh IS NOT NULL
        AND EXISTS (SELECT 1 FROM measurement_hour AS p
            WHERE p.timestamp < h.timestamp AND p.total_kwh IS NOT NULL)
    """)
    
    # 3. Stunden -> Tage (älter als 90 Tage)
    cur.execute("""
        INSERT INTO measurement_day (timestamp, power_avg, power_max, power_min, kwh_used, total_kwh)
        SELECT 
            datetime(strftime('%Y-%m-%d 00:00:00', timestamp)),
            AVG(power_avg), MAX(power_max), MIN(power_min),
            NULL,
            MAX(total_kwh)
        FROM measurement_hour
        WHERE timestamp < ?
        AND datetime(strftime('%Y-%m-%d 00:00:00', timestamp)) NOT IN 
            (SELECT timestamp FROM measurement_day)
        GROUP BY strftime('%Y-%m-%d', timestamp)
    """, (cutoff_90d.isoformat(),))
    rows_day = cur.rowcount
    cur.execute("""
        UPDATE measurement_day AS d SET kwh_used = d.total_kwh - (
            SELECT p.total_kwh FROM measurement_day AS p
            WHERE p.timestamp < d.timestamp AND p.total_kwh IS NOT NULL
            ORDER BY p.timestamp DESC LIMIT 1)
        WHERE d.kwh_used IS NULL AND d.total_kwh IS NOT NULL
        AND EXISTS (SELECT 1 FROM measurement_day AS p
            WHERE p.timestamp < d.timestamp AND p.total_kwh IS NOT NULL)
    """)

    
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
