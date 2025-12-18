import sqlite3
import pandas as pd
from pathlib import Path

DB_PATH = Path("db/nfl.db")

if DB_PATH.exists():
    conn = sqlite3.connect(DB_PATH)
    # Listet alle Tabellen in der Datenbank auf
    tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn)
    print("Tabellen in der DB:", tables['name'].tolist())
    
    for table in tables['name'].tolist():
        count = pd.read_sql(f"SELECT COUNT(*) as anz FROM {table}", conn)
        print(f"Einträge in '{table}': {count['anz'][0]}")
    conn.close()
else:
    print("Datenbank-Datei existiert gar nicht!")