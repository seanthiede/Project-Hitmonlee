from pathlib import Path
import sqlite3
import pandas as pd
import logging

RAW_DIR = Path("raw")
DB_DIR = Path("db")
DB_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DB_DIR / "nfl.db"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("ingest")

def _safe_read(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)
    if path.suffix == ".parquet":
        return pd.read_parquet(path)
    else:
        return pd.read_csv(path)
    
def normalize_players(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # vereinheitliche Spaltennamen
    df.columns = [str(c).strip() for c in df.colums]
    # mögliche Spaltenumbennenungen (falls API unterschiedliche Namen nutzt)
    rename_map = {}

    for candidate in ["player_id", "id", "playerID"]:
        if candidate in df.columns and "player_id" not in df.columns:
            rename_map[candidate] = "player_id"
        
    if "full_name" not in df.columns:
        for c in ["fullName", "name"]:
            if c in df.columns:
                rename_map[c] = "full_name"
    
    df = df.rename(columns=rename_map)
    # falls birthdate vorhanden -> parse
    if "birthdate" in df.columns:
        df["birthdate"] = pd.to_datetime(df["birthdate"], errors="coerce").dt.date
    
    return df

def normalize_gamelogs(df: pd.DataFrame) -> pd.DataFrame:
    pass