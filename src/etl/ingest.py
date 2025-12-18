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
    
    # Wenn es die Players-Datei ist, laden wir sie OHNE Header
    if "players" in path.name.lower():
        logger.info(f"Erzwinge Header-Struktur für {path.name}...")
        df = pd.read_parquet(path) if path.suffix == ".parquet" else pd.read_csv(path, header=None)
        
        # Falls der Header schon als Zahlen (0, 1, 2) da ist oder Bernard in der ersten Zeile steht
        if isinstance(df.columns[0], int) or str(df.columns[0]) == '0' or "Bernard" in str(df.columns[6]):
            # Wir definieren die wichtigsten Spalten, die wir für dein Dashboard brauchen
            # Die Reihenfolge entspricht exakt deiner CSV/Screenshots
            new_cols = [
                "season", "team", "position", "pos_detail", "depth", "status", 
                "full_name", "first_name", "last_name", "birth_date", "height", 
                "weight", "college", "player_id", "gsis_id", "espn_id", 
                "sportradar_id", "yahoo_id", "rotowire_id", "pff_id", "pfr_id", 
                "fantasy_data_id", "sleeper_id", "years_exp", "headshot_url", "is_active"
            ]
            
            # Wir füllen den Rest mit "extra", falls die Datei mehr Spalten hat
            if len(df.columns) > len(new_cols):
                new_cols += [f"extra_{i}" for i in range(len(df.columns) - len(new_cols))]
            
            df.columns = new_cols[:len(df.columns)]
            
        return df

    # Normales Laden für Gamelogs (die scheinen ja zu funktionieren)
    return pd.read_parquet(path) if path.suffix == ".parquet" else pd.read_csv(path)
    
def normalize_players(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # vereinheitliche Spaltennamen
    df.columns = [str(c).strip() for c in df.columns]
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
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    rename_map = {}

    # übliche Normalisierungen
    for cand, std in [
        ("player_id", "player_id"),
        ("playerId","player_id"),
        ("player","player_id"),
        ("game_id","game_id"),
        ("gameId","game_id"),
        ("rush_yards","rushing_yards"),
        ("rush_yards","rushing_yards"),
        ("yds","yards")]:
        
        if cand in df.columns and std not in df.columns:
            rename_map[cand] = std
    
    df = df.rename(columns=rename_map)
    # numeric coercion for common stat cols
    for col in ["yards", "rushing_yards", "passing_yards", "td", "touchdowns", "week"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    
    return df

def upsert_table(df: pd.DataFrame, table_name: str, conn: sqlite3.Connection, if_exists="replace"):
    # 'replace' sorgt dafür, dass die alte (kaputte) Struktur gelöscht wird
    df.to_sql(table_name, conn, if_exists=if_exists, index=False)
    logger.info(f"Wrote {len(df)} rows into table '{table_name}'")

def ingest_season(season: int):
    conn = sqlite3.connect(DB_PATH)

    try:
        # players
        p_path = RAW_DIR / f"players_{season}.parquet"
        if not p_path.exists():
            # try csv fallback
            p_path = p_path.with_suffix(".csv")
        
        if p_path.exists():
            players = _safe_read(p_path)
            players = normalize_players(players)
            upsert_table(players, "players", conn)
        else:
            logger.warning(f"No players file for season {season}: {p_path}")
        
        # gamelogs
        g_path = RAW_DIR / f"gamelogs_{season}.parquet"
        if not g_path.exists():
            g_path = g_path.with_suffix(".csv")
        if g_path.exists():
            gamelogs = _safe_read(g_path)
            gamelogs = normalize_gamelogs(gamelogs)
            upsert_table(gamelogs, "gamelogs", conn)
        else:
            logger.warning(f"No gamelogs file for season {season}: {g_path}")
    finally:
        conn.close()
    

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Ingest raw files into sqlite db")
    parser.add_argument("--seasons", "-s", type=int, nargs="+", default=[2023])
    args = parser.parse_args()

    for s in args.seasons:
        ingest_season(s)