"""
src/fetchers/nfl_fetcher.py

Starter-Fetcher für dein NFL-Projekt.

Funktionen:
- fetch_rosters(seasons, force=False)
- fetch_gamelogs(seasons, force=False)

Verhalten:
- Versucht, eine installierte nfl-library (z.B. `nflreadpy` oder `nfl_data_py`) zu nutzen.
  - Die genaue API der Bibliotheken kann variieren; deshalb versuchen wir verschiedene
    bekannte Funktionsnamen in Reihenfolge.
- Wenn keine passende Bibliothek gefunden oder ein Fehler auftritt, wird ein Demo-CSV / parquet
  erzeugt (sodass du weiterarbeiten kannst).
- Speichert Rohdaten in `raw/` als Parquet (einfache, effiziente Form).
- CLI: `python src/fetchers/nfl_fetcher.py --seasons 2023 2022 [--force]`

Anpassung:
- Wenn du `nflreadpy` installiert hast, öffne dieses Skript und passe die Stelle
  `# === use nflreadpy ===` an die tatsächliche API (z.B. name der Funktion).
"""

from pathlib import Path
import pandas as pd
import logging
import sys
import time

RAW_DIR = Path("raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

# set up Logging
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("nfl_fetcher")

# small helper: write parquet safely
def write_parquet(df: pd.DataFrame, path: Path):
    
    # Stelle sicher, dass alle Spaltennamen Strings sind
    df.columns = [str(col) for col in df.columns]

    # Zielordner erstellen, falls nicht vorhanden
    path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
      df.to_parquet(path, index=False)
      logger.info(f"Wrote {len(df)} rows to {path}")
    except Exception as e:
      # fallback to csv if parquet fails for any reason
      logger.warning(f"Parquet write failed ({e}); wrote CSV to {csv_path}")
      csv_path = path.with_suffix(".csv")
      df.to_csv(csv_path, index=False)
      logger.info(f"Wrote {len(df)} rows to {csv_path}")

def use_nfl_library_available():
  """
  Prüft, ob bekannte nfl-Pakete installiert sind und gibt die importierten Module zurück.
  Reihenfolge: nflreadpy, nfl_data_py
  """
  mods = {}
  try: 
    import nflreadpy as nrp
    mods['nflreadpy'] = nrp
    logger.info("Found module: nflreadpy")
  except Exception:
    logger.debug("nflreadpy not available")

  try:
    import nfl_data_py as ndp
    mods['nfl_data_py'] = ndp
    logger.info("Found module: nfl_data_py")
  except Exception:
    logger.debug("nfl_data_py not available")

  return mods

def fetch_rosters(seasons=[2023], force=False, sleep_between=0.5):
  """
    Lade Rosters/Players für die gegebenen Seasons.
    - seasons: Liste von ints
    - force: True -> überschreibe vorhandene raw-Dateien; False -> skip wenn vorhanden
  """
  mods = use_nfl_library_available()

  results = []
  for s in seasons:
    out_path = RAW_DIR / f"players_{s}.parquet"
    if out_path.exists() and not force:
      logger.info(f"{out_path} already exists -> skipping (use --force to overwrite)")
      try:
        df = pd.read_parquet(out_path)
        results.append(df)
        continue
      except Exception:
        logger.warning(f"Failed to read existing {out_path}; will re-fetch")

    # Try nflreadpy first (if available)
    if 'nflreadpy' in mods:
      nrp = mods['nflreadpy']
      try:
        # === Anpassungspunkt ===
        # nflreadpy API-Beispiele unterscheiden sich; passe hier an, falls nötig.
        # Typische Möglichkeiten (je nach Version):
        # - nrp.tidy.read_rosters(season)
        # - nrp.load_rosters(season)
        # - nrp.rosters(season)
        # Wir probieren mehrere Aufrufe in einer Reihenfolge und verwenden den ersten, der funktioniert
        
        got = None
        try:
          got = nrp.tidy.read_rosters(s)
        except Exception:
          pass
        if got is None and hasattr(nrp, "load_rosters"):
          try:
            got = nrp.load_rosters(s)
          except Exception:
            pass
        if got is None and hasattr(nrp, "rosters"):
          try:
            got = nrp.rosters(s)
          except Exception:
            pass

        if got is not None:
          df = pd.DataFrame(got)
          write_parquet(df, out_path)
          results.append(df)
          logger.info(f"Fetched rosters ffor {s} via nflreadpy")
          time.sleep(sleep_between)
          continue
        else:
          logger.warning("nflreadpy is installed but automatic call paterns failed; falling back.")
      except Exception as e:
        logger.exception("Error while using nflreadpy for rosters (falling back): %s", e)
    
    # Try nfl_data_py next (if available)
    if 'nfl_data_py' in mods:
      ndp = mods['nfl_data_py']
      try:
        got = None
        if hasattr(ndp, "get_rosters"):
          got = ndp.get_rosters(s)
        elif hasattr(ndp, "rosters"):
          got = ndp.rosters(s)
        if got is not None:
          df = pd.DataFrame(got)
          write_parquet(df, out_path)
          results.append(df)
          logger.info(f"Fetched rosters for {s} via nfl_data_py")
          time.sleep(sleep_between)
          continue
      except Exception as e:
        logger.exception("Error while using nfl_data_py for rosters (falling back): %s", e)

    # Fallback: generate demo data
    logger.info(f"No fetching library worked for season {s}. Writing demo players file.")
    demo = pd.DataFrame([
      {"player_id": 1, "full_name": "Sean Example", "position": "QB", "team": "KC", "birthdate": "1998-10-14"},
      {"player_id": 2, "full_name": "Dodo Piss", "position": "FB", "team": "DAL", "birthdate": "1998-11-12"},
    ])
    write_parquet(demo, out_path)
    results.append(demo)
  
  return pd.concat(results, ignore_index=True)

def fetch_gamelogs(seasons=[2023], force=False, sleep_between=0.5):
  """
    Lade Gamelogs (per-game stats) für gegebene Seasons.
    Verhalten analog zu fetch_rosters.
  """

  mods = use_nfl_library_available()
  results = []

  for s in seasons:
    out_path = RAW_DIR / f"gamelogs_{s}.parquet"
    if out_path.exists() and not force:
      logger.info(f"{out_path} already exists -> skipping (use --force to overwrite)")
      try:
        df = pd.read_parquet(out_path)
        results.append(df)
        continue
      except Exception:
        logger.warning(f"Failed to read existing {out_path}; will re-fetch")
        
    got = None

    # Try nflreadpy
    if 'nflreadpy' in mods:
      nrp = mods['nflreadpy']
      try:
        got = None
        try:
          got = nrp.tidy.read_gamelogs(s)
        except Exception:
          pass
        if got is None and hasattr(nrp, "load_gamelogs"):
          try:
            got = nrp.load_gamelogs(s)
          except Exception:
            pass
        if got is None and hasattr(nrp, "gamelogs"):
          try:
            got = nrp.gamelogs(s)
          except Exception:
            pass
        
        if got is not None:
          df = pd.DataFrame(got)
          write_parquet(df, out_path)
          results.append(df)
          logger.info(f"Fetched gamelogs for {s} via nflreadpy")
          time.sleep(sleep_between)
          continue
        else:
          logger.warning("nflreadpy installed but could not auto-fetch gamelogs")
      except Exception as e:
        logger.exception("Error while using nflreadpy for gamelogs (falling back): %s", e)

    # Try nfl_data_py
    if 'nfl_data_py' in mods:
      ndp = mods['nfl_data_py']
      try:
        # Dies ist der Standard-Befehl für wöchentliche Stats/Gamelogs
        got = ndp.import_weekly_data([s]) 
        
        if got is not None:
          df = pd.DataFrame(got)
          write_parquet(df, out_path)
          results.append(df)
          logger.info(f"Fetched gamelogs for {s} via nfl_data_py")
          time.sleep(sleep_between)
          continue
      except Exception as e:
        logger.error(f"Error using nfl_data_py: {e}")
    
    # Fallback demo
    logger.info(f"No fetching library worked for season {s}. Writing demo gamelogs file.")
    demo = pd.DataFrame([
      {"game_id": 1, "player_id": 1, "season": s, "yards": 5478, "td": 8},
    ])
    write_parquet(demo, out_path)
    results.append(demo)
  
  return pd.concat(results, ignore_index=True)

