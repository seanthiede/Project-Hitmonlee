import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).parents[2] # repo root
sys.path.insert(0, str(ROOT / "src"))

from fetchers.nflreadpy_fetch import fetch_gamelogs, fetch_rosters, logger

def main(args):
  seasons = args.seasons if args.seasons else [2023]
  force = args.force
  
  if args.rosters:
    logger.info(f"Fetching rosters for seasons: {seasons} (force={force})")
    fetch_rosters(seasons=seasons, force=force)
  
  if args.gamelogs:
    logger.info(f"Fetching gamelogs for seasons: {seasons} (force={force})")
    fetch_gamelogs(seasons=seasons, force=force)
  
  if not args.rosters and not args.gamelogs:
    # default run both
    fetch_rosters(seasons=seasons, force=force)
    fetch_gamelogs(seasons=seasons, force=force)
    

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description="NFL data fetcher (starter).")
  parser.add_argument("--seasons", "-s", type=int, nargs="+", help="Seasons to fetch (e.g. 2023 2022)", default=[2023])
  parser.add_argument("--force", "-f", action="store_true", help="Force re-fetch even if raw files exist")
  parser.add_argument("--rosters", action="store_true", help="Fetch rosters")
  parser.add_argument("--gamelogs", action="store_true", help="Fetch gamelogs")
  args = parser.parse_args()
  main(args)