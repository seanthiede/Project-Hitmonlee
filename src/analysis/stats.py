import sqlite3
import pandas as pd
from pathlib import Path
import plotly.express as px

DB_PATH = Path("db/nfl.db")

import pandas as pd
from pathlib import Path

players_df = pd.read_parquet(Path("raw/players_2023.parquet"))
gamelogs_df = pd.read_parquet(Path("raw/gamelogs_2023.parquet"))

print(players_df.columns)
print(gamelogs_df.columns)
print(players_df.head())


def load_tables(limit_players=None, limit_gamelogs=None):
    conn = sqlite3.connect(DB_PATH)

    try:
        players = pd.read_sql("SELECT * FROM players", conn)
        gamelogs = pd.read_sql("SELECT * FROM gamelogs", conn)
    finally:
        conn.close()
    
    return players, gamelogs

def aggregate_player_totals(gamelogs_df):
    # simple sum of yards and touchdown per player
    if "yards" not in gamelogs_df.columns:
        raise ValueError("gamelogs no 'yards' column")
    agg = gamelogs_df.groupby("player_id", as_index=False).agg({
        "yards": "sum",
        "td": "sum"
    }).rename(columns={"td":"touchdowns"})

    return agg

def top_players_by_yards(players_df, gamelogs_df, top_n=10):
    agg = aggregate_player_totals(gamelogs_df)
    merged = agg.merge(players_df, on="player_id", how="left")
    topn = merged.sort_values("yards", ascending=False).head(top_n)
    
    return topn

def plot_top_players_bar(topn_df):
    fig = px.bar(topn_df, x="full_name", y="yards", color="team", title="Top Players by Yards")
    fig.update_layout(xaxis_tickangle=-30, margin=dict(t=40, b=80))

    return fig