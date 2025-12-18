import sqlite3
import pandas as pd
from pathlib import Path
import plotly.express as px


DB_PATH = Path("db/nfl.db")

def get_data_from_db(query):
    """
    Diese Funktion verbindet sich mit der Datenbank, führt einen 
    Befehl (Query) aus und gibt das Ergebnis als Pandas DataFrame zurück.
    """
    if not DB_PATH.exists():
        print("Fehler: Datenbank nicht gefunden! Hast du ingest.py schon ausgeführt?")
        return pd.DataFrame() # returns empty df
    
    conn = sqlite3.connect(DB_PATH)
    # pd.read_sql macht die ganze Arbeit: Verbindung öffnen, Daten holen, Tabelle erstellen
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def get_top_offensive_players(season=2023, top_n=10):
    """
    Holt die Spieler und Statistiken, führt sie zusammen und berechnet die Top-Spieler.
    """

    # first, load data
    players = get_data_from_db("SELECT player_id, full_name, team, position FROM players")
    gamelogs = get_data_from_db(f"SELECT player_id, yards, td FROM gamelogs WHERE season = {season}")

    if players.empty() or gamelogs.empty():
        return pd.DataFrame()
    
    # second, aggregation (sum stats per player)
    player_stats = gamelogs.groupby("player_id").agg({
        "yards": "sum", 
        "td": "sum"
    }).reset_index() # makes normal column of player_id

    # third, merging
    # we merge de stats def with the name df with player_id
    combined = player_stats.merge(players, on="player_id", how="left")

    # 4. sort (best first)
    top_players = combined.sort_values(by="yards", ascending=False).head(top_n)

    return top_players

def plot_top_players_bar(topn_df):
    """Erstellt das Balkendiagramm mit Plotly."""
    if topn_df.empty:
        return None
    
    fig = px.bar(
        topn_df,
        x="full_name",
        y="yards", 
        color="team",
        title="Top Players by Yards",
        template="plotly_datk"
    )
    fig.update_layout(xaxis_tickangle=-30)
    