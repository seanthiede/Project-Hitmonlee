import sqlite3
import pandas as pd
from pathlib import Path
import plotly.express as px


DB_PATH = Path("db/nfl.db")

def check_columns():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(players)")
    cols = cursor.fetchall()
    print("Spalten in der Tabelle 'players':", [c[1] for c in cols])
    conn.close()

check_columns()

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
    players = get_data_from_db("SELECT * FROM players")
    gamelogs = get_data_from_db("SELECT * FROM gamelogs")

    if not players.empty:
        # Das zeigt uns im Terminal, welche Spalten wir jetzt wirklich haben
        print("Verfügbare Spalten in Players:", players.columns.tolist())

    if players.empty or gamelogs.empty:
        return pd.DataFrame()

    # 1. Radikale Reinigung: Leerzeichen aus Spaltennamen entfernen
    players.columns = [c.strip() for c in players.columns]
    gamelogs.columns = [c.strip() for c in gamelogs.columns]

    # 2. ID-Normalisierung mit Erfolgs-Kontrolle
    for df_name, df in [("players", players), ("gamelogs", gamelogs)]:
        if 'player_id' not in df.columns:
            # Wir suchen nach ALLEM, was wie eine ID aussieht
            possible_ids = [c for c in df.columns if 'id' in c.lower()]
            if possible_ids:
                # Wir nehmen den ersten Treffer und taufen ihn um
                df.rename(columns={possible_ids[0]: 'player_id'}, inplace=True)
            else:
                # Wenn gar nichts gefunden wird, zeigen wir die Spalten im Terminal
                print(f"WARNUNG: Keine ID-Spalte in {df_name} gefunden! Vorhanden: {list(df.columns)}")

    # 3. Saison & Stats sicherstellen
    if 'season' not in gamelogs.columns:
        gamelogs['season'] = season
    
    gamelogs = gamelogs[gamelogs['season'] == season]

    # Sicherstellen, dass yards und td existieren
    for col, alt in [('yards', 'yds'), ('td', 'touchdowns')]:
        if col not in gamelogs.columns:
            if alt in gamelogs.columns:
                gamelogs.rename(columns={alt: col}, inplace=True)
            else:
                gamelogs[col] = 0

    # 4. Aggregation
    player_stats = gamelogs.groupby("player_id").agg({
        "yards": "sum",
        "td": "sum"
    }).reset_index()

    # 5. Zusammenführen (Merge)
    # Wir prüfen VORHER, ob beide Tabellen player_id haben
    if 'player_id' in player_stats.columns and 'player_id' in players.columns:
        combined = player_stats.merge(players, on="player_id", how="left")
    else:
        print("Fehler: Merge nicht möglich, player_id fehlt in einer der Tabellen.")
        return pd.DataFrame()
    
    return combined.sort_values(by="yards", ascending=False).head(top_n)

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
    return fig