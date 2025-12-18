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

def get_top_offensive_players(season=2023, top_n=10, positions=None, teams=None):
    """
    Holt Spieler- und Statistiken-Daten, bereinigt sie und filtert nach 
    Saison, Position und Team.
    """
    players = get_data_from_db("SELECT * FROM players")
    gamelogs = get_data_from_db("SELECT * FROM gamelogs")

    if players.empty or gamelogs.empty:
        return pd.DataFrame()

    # 1. HEADER-REPARATUR (Falls '0' als Spaltenname erscheint)
    for df in [players, gamelogs]:
        if '0' in df.columns or 0 in df.columns:
            new_header = df.iloc[0]
            df.columns = [str(c).strip() for c in new_header]
            df.drop(df.index[0], inplace=True)
            df.reset_index(drop=True, inplace=True)

    # 2. ID-NORMALISIERUNG (GSIS_ID zu player_id)
    for df in [players, gamelogs]:
        # Wir suchen nach typischen ID-Spaltennamen, falls 'player_id' fehlt
        if 'player_id' not in df.columns:
            for col in ['gsis_id', 'id', 'playerID', '00-0017724']:
                if col in df.columns:
                    df.rename(columns={col: 'player_id'}, inplace=True)
                    break
        
        # Radikale Reinigung der Spaltennamen (Leerzeichen entfernen)
        df.columns = [str(c).strip() for c in df.columns]

    # 3. FILTERING (Bevor wir aggregieren und mergen)
    # Filter auf die Saison in den Gamelogs
    if 'season' in gamelogs.columns:
        # Sicherstellen, dass season ein Integer ist für den Vergleich
        gamelogs['season'] = pd.to_numeric(gamelogs['season'], errors='coerce')
        gamelogs = gamelogs[gamelogs['season'] == int(season)]
    
    # Filter auf Positionen (z.B. ['QB', 'WR'])
    if positions and 'position' in players.columns:
        players = players[players['position'].isin(positions)]

    # Filter auf Teams (z.B. ['PHI', 'KC'])
    if teams and 'team' in players.columns:
        players = players[players['team'].isin(teams)]

    # 4. AGGREGATION (Stats zusammenrechnen)
    # Sicherstellen, dass yards und td existieren, sonst 0 setzen
    for col in ['yards', 'td']:
        if col not in gamelogs.columns:
            gamelogs[col] = 0
        else:
            gamelogs[col] = pd.to_numeric(gamelogs[col], errors='coerce').fillna(0)

    player_stats = gamelogs.groupby("player_id").agg({
        "yards": "sum",
        "td": "sum"
    }).reset_index()

    # 5. MERGE (Zusammenführen)
    # 'inner' join sorgt dafür, dass nur Spieler bleiben, die Stats haben UND die Filter überlebt haben
    combined = player_stats.merge(players, on="player_id", how="inner")
    
    # Sortieren und Top N zurückgeben
    return combined.sort_values(by="yards", ascending=False).head(top_n)

def plot_top_players_bar(topn_df):
    """Erstellt das Balkendiagramm mit Plotly."""
    if topn_df is None or topn_df.empty:
        return None
        
    fig = px.bar(
        topn_df, 
        x="full_name", 
        y="yards", 
        color="team", 
        title="Top Players by Yards"
        # Wir entfernen template="plotly_dark" oder ersetzen es durch "none"
    )
    
    # Statt im Konstruktor setzen wir das Design hier manuell, 
    # das ist weniger fehleranfällig:
    fig.update_layout(
        template="plotly_dark",
        xaxis_tickangle=-30,
        margin=dict(t=50, b=100)
    )
    return fig

def get_player_headshot(player_id):
    """Holt die Headshot-URL für einen bestimmten Spieler aus der DB."""
    query = f"SELECT headshot_url FROM players WHERE player_id = '{player_id}'"
    df = get_data_from_db(query)
    if not df.empty and df['headshot_url'].iloc[0]:
        return df['headshot_url'].iloc[0]
    return "https://via.placeholder.com/150" # Fallback, falls kein Bild da ist