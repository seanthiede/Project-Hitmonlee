import streamlit as st
import pandas as pd
from src.analysis.stats import get_top_offensive_players, plot_top_players_bar

# --- SEITEN-KONFIGURATION ---
st.set_page_config(page_title="NFL Stats Dashboard", layout="wide")

st.title("🏈 NFL Offensive Player Dashboard")
st.markdown("Analysiere die Top-Performer der NFL Saison.")

# --- SIDEBAR / FILTER ---
st.sidebar.header("Einstellungen & Filter")

# 1. Saison & Top N (Standard)
selected_season = st.sidebar.selectbox("Saison", [2023, 2022, 2021], index=0)
top_n = st.sidebar.slider("Anzahl Spieler", 5, 50, 10)

st.sidebar.divider()

# 2. Positions-Filter
# Wir definieren die gängigen Offensiv-Positionen
pos_options = ["QB", "RB", "WR", "TE"]
selected_pos = st.sidebar.multiselect(
    "Positionen auswählen", 
    options=pos_options, 
    default=pos_options
)

# 3. Team-Filter
# Wir holen uns erst einmal alle Daten kurz ohne Team-Filter, 
# um eine Liste aller verfügbaren Teams für das Dropdown zu bekommen
df_all_teams = get_top_offensive_players(season=selected_season, top_n=1000)
if not df_all_teams.empty:
    team_list = sorted(df_all_teams['team'].unique().tolist())
else:
    team_list = []

selected_teams = st.sidebar.multiselect(
    "Teams auswählen", 
    options=team_list, 
    default=team_list
)

# --- DATEN LADEN (mit allen Filtern) ---
df_top = get_top_offensive_players(
    season=selected_season, 
    top_n=top_n,
    positions=selected_pos,
    teams=selected_teams
)

# --- DASHBOARD LAYOUT ---

if df_top.empty:
    st.warning("Keine Daten für diese Auswahl gefunden. Probiere andere Filter!")
else:
    # 🏆 TOP PERFORMER CARD (Die "Helden"-Sektion)
    top_player = df_top.iloc[0]
    
    with st.expander("⭐ Top Performer Details", expanded=True):
        col1, col2, col3 = st.columns([1, 2, 2])
        
        with col1:
            # Bild-Logik mit Sicherheits-Check
            img_url = top_player.get('headshot_url')
            if isinstance(img_url, str) and len(img_url) > 10:
                st.image(img_url, width=150)
            else:
                st.image("https://via.placeholder.com/150", caption="Kein Bild verfügbar")
        
        with col2:
            st.subheader(top_player['full_name'])
            st.metric("Yards", f"{int(top_player['yards']):,}")
            st.write(f"**Team:** {top_player['team']} | **Pos:** {top_player['position']}")
            
        with col3:
            st.subheader("Stats")
            st.metric("Touchdowns", int(top_player['td']))

    st.divider()

    # 📊 GRAFIK-SEKTION
    col_chart, col_table = st.columns([2, 1])
    
    with col_chart:
        st.subheader(f"Top {top_n} Spieler nach Yards")
        fig = plot_top_players_bar(df_top)
        if fig:
            st.plotly_chart(fig, use_container_width=True)

    with col_table:
        st.subheader("Daten-Tabelle")
        # Wir zeigen nur die wichtigsten Spalten in der Tabelle an
        display_cols = ['full_name', 'team', 'position', 'yards', 'td']
        st.dataframe(
            df_top[display_cols], 
            height=400,
            use_container_width=True
        )

# Footer
st.sidebar.info(f"Datenstand: {len(df_top)} Spieler geladen.")