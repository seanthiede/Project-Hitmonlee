import streamlit as st
import pandas as pd
from src.analysis.stats import get_top_offensive_players, plot_top_players_bar

# 1. Seiteneinstellungen (Das sorgt für den breiten, modernen Look)
st.set_page_config(page_title="NFL Analytics Dashboard", layout="wide")

# 2. Ein bisschen "Ästhetik" via CSS
st.markdown("""
            <style>
            .main {
                background-color: #0b1220;
            }
            h1 {
                color: #0f62fe;
                font-family: 'Inter', sans-serif;    
            }
            </style>
            """, unsafe_allow_html=True)

st.title("🏈 NFL Player Performance")

# 3. Sidebar für Filter
st.sidebar.header("Filter-Optionen")
selected_season = st.sidebar.selectbox("Saison wählen", [2023, 2022], index=0)
top_n = st.sidebar.slider("Anzahl Spieler", 5, 20, 10)

# 4. Daten laden & anzeigen
st.subheader(f"Top {top_n} Spieler nach Yards - Saison {selected_season}")

# Hier rufen wir deine Logik auf!
df_top = get_top_offensive_players(season=selected_season, top_n=top_n)

if not df_top.empty:
    top_player = df_top.iloc[0]
    
    with st.container():
        col_img, col_text = st.columns([1, 4])
        
        with col_img:
            # 1. Wir holen die URL
            raw_url = top_player.get('headshot_url')
            
            # 2. Sicherheits-Check: Ist die URL ein gültiger Text?
            # Wenn raw_url None ist oder kein Text, nutzen wir den Platzhalter
            if isinstance(raw_url, str) and raw_url.strip() != "":
                image_url = raw_url
            else:
                image_url = "https://via.placeholder.com/150"
            
            # 3. Das Bild anzeigen
            st.image(image_url, width=150)
            
        with col_text:
            st.markdown(f"## 🏆 Top Performer: {top_player['full_name']}")
            # Wir nutzen hier .get() mit Fallback 'N/A', falls Team oder Stats fehlen
            team = top_player.get('team', 'N/A')
            yards = top_player.get('yards', 0)
            st.markdown(f"**Team:** {team} | **Yards:** {yards:,}")

    st.divider()

if not df_top.empty:
    # Zwei Spalten Layout: Links Grafik, Rechts Tabelle
    col1, col2 = st.columns([2, 1])

    with col1:
        # Hier nutzen wir deine Plotly-Funktion aus stats.py
        fig = plot_top_players_bar(df_top)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.write("Rohdaten:")
        # Eine schön formatierte Tabelle
        st.dataframe(df_top[["full_name", "team", "yards", "td"]], hide_index=True)
else:
    st.warning("Keine Daten gefunden. Hast du die Datenbank schon befüllt?")