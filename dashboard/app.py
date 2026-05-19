"""
app.py -- Dashboard Streamlit
==============================
Visualisation interactive des datamarts Spotify (couche Gold MySQL).

Lancement :
  streamlit run app.py --server.port 8501

Variables d'environnement :
  MYSQL_HOST, MYSQL_DB, MYSQL_USER, MYSQL_PASSWORD
  API_URL (optionnel)
"""

import os

import pandas as pd
import plotly.express as px
import pymysql
import streamlit as st

# ─────────────────────────────────────────────────────────────
#  Configuration de la page
# ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Spotify Analytics — Architecture Médaillon",
    page_icon="🎵",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────
#  Connexion MySQL
# ─────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.getenv("MYSQL_HOST",     "mysql"),
    "database": os.getenv("MYSQL_DB",       "spotify_bdf"),
    "user":     os.getenv("MYSQL_USER",     "root"),
    "password": os.getenv("MYSQL_PASSWORD", "root123"),
    "port":     int(os.getenv("MYSQL_PORT", "3306")),
    "cursorclass": pymysql.cursors.DictCursor,
}


@st.cache_data(ttl=300)
def query(sql: str, params: tuple = ()) -> pd.DataFrame:
    """Execute une requete SQL et retourne un DataFrame Pandas (cache 5 min)."""
    conn = pymysql.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql, params)
            rows = cursor.fetchall()
        return pd.DataFrame(rows)
    finally:
        conn.close()


def check_connection() -> bool:
    try:
        query("SELECT 1")
        return True
    except Exception:
        return False


# ─────────────────────────────────────────────────────────────
#  Chargement des listes de filtres
# ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=600)
def load_genres() -> list:
    df = query("SELECT DISTINCT genre FROM dm_track_popularity WHERE genre IS NOT NULL ORDER BY genre")
    return df["genre"].tolist() if not df.empty else []


@st.cache_data(ttl=600)
def load_year_range() -> tuple:
    df = query("SELECT MIN(year) AS y_min, MAX(year) AS y_max FROM dm_track_popularity")
    if df.empty or df["y_min"][0] is None:
        return 2000, 2023
    return int(df["y_min"][0]), int(df["y_max"][0])


@st.cache_data(ttl=600)
def load_decades() -> list:
    df = query("SELECT DISTINCT decade FROM dm_genre_trends WHERE decade IS NOT NULL ORDER BY decade")
    return df["decade"].tolist() if not df.empty else []


# ─────────────────────────────────────────────────────────────
#  Sidebar
# ─────────────────────────────────────────────────────────────
st.sidebar.title("🎵 Spotify Analytics")
st.sidebar.markdown("**Architecture Médaillon**")
st.sidebar.markdown("HDFS → Hive → MySQL")
st.sidebar.divider()

db_ok = check_connection()

try:
    all_genres = load_genres()
    y_min, y_max = load_year_range()
    all_decades = load_decades()
except Exception as e:
    st.sidebar.error(f"❌ MySQL : {e}")
    all_genres = []
    y_min, y_max = 2000, 2023
    all_decades = []

selected_genres = st.sidebar.multiselect(
    "Genres",
    options=all_genres,
    default=all_genres[:8] if len(all_genres) >= 8 else all_genres,
    help="Selectionner un ou plusieurs genres",
)

year_range = st.sidebar.slider(
    "Plage d'annees",
    min_value=y_min,
    max_value=y_max,
    value=(max(y_min, 2010), y_max),
)

top_n = st.sidebar.number_input(
    "Nombre d'artistes (Top N)",
    min_value=5,
    max_value=50,
    value=20,
    step=5,
)

st.sidebar.divider()
st.sidebar.caption(f"🗄️ Base : `{DB_CONFIG['database']}` | {'✅ Connecte' if db_ok else '❌ Deconnecte'}")

# ─────────────────────────────────────────────────────────────
#  En-tete principal
# ─────────────────────────────────────────────────────────────
st.title("🎵 Spotify Analytics — Tendances Musicales")
st.markdown(
    "Dashboard base sur le dataset **Spotify 1 Million Tracks** | "
    "Couche Gold — Datamarts MySQL"
)
st.divider()

if not db_ok:
    st.error("Impossible de se connecter a MySQL. Verifiez que la base est demarree.")
    st.stop()

if not selected_genres:
    st.warning("Selectionnez au moins un genre dans la barre laterale.")
    st.stop()

genre_placeholders = ", ".join(["%s"] * len(selected_genres))

# ─────────────────────────────────────────────────────────────
#  Metriques de synthese
# ─────────────────────────────────────────────────────────────
df_summary = query(
    f"""
    SELECT
        COUNT(*)                    AS total_tracks,
        COUNT(DISTINCT genre)       AS nb_genres,
        ROUND(AVG(popularity), 1)   AS avg_pop,
        ROUND(AVG(tempo), 1)        AS avg_tempo
    FROM dm_track_popularity
    WHERE genre IN ({genre_placeholders})
      AND year BETWEEN %s AND %s
    """,
    tuple(selected_genres) + year_range,
)

col1, col2, col3, col4 = st.columns(4)
col1.metric("Tracks analyses",     f"{int(df_summary['total_tracks'][0] or 0):,}")
col2.metric("Genres selectionnes", int(df_summary['nb_genres'][0] or 0))
col3.metric("Popularite moyenne",  f"{df_summary['avg_pop'][0] or 0}")
col4.metric("Tempo moyen (BPM)",   f"{df_summary['avg_tempo'][0] or 0}")

st.divider()

# ─────────────────────────────────────────────────────────────
#  GRAPHIQUE 1 — Popularite par genre (Bar chart)
# ─────────────────────────────────────────────────────────────
st.subheader("Graphique 1 — Popularite Moyenne par Genre")
st.caption("Source : `dm_track_popularity`")

df_pop = query(
    f"""
    SELECT genre,
           ROUND(AVG(popularity), 2) AS avg_popularity,
           COUNT(*)                   AS total_tracks
    FROM dm_track_popularity
    WHERE genre IN ({genre_placeholders})
      AND year BETWEEN %s AND %s
    GROUP BY genre
    ORDER BY avg_popularity DESC
    """,
    tuple(selected_genres) + year_range,
)

if not df_pop.empty:
    fig1 = px.bar(
        df_pop,
        x="genre",
        y="avg_popularity",
        color="avg_popularity",
        color_continuous_scale="Greens",
        text=df_pop["avg_popularity"].round(1),
        hover_data={"total_tracks": True},
        labels={"genre": "Genre", "avg_popularity": "Popularite moyenne", "total_tracks": "Nb tracks"},
        title=f"Popularite moyenne par genre ({year_range[0]}-{year_range[1]})",
        height=450,
    )
    fig1.update_traces(textposition="outside")
    fig1.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        coloraxis_showscale=False,
        xaxis_tickangle=-30,
    )
    st.plotly_chart(fig1, use_container_width=True)
else:
    st.info("Aucune donnee disponible pour ces filtres.")

st.divider()

# ─────────────────────────────────────────────────────────────
#  GRAPHIQUE 2 — Tendances genre x decennie (Line chart)
# ─────────────────────────────────────────────────────────────
st.subheader("Graphique 2 — Tendances Audio par Genre et Decennie")
st.caption("Source : `dm_genre_trends`")

genres_evo = selected_genres[:5]
gp_evo = ", ".join(["%s"] * len(genres_evo))

df_evo = query(
    f"""
    SELECT genre, decade, avg_popularity, avg_danceability, avg_energy, nb_tracks
    FROM dm_genre_trends
    WHERE genre IN ({gp_evo})
    ORDER BY genre, decade
    """,
    tuple(genres_evo),
)

if not df_evo.empty:
    metric_choice = st.radio(
        "Metrique",
        ["avg_popularity", "avg_energy", "avg_danceability"],
        format_func=lambda x: {
            "avg_popularity":   "Popularite",
            "avg_energy":       "Energie",
            "avg_danceability": "Dansabilite",
        }[x],
        horizontal=True,
    )

    fig2 = px.line(
        df_evo,
        x="decade",
        y=metric_choice,
        color="genre",
        markers=True,
        labels={"decade": "Decennie", metric_choice: metric_choice.replace("avg_", "").capitalize(), "genre": "Genre"},
        title=f"Evolution de '{metric_choice.replace('avg_', '')}' par genre",
        height=450,
        color_discrete_sequence=px.colors.qualitative.Set2,
    )
    fig2.update_layout(plot_bgcolor="rgba(0,0,0,0)", hovermode="x unified")
    st.plotly_chart(fig2, use_container_width=True)
else:
    st.info("Aucune donnee disponible pour ces filtres.")

st.divider()

# ─────────────────────────────────────────────────────────────
#  GRAPHIQUE 3 — Top Artistes par decennie
# ─────────────────────────────────────────────────────────────
st.subheader("Graphique 3 — Top Artistes par Decennie")
st.caption("Source : `dm_top_artists` | Classe par influence_score = avg_popularity × log1p(total_tracks)")

col_dec, _ = st.columns([2, 3])
with col_dec:
    decade_top = st.selectbox(
        "Decennie",
        options=all_decades,
        index=len(all_decades) - 1 if all_decades else 0,
        key="decade_top_artists",
    )

df_top = query(
    """
    SELECT artist_name, avg_popularity, total_tracks, influence_score, rank_in_decade, main_genre
    FROM dm_top_artists
    WHERE decade = %s
    ORDER BY rank_in_decade
    LIMIT %s
    """,
    (decade_top, int(top_n)),
)

if not df_top.empty:
    fig3 = px.bar(
        df_top.sort_values("influence_score"),
        x="influence_score",
        y="artist_name",
        orientation="h",
        color="influence_score",
        color_continuous_scale="Greens",
        text=df_top.sort_values("influence_score")["influence_score"].round(1),
        hover_data={"total_tracks": True, "avg_popularity": True, "main_genre": True},
        labels={
            "influence_score": "Influence Score",
            "artist_name":     "Artiste",
            "total_tracks":    "Nb tracks",
            "avg_popularity":  "Pop. moy.",
            "main_genre":      "Genre principal",
        },
        title=f"Top {top_n} artistes — Decennie {decade_top}",
        height=max(400, int(top_n) * 22),
    )
    fig3.update_traces(textposition="outside")
    fig3.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        coloraxis_showscale=False,
        yaxis_title="",
    )
    st.plotly_chart(fig3, use_container_width=True)
else:
    st.info(f"Aucun artiste trouve pour la decennie {decade_top}.")

st.divider()

# ─────────────────────────────────────────────────────────────
#  GRAPHIQUE 4 — Hits emergents
# ─────────────────────────────────────────────────────────────
st.subheader("Graphique 4 — Hits Emergents par Genre et Annee")
st.caption("Source : `dm_hits_emergents` | Top 10 par genre × annee")

col_g4, col_y4 = st.columns(2)
with col_g4:
    genre_hits = st.selectbox("Genre", options=selected_genres, key="genre_hits")
with col_y4:
    year_hits = st.slider("Annee", min_value=y_min, max_value=y_max, value=y_max, key="year_hits")

df_hits = query(
    """
    SELECT track_name, artist_name, popularity, danceability, energy, valence, tempo, rank_in_year
    FROM dm_hits_emergents
    WHERE genre = %s AND year = %s
    ORDER BY rank_in_year
    LIMIT 10
    """,
    (genre_hits, year_hits),
)

if not df_hits.empty:
    fig4 = px.scatter(
        df_hits,
        x="danceability",
        y="energy",
        size="popularity",
        color="valence",
        hover_name="track_name",
        hover_data={"artist_name": True, "popularity": True, "tempo": True, "rank_in_year": True},
        color_continuous_scale="RdYlGn",
        labels={
            "danceability": "Dansabilite",
            "energy":       "Energie",
            "valence":      "Valence (positivite)",
        },
        title=f"Top 10 hits {genre_hits} — {year_hits} (taille = popularite, couleur = valence)",
        height=500,
    )
    fig4.update_layout(plot_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig4, use_container_width=True)

    st.dataframe(
        df_hits[["rank_in_year", "track_name", "artist_name", "popularity", "danceability", "energy", "valence", "tempo"]],
        use_container_width=True,
        hide_index=True,
    )
else:
    st.info(f"Aucun hit trouve pour {genre_hits} en {year_hits}.")

st.divider()

# ─────────────────────────────────────────────────────────────
#  Tableau de donnees brutes
# ─────────────────────────────────────────────────────────────
with st.expander("Donnees brutes — dm_track_popularity (100 lignes)"):
    df_raw = query(
        f"""
        SELECT track_id, track_name, artist_name, genre, year, popularity,
               danceability, energy, valence, tempo, popularity_category, rank_in_genre
        FROM dm_track_popularity
        WHERE genre IN ({genre_placeholders})
          AND year BETWEEN %s AND %s
        ORDER BY popularity DESC
        LIMIT 100
        """,
        tuple(selected_genres) + year_range,
    )
    st.dataframe(df_raw, use_container_width=True, hide_index=True)

st.caption("Efrei - Mastère Data Engineering & AI 2025-2027 | Projet de Big Data Framework")
st.caption("Auteurs : TA Khanh Vy | MADOUNGOU Alice Colombe | AMINI Gloria")
