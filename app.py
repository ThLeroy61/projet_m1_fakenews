import streamlit as st
from datetime import date
from bluesky_ml.predict import cluster_posts, summarize_clusters

# Connexion Postgres
CONN = "postgresql+psycopg2://postgres:ThKay26091995@localhost:5432/bluesky_clean"


st.title("Clustering Bluesky / Fake News")

# --- Paramètres généraux ---
n_posts = st.slider("Nombre de posts", min_value=1000, max_value=50000, value=10000, step=1000)

mode = st.radio(
    "Mode de filtrage temporel",
    ["7 derniers jours", "Plage de dates"],
    index=0,
)

days_back = None
date_start = None
date_end = None

if mode == "7 derniers jours":
    days_back = st.slider("Nombre de jours en arrière", min_value=1, max_value=30, value=7)
else:
    col1, col2 = st.columns(2)
    with col1:
        date_start = st.date_input("Date de début", value=date(2026, 1, 6))
    with col2:
        date_end = st.date_input("Date de fin", value=date(2026, 1, 13))

    # Conversion en string ISO pour SQL
    date_start = date_start.isoformat() if date_start else None
    date_end = date_end.isoformat() if date_end else None

if st.button("Lancer le clustering"):
    with st.spinner("Récupération des posts et clustering..."):
        df_clusters = cluster_posts(
            con_string=CONN,
            n_posts=n_posts,
            days_back=days_back if mode == "7 derniers jours" else None,
            date_start=date_start if mode == "Plage de dates" else None,
            date_end=date_end if mode == "Plage de dates" else None,
        )

    st.write(f"{len(df_clusters)} posts récupérés")

    if df_clusters.empty:
        st.warning("Aucun post trouvé avec ces paramètres.")
    else:
        summary = summarize_clusters(df_clusters, n_top_words=10, n_examples=3)

        for cid, info in sorted(summary.items()):
            st.subheader(f"Cluster {cid}")

            # top_words = [(mot, score), ...]
            formatted = [f"{w} ({score:.3f})" for w, score in info["top_words"]]
            st.write("Top mots :", ", ".join(formatted))

            with st.expander("Exemples"):
                for ex in info["examples"]:
                    st.write("- " + ex)
