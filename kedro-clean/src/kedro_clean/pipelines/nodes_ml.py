import pandas as pd
from sqlalchemy import create_engine, text
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import MiniBatchKMeans
import joblib
from pathlib import Path

def train_clustering_model(con_string: str) -> None:
    """
    1) Lit un échantillon dans PostgreSQL
    2) Entraîne TF-IDF + MiniBatchKMeans
    3) Sauvegarde vectorizer.pkl + kmeans.pkl
    """
    print("[ML] Connexion Postgres...")
    engine = create_engine(con_string)

    query = text("""
        SELECT text_clean
        FROM bluesky_posts_clean
        WHERE text_clean IS NOT NULL
        ORDER BY random()
        LIMIT 300000
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    print(f"[ML] Nombre de posts récupérés pour l'entraînement : {len(df)}")
    print("[ML] Exemples de textes :")
    print(df["text_clean"].head(3).to_string(index=False))

    corpus = df["text_clean"].astype(str).tolist()

    print("[ML] Entraînement TF-IDF...")
    vectorizer = TfidfVectorizer(
        max_features=50000,
        stop_words="english",
        min_df=5,
        max_df=0.8,
    )
    X = vectorizer.fit_transform(corpus)
    print(f"[ML] Matrice TF-IDF shape = {X.shape}")

    print("[ML] Entraînement MiniBatchKMeans...")
    kmeans = MiniBatchKMeans(
        n_clusters=15,
        random_state=42,
        batch_size=1024,
        n_init="auto",
    )
    kmeans.fit(X)
    print("[ML] Entraînement terminé.")

    models_dir = Path("data/06_models")
    models_dir.mkdir(parents=True, exist_ok=True)
    print(f"[ML] Dossier modèles : {models_dir.resolve()}")

    joblib.dump(vectorizer, models_dir / "vectorizer.pkl")
    joblib.dump(kmeans, models_dir / "kmeans.pkl")

    print("[ML] vectorizer.pkl et kmeans.pkl sauvegardés.")