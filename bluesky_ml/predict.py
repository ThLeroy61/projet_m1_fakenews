import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text
import joblib
from collections import Counter
from typing import Dict, List
import numpy as np

# 1. BASE_DIR : on part de ce fichier → on remonte à la racine
BASE_DIR = Path(__file__).resolve().parent.parent# src/bluesky_ml/ -> src/ -> racine

# 2. Dossier des modèles
MODELS_DIR = BASE_DIR / "kedro-clean" / "data" / "06_models"

def cluster_posts(
    con_string: str,
    n_posts: int,
    days_back: int | None = 7,
    date_start: str | None = None,
    date_end: str | None = None,
) -> pd.DataFrame:
    """
    Si days_back est renseigné => filtre sur NOW()-days_back.
    Sinon, si date_start/date_end sont renseignées => BETWEEN.
    """
    engine = create_engine(con_string)

    where_clauses = ["text_clean IS NOT NULL"]
    params: dict = {"n_posts": n_posts}

    if days_back is not None:
        where_clauses.append("created_at >= NOW() - (:days_back * INTERVAL '1 day')")
        params["days_back"] = days_back
    else:
        if date_start is not None:
            where_clauses.append("created_at >= :date_start")
            params["date_start"] = date_start
        if date_end is not None:
            where_clauses.append("created_at <= :date_end")
            params["date_end"] = date_end

    where_sql = " AND ".join(where_clauses)

    query = text(f"""
        SELECT mongo_id, created_at, text_clean
        FROM bluesky_posts_clean
        WHERE {where_sql}
        ORDER BY created_at DESC
        LIMIT :n_posts
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params=params)

    if df.empty:
        return df.assign(cluster=pd.Series(dtype="int64"))

    vectorizer = joblib.load(MODELS_DIR / "vectorizer.pkl")
    kmeans = joblib.load(MODELS_DIR / "kmeans.pkl")

    X = vectorizer.transform(df["text_clean"].astype(str).tolist())
    labels = kmeans.predict(X)

    df["cluster"] = labels
    return df



def summarize_clusters(
    df,
    n_top_words: int = 10,
    n_examples: int = 5,
):
    """
    Renvoie, pour chaque cluster :
      - 'top_words': liste de tuples (mot, score)
      - 'examples' : quelques textes exemple
    """
    vectorizer = joblib.load(MODELS_DIR / "vectorizer.pkl")
    feature_names = np.array(vectorizer.get_feature_names_out())

    result = {}

    for cluster_id, sub in df.groupby("cluster"):
        texts = sub["text_clean"].astype(str).tolist()
        X_sub = vectorizer.transform(texts)

        tfidf_sum = np.asarray(X_sub.sum(axis=0)).ravel()
        top_indices = tfidf_sum.argsort()[::-1][:n_top_words]

        top_words = [
            (feature_names[i], float(tfidf_sum[i]))
            for i in top_indices
        ]

        examples = texts[:n_examples]

        result[int(cluster_id)] = {
            "top_words": top_words,
            "examples": examples,
        }

    return result