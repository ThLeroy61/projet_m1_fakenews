import re
import unicodedata
import pandas as pd
import json
from sqlalchemy import create_engine, text

#Récupération de toutes les données de MongoDB ; traitement des types de données dict et ObjectID en str
def load_mongo_posts(df):
    df = df.copy()

    # FIX NUL BYTES - AVANT tout traitement pandas/SQL
    

    df.rename(columns={"_id": "mongo_id"}, inplace=True)
    
    if "mongo_id" in df.columns:
        df["mongo_id"] = df["mongo_id"].astype(str)
        
    dict_columns = df.select_dtypes(include=["object"]).columns
    for col in dict_columns:
        # On teste si la colonne contient au moins un dict, si c'est le cas on le transforme en json
        if df[col].apply(lambda x: isinstance(x, dict)).any():
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)
            
    if "created_at" in df.columns:
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")

    #Transformation des dates au format date
    if "fetched_at" in df.columns:
        df["fetched_at"] = pd.to_datetime(df["fetched_at"], errors="coerce")

    return df

#Nettoyage des informations - Suppression des URL, mentions, hashtags, emojis, retirer la ponctuation mais conserver celle qui est nécessaire
def clean_text(df):
    df = df.copy()

    def normalize_text(text):
        if not isinstance(text, str):
            return ""

        # Normalisation unicode
        text = unicodedata.normalize("NFKC", text)
        text = text.lower()

        # Suppression des URLs
        text = re.sub(r"http\S+|www\S+", "", text)

        # Suppression des mentions @user
        text = re.sub(r"@\w+", "", text)

        # Suppression des hashtags (on garde le mot)
        text = re.sub(r"#(\w+)", r"\1", text)

        # Suppression des emojis (plages unicode)
        text = re.sub(
            "[" 
            "\U0001F600-\U0001F64F"  # emoticons
            "\U0001F300-\U0001F5FF"  # symbols & pictographs
            "\U0001F680-\U0001F6FF"  # transport & map symbols
            "\U0001F1E0-\U0001F1FF"  # flags
            "]+",
            "",
            text,
        )

        # Suppression ponctuation faible (on garde ! ? .)
        text = re.sub(r"[^\w\s!?\.]", " ", text)

        # Suppression espaces multiples
        text = re.sub(r"\s+", " ", text).strip()

        return text

    df["text_clean"] = df["text_raw"].apply(normalize_text)

    return df

#Gestion des posts : Suppression des textes trop courts ; suppression des textes nuls ; des doublons exacts
def validate_quality(
    df,
    min_length = 15,
    allowed_languages = ("fr", "en"),
):

    df = df.copy()

    df = df[df["text_clean"].notna()]
    df = df[df["text_clean"].str.len() >= min_length]

    if "lang" in df.columns:
        df = df[df["lang"].isin(allowed_languages)]

    df = df.drop_duplicates(subset="text_clean")
    df = df.reset_index(drop=True)

    return df


URL_RE = re.compile(r"http\S+|www\S+")

#Récupération d'informations supplémentaires pouvant être utiles pour le modèle IA : Nombre de liens, nombre de !?, 
#longueur du texte, nombre de mots, nombre de majuscules, ratio min/maj, longueur moyenne des mots
def extract_text_signals(df):
    """
    Extraction de signaux statistiques et stylistiques
    utiles pour la détection de fake news.
    """

    df = df.copy()
    print(df)
    text = df["text_raw"].fillna("")

    df["nb_urls"] = text.str.count(URL_RE)
    df["nb_exclamations"] = text.str.count("!")
    df["nb_questions"] = text.str.count(r"\?")
    df["text_length"] = text.str.len()
    df["word_count"] = text.str.split().str.len()

    upper = text.str.count(r"[A-Z]")
    alpha = text.str.count(r"[A-Za-z]")

    df["ratio_uppercase"] = (upper / alpha).fillna(0)

    df["avg_word_length"] = (
        df["text_length"] / df["word_count"]
    ).replace([float("inf")], 0).fillna(0)

    return df

#Juste pour debug
def inspect_df(df):
    print("Columns:", df.columns.tolist())
    print(df.head(3))
    return df

def upsert_posts(df, con_string):
    engine = create_engine(con_string)

    temp_table = "bluesky_posts_clean_temp"

    # Colonnes EXACTES de la table cible
    target_cols = [
        "mongo_id",
        "fetched_at",
        "source",
        "post_id",
        "cid",
        "profile_id",
        "profile_name",
        "profile_display_name",
        "text_raw",
        "created_at",
        "engagement",
        "nb_urls",
        "nb_exclamations",
        "nb_questions",
        "text_length",
        "word_count",
        "ratio_uppercase",
        "avg_word_length",
        "text_clean",
    ]

    # Ajoute les colonnes manquantes au df (None)
    for col in target_cols:
        if col not in df.columns:
            df[col] = None

    # Garde seulement ces colonnes, dans le bon ordre
    df = df[target_cols]

    # 1) écrire dans une table temporaire
    df.to_sql(temp_table, engine, if_exists="replace", index=False)

    cols_sql = ", ".join(target_cols)

    # 2) upsert avec colonnes explicites
    with engine.begin() as conn:
        conn.execute(
            text(f"""
                INSERT INTO bluesky_posts_clean ({cols_sql})
                SELECT {cols_sql} FROM {temp_table}
                ON CONFLICT (mongo_id) DO NOTHING
            """)
        )
        conn.execute(text(f"DROP TABLE {temp_table}"))

    return df
