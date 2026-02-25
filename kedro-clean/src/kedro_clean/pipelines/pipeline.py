from kedro.pipeline import Pipeline, node
from kedro_clean.pipelines.nodes import (
    load_mongo_posts,
    extract_text_signals,
    clean_text,
    validate_quality,
    inspect_df,
    upsert_posts
)

def create_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=load_mongo_posts,
                inputs="mongo_posts",
                outputs="posts_loaded",
                name="load_mongo_posts",
            ),
            node(
                func=inspect_df,
                inputs="posts_loaded",
                outputs="posts_loaded_debug",
                name="inspect_posts_loaded",
            ),
            node(
                func=extract_text_signals,
                inputs="posts_loaded_debug",
                outputs="posts_signals",
                name="extract_text_signals",
            ),
            node(
                func=clean_text,
                inputs="posts_signals",
                outputs="posts_cleaned",
                name="clean_text",
            ),
            node(
                func=validate_quality,
                inputs="posts_cleaned",
                outputs="posts_ready",
                name="validate_quality",
            ),
            node(
                func=upsert_posts,
                inputs=["posts_ready", "params:postgres_con_string"],
                outputs="postgres_posts_final",
                name="upsert_into_postgres",
            )
        ]
    )
