from kedro.pipeline import Pipeline, node
from kedro_clean.pipelines.nodes_ml import train_clustering_model

def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        [
            node(
                func=train_clustering_model,
                inputs="params:postgres_con_string",
                outputs=None,
                name="train_clustering_model",
            ),
        ]
    )
