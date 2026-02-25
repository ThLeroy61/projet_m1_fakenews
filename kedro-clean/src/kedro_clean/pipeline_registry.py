from kedro.pipeline import pipeline
from kedro_clean.pipelines.pipeline import create_pipeline
from kedro_clean.pipelines import pipeline_ml


def register_pipelines():
    return {
        "__default__": create_pipeline(),
        "ml": pipeline_ml.create_pipeline(),
        "data_engineering": create_pipeline(),
    }