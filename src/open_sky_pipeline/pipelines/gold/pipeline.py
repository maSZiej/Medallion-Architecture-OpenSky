from kedro.pipeline import Pipeline, node
from .node import gold_layer
def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline([
                        node(
            func=gold_layer,
            inputs='silver',      # pobiera z catalog.yml
            outputs="S3_Postgres",   # KEDRO AUTOMATYCZNIE ZAPISUJE DO S3/MinIO
            name='gold_layer'
        )])