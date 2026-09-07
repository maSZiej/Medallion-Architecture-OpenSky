from kedro.pipeline import Pipeline, node
from .node import gold_layer
def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline([
                        node(
            func=gold_layer,
            inputs='silver',      
            outputs="S3_Postgres",   
            name='gold_layer'
        )])