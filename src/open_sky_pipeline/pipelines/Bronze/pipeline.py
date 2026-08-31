from kedro.pipeline import Pipeline, node
from .node import read_data_from_api

def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline([
        node(
            func=read_data_from_api,
            inputs=None,       
            outputs="bronze_layer",   
            name="data_extraction"
        )])