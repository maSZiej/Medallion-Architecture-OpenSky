from kedro.pipeline import Pipeline, node
from .node import read_data_from_api

def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline([
        node(
            func=read_data_from_api,
            inputs=None,       # pobiera z catalog.yml
            outputs="bronze_ret",   # KEDRO AUTOMATYCZNIE ZAPISUJE DO S3/MinIO
            name="bronzee"
        )])