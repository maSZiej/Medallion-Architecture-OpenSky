from kedro.pipeline import Pipeline, node
from .node import gold_layer
def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline([
                        node(
            func=gold_layer,
            inputs='silver_ret',
            # inputs=["silver","silver_hist"],       # pobiera z catalog.yml
            outputs="gold_ret",   # KEDRO AUTOMATYCZNIE ZAPISUJE DO S3/MinIO
            name="gold_aircraft_node"
        )])