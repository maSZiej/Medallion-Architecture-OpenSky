from kedro.pipeline import Pipeline, node
from .node import gold_layer
def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline([
                        node(
            func=gold_layer,
            inputs=[],
            # inputs=["silver","silver_hist"],       # pobiera z catalog.yml
            outputs="silver_aircraft",   # KEDRO AUTOMATYCZNIE ZAPISUJE DO S3/MinIO
            name="gold_aircraft_node"
        )])