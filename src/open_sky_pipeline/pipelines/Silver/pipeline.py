from kedro.pipeline import Pipeline, node
from .node import silver_node,silver_node_hist
def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline([
                node(
            func=silver_node_hist,
            inputs=None,       # pobiera z catalog.yml
            outputs="silver_hist",   # KEDRO AUTOMATYCZNIE ZAPISUJE DO S3/MinIO
            name="save_to_hist"
        ),        
                node(
            func=silver_node,
            inputs="bronze",       # pobiera z catalog.yml
            outputs="silver",   # KEDRO AUTOMATYCZNIE ZAPISUJE DO S3/MinIO
            name="clean_aircraft_node"
        )])