from kedro.pipeline import Pipeline, node
from .node import silver_node,silver_node_hist
def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline([
                node(
            func=silver_node_hist,
            inputs="bronze_layer",      
            outputs="silver_history",   
            name="save_to_hist"
        ),        
                node(
            func=silver_node,
            inputs=["bronze_layer","silver_history"],       
            outputs="silver",   
            name="clean_aircraft_node"
        )])