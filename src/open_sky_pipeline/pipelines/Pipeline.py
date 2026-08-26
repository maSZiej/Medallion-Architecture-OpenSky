# # src/twoj_projekt/pipeline.py
# from kedro.pipeline import Pipeline, node
# from .Bronze.node import read_data_from_api
# from .Silver.node import silver_node_hist,silver_node
# from .Gold.node import gold_layer
# def create_pipeline(**kwargs) -> Pipeline:
#     return Pipeline([
#         node(
#             func=read_data_from_api,
#             inputs=None,       # pobiera z catalog.yml
#             outputs="bronze",   # KEDRO AUTOMATYCZNIE ZAPISUJE DO S3/MinIO
#             name="bronzee"
#         ),
#                 node(
#             func=silver_node_hist,
#             inputs=None,       # pobiera z catalog.yml
#             outputs="silver_hist",   # KEDRO AUTOMATYCZNIE ZAPISUJE DO S3/MinIO
#             name="save_to_hist"
#         ),        
#                 node(
#             func=silver_node,
#             inputs="bronze",       # pobiera z catalog.yml
#             outputs="silver",   # KEDRO AUTOMATYCZNIE ZAPISUJE DO S3/MinIO
#             name="clean_aircraft_node"
#         ),
#                         node(
#             func=gold_layer,
#             inputs=[],
#             # inputs=["silver","silver_hist"],       # pobiera z catalog.yml
#             outputs="silver_aircraft",   # KEDRO AUTOMATYCZNIE ZAPISUJE DO S3/MinIO
#             name="gold_aircraft_node"
#         )
        
    
#     ])