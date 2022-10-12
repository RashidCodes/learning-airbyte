from airbyte.airbyte import AirbyteConnection
from graphlib import TopologicalSorter
from database.postgres import PostgresDB
from s3.etl.transform import Transform 
from io import StringIO
from utility.metadata_logging import MetadataLogging
import os 
import yaml 
import logging
import datetime as dt 



def run_pipeline():
    # set up logging 
    run_log = StringIO()
    logging.basicConfig(stream=run_log,level=logging.INFO, format="[%(levelname)s][%(asctime)s]: %(message)s")

    # set up metadata logger 
    metadata_logger = MetadataLogging(db_target="target")

    # configure pipeline 
    with open("config.yaml") as stream:
        config = yaml.safe_load(stream)

    path_transform_model = config.get('transform').get('model_path')
    airbyte_host = config["extract_load"]["host"]
    airbyte_connection_id = config["extract_load"]["connection_id"]
    target_engine = PostgresDB.create_pg_engine(db_target="target")
    
    metadata_log_table = config["meta"]["log_table"]
    metadata_log_run_id = metadata_logger.get_latest_run_id(db_table=metadata_log_table)
    metadata_logger.log(
        run_timestamp=dt.datetime.now(),
        run_status="started",
        run_id=metadata_log_run_id, 
        run_config=config,
        db_table=metadata_log_table
    )

    try: 
        logging.info("Creating extract and load nodes")
        # build dag 
        dag = TopologicalSorter()

        # extract load node 
        airbyte_connection = AirbyteConnection(host=airbyte_host, connection_id=airbyte_connection_id)
        dag.add(airbyte_connection)
        
        # transform nodes  
        logging.info("Creating transform nodes")
        node_staging_orders = Transform("happiness", engine=target_engine, models_path=path_transform_model)
        dag.add(node_staging_orders, airbyte_connection)
        
        # run dag 
        logging.info("Executing DAG")
        dag_rendered = tuple(dag.static_order())
        for node in dag_rendered: 
            node.run()
        
        logging.info("Pipeline run successful")
        metadata_logger.log(
            run_timestamp=dt.datetime.now(),
            run_status="completed",
            run_id=metadata_log_run_id, 
            run_config=config,
            run_log=run_log.getvalue(),
            db_table=metadata_log_table
        )
    except Exception as e: 
        logging.exception(e)
        metadata_logger.log(
            run_timestamp=dt.datetime.now(),
            run_status="error",
            run_id=metadata_log_run_id, 
            run_config=config,
            run_log=run_log.getvalue(),
            db_table=metadata_log_table
        )
    print(run_log.getvalue())

if __name__ == "__main__":
    run_pipeline()
