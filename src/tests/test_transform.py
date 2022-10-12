import yaml 
from s3.etl.transform import Transform 
from database.postgres import PostgresDB


def test_transformation():

    """ Test the transformation using Transform.run() """

    with open('config.yaml', 'r') as config_file:
        config = yaml.safe_load(config_file) 


    table_name = 'serving_coin_price_history'
    engine = PostgresDB.create_pg_engine(db_target='target')
    models_path = config.get('transform').get('model_path')

    test_transform = Transform(model=table_name, engine=engine, models_path=models_path)

    assert test_transform.run() == True

