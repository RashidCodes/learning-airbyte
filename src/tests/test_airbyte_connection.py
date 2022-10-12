import yaml
from airbyte.airbyte import AirbyteConnection 

def test_airbyte_job_status():

    
    with open('config.yaml', 'r') as config_file:
        config = yaml.safe_load(config_file) 
        
    connection_id = config.get('extract_load').get('connection_id')
    airbyte_connection = AirbyteConnection(connection_id=connection_id)
    
    assert airbyte_connection.run() == True 


