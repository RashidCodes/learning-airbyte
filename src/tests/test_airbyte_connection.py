from airbyte.airbyte import AirbyteConnection 

def test_airbyte_job_status():

    connection_id = 'ea6ed80f-6f84-4907-ad16-9581ce8661b9'
    airbyte_connection = AirbyteConnection(connection_id=connection_id)
    
    assert airbyte_connection.run() == True 


