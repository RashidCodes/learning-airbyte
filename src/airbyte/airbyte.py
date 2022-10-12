import time 
import requests
import logging 


class AirbyteConnection():

    def __init__(self, connection_id: str, host: str='localhost:8000', poll_time: int=5):
        self.host = host 
        self.connection_id = connection_id 
        self.poll_time = poll_time 

    
    def run(self) -> bool:

        """
        Trigger an airbyte connection 

        Parameters 
        -----------
        connection_id: str 
            The connection id of the connection 

        Returns
        -------
        bool 

        """

        # A post request to this url is used to trigger connections
        # See https://airbyte-public-api-docs.s3.us-east-2.amazonaws.com/rapidoc-api-docs.html#post-/v1/connections/sync
        url = f"http://{self.host}/api/v1/connections/sync"
        data = {
            "connectionId": self.connection_id
        }
        sync_response = requests.post(url=url, json=data)

        try:
            job_id = sync_response.json()["job"]["id"]
            job_status = 'running'

        except BaseException as err:
            logging.exception(f"An error occurred: {err}")

        else:
            # keep polling the api for the status of the run 
            while job_status == 'running':

                time.sleep(self.poll_time)

                # get the status job 
                url = f"http://{self.host}/api/v1/jobs/get"
                data = {
                    "id": job_id
                }

                job_response = requests.post(url, json=data)
                
                if job_response.status_code == 200:
                    job_status = job_response.json().get('job').get('status') 

                    if job_status == 'failure':
                        raise Exception(f'Run failed: {job_response.text}')

            return True 

                


