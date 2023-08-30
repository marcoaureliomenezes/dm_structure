from datetime import datetime
import os, subprocess, json
from scripts.python.redis_api import RedisAPI
import subprocess, json



class Ingestor:

    def __init__(self, redis_client):
        self.redis_client = redis_client


    def __get_last_key(self):
        redis_keys = self.redis_client.list_keys()
        last_file = [int(i.split('_')[-1].split('.')[0]) for i in redis_keys]
        max_index = last_file.index(max(last_file))
        key = redis_keys[max_index]
        return key
    
    def get_data(self):
        key = self.__get_last_key()
        data = self.redis_client.get_key(key)
        return data
    
    def write_json(self, data, path):
        with open(path, 'w') as file:
            file.write(json.dumps(data))

    def delete_json(self, path):
        subprocess.run(["rm", path])
        #self.redis_client.delete_key(path.split('/')[-1])

    def form_tmp_filename(self, data):
        timestamp_start, timestamp_end = data[0]['timeStamp'], data[-1]['timeStamp']
        odate = datetime.fromtimestamp(int(timestamp_start)).strftime('%Y%m%d')
        path = f'transactions_{timestamp_start}_to_{timestamp_end}_{odate}.json'
        return path
    
class HadoopIngestor(Ingestor):
    
    def __init__(self, redis_client, container):
        super().__init__(redis_client)
        self.container = container


    def upload_to_hadoop(self, path, directory):
        directory = os.path.join('/', self.container, directory)
        subprocess.run(["hdfs", "dfs", "-mkdir", "-p", directory])
        subprocess.run(["hdfs", "dfs", "-put", path, directory])
        self.delete_json(path)


    def ingest(self, directory='/user/hadoop/'):
        data = self.get_data()
        path = os.path.join("/tmp", self.form_tmp_filename(data))
        self.write_json(data, path)
        self.upload_to_hadoop(path, directory=directory)


def run_ingestor(network, contract):


    container_name = network
    redis_client = RedisAPI(host='redis', port=6379)

    blob_ingestor = HadoopIngestor(redis_client, container_name)
    blob_ingestor.ingest(f'batch/aave_v2')
    print(f"Successfully ingested data to HADOOP {container_name}")