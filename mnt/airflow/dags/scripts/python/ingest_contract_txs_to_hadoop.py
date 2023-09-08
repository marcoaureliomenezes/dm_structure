import json, os, subprocess
from datetime import datetime as dt
from caixa_de_ferramentas.redis_client_api import RedisAPI



class Ingestor:

    def __init__(self, redis_client):
        self.redis_client = redis_client


    def form_filename(self, data):
        timestamp_start, timestamp_end = data[0]['timeStamp'], data[-1]['timeStamp']
        odate = dt.fromtimestamp(int(timestamp_start)).strftime('%Y%m%d')
        path = f'transactions_{timestamp_start}_to_{timestamp_end}_{odate}.json'
        return path
    

    def gen_redis_key(self, contract_name, start_date):
        redis_key = f'{contract_name}_{start_date}'
        return redis_key
    


class HadoopIngestor:
    
    def __init__(self, redis_client, container, contract_name, start_date):
        self.contract_name = contract_name
        self.start_date = start_date
        self.redis_client = redis_client
        self.container = container
        self.ingestor = Ingestor(redis_client)


    def upload_to_hadoop(self, path, directory):
        print(f"Uploading TAUAN {path} to HADOOP")
        directory = os.path.join('/', self.container, directory)
        print(f"Uploading TAUAN {path} to HADOOP {directory}")
        subprocess.run(["hdfs", "dfs", "-mkdir", "-p", directory])
        subprocess.run(["hdfs", "dfs", "-put", path, directory])
        subprocess.run(["hdfs", "dfs", "-ls", directory])
        subprocess.run(["rm", path])


    def write_json(self, data, path):
        with open(path, 'w') as file:
            file.write(json.dumps(data))

            
    def ingest(self, directory='/user/hadoop/'):
        odate = dt.strftime(dt.strptime(self.start_date, '%Y-%m-%d'), '%Y%m%d')
        key = self.ingestor.gen_redis_key(self.contract_name, odate)
        data = self.redis_client.get_key(key)
        if len(data) == 0: 
            print(f"Data for {key} is empty")
            return
        path = os.path.join("/tmp", self.ingestor.form_filename(data))
        print(data)
        self.write_json(data, path)
        self.upload_to_hadoop(path, directory=directory)



def run_ingestor(network, contract_name, start_date):

    container_name = network
    redis_client = RedisAPI(host='redis', port=6379)
    blob_ingestor = HadoopIngestor(redis_client, container_name, contract_name, start_date)
    blob_ingestor.ingest(f'batch/{contract_name}')
    print(f"Successfully ingested data to HADOOP {container_name}")