import json
import redis

class RedisAPI:

    def __init__(self, host, port):
        self.redis = redis.Redis(host=host, port=port, decode_responses=True)
     
    
    def register_key(self, key, value):
        self.redis.set(key, json.dumps(value))


    def get_key(self, key):
        data = self.redis.get(key)
        if data is None:
            return []
        return json.loads(data)
    
    
    def delete_key(self, key):
        self.redis.delete(key)


    def delete_keys(self):
        self.redis.flushall()


    def list_keys(self):
        return self.redis.keys()
   
    def get_dict(self, key):
        data = self.redis.get(key)
        if data is None:
            return {}
        return json.loads(data)


    
        