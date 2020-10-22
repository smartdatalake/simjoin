import requests
import json
from time import sleep
from datasources import datasource1
              
result = requests.post('http://localhost:8080/simjoin/api/addsource',
                       data=json.dumps(datasource1),
                       headers={'Content-Type':'application/json',
                                'accept': 'application/json'})
              
print(result.json())
api_key = result.headers['id']
ds_id = result.json()[0]['id']

result = requests.post('http://localhost:8080/simjoin/api/addsource',
                       data=json.dumps(datasource1),
                       headers={'Content-Type':'application/json',
                                'accept': 'application/json'})
              
print(result.json())
api_key2 = result.headers['id']
ds_id2 = result.json()[0]['id']

join_json = {
              "limit": 50,
              "params": {
                "query_dataSource": ds_id,
                "input_dataSource": ds_id2,
                "join_type": "knn",
                "k": 2,
                "max_lines": 100,
                "threshold": 0
              }
            } 

result = requests.post('http://localhost:8080/simjoin/api/startjoin',
                       data=json.dumps(join_json),
                       headers={'Content-Type':'application/json',
                                'accept': 'application/json',
                                'api_key': api_key,
                                'api_key2': api_key2
                                })
              
print(result.json())