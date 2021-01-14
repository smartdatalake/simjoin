import requests
import json
from datasources import ds_csv_1
              
result = requests.post('http://localhost:8080/simjoin/api/addsource',
                       data=json.dumps(ds_csv_1),
                       headers={'Content-Type':'application/json',
                                'accept': 'application/json'})
              
print(result.json())
api_key = result.headers['id']
ds_id = result.json()[0]['id']

join_json = {
              "limit": 50,
              "params": {
                "input_dataSource": ds_id,
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
                                'api_key': api_key})
              
print(result.json())    