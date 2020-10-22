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
job_id = result.json()['id']

get_json = { 'id': job_id}

result = requests.post('http://localhost:8080/simjoin/api/getstatus',
                       data=json.dumps(get_json),
                       headers={'Content-Type':'application/json',
                                'accept': 'application/json',
                                'api_key': api_key})
print(result.json())    

sleep(3)

result = requests.post('http://localhost:8080/simjoin/api/getstatus',
                       data=json.dumps(get_json),
                       headers={'Content-Type':'application/json',
                                'accept': 'application/json',
                                'api_key': api_key})
print(result.json())   