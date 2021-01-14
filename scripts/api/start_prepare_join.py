import requests
import json
from datasources import ds_csv_2, ds_csv_1
from time import sleep
              
result = requests.post('http://localhost:8080/simjoin/api/addsource',
                       data=json.dumps(ds_csv_2),
                       headers={'Content-Type':'application/json',
                                'accept': 'application/json'})
              
print(result.json())
api_key = result.headers['id']
input_id = result.json()[0]['id']

print('Private API key is {}'.format(api_key))
result = requests.post('http://localhost:8080/simjoin/api/appendsource',
                       data=json.dumps(ds_csv_1),
                       headers={'Content-Type':'application/json',
                                'accept': 'application/json',
                                'api_key': api_key})

print(result.json())
api_key = result.headers['id']
query_id = result.json()[0]['id']

sleep(5)

join_json = {
              "limit": 50,
              "params": {
                "input_dataSource": input_id,
                "query_dataSource": query_id,
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

sleep(3)

result = requests.post('http://localhost:8080/simjoin/api/getstatus',
                       data=json.dumps(get_json),
                       headers={'Content-Type':'application/json',
                                'accept': 'application/json',
                                'api_key': api_key})
print(result.json())    

sleep(10)

join_json['params']['k'] = 4

result = requests.post('http://localhost:8080/simjoin/api/startjoin',
                       data=json.dumps(join_json),
                       headers={'Content-Type':'application/json',
                                'accept': 'application/json',
                                'api_key': api_key})
              
print(result.json())    
job_id = result.json()['id']
get_json = { 'id': job_id}

sleep(3)

result = requests.post('http://localhost:8080/simjoin/api/getstatus',
                       data=json.dumps(get_json),
                       headers={'Content-Type':'application/json',
                                'accept': 'application/json',
                                'api_key': api_key})
print(result.json())  