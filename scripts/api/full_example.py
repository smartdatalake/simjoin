import requests
import json
from time import sleep
from datasources import datasource1, datasource2

              
result = requests.post('http://localhost:8080/simjoin/api/addsource',
                       data=json.dumps(datasource1),
                       headers={'Content-Type':'application/json',
                                'accept': 'application/json'})
              
print(result.json())              
id1 = result.json()[0]['id']
api_key = result.headers['id']

result = requests.post('http://localhost:8080/simjoin/api/appendsource',
                       data=json.dumps(datasource2),
                       headers={'Content-Type':'application/json',
                                'accept': 'application/json',
                                'api_key': api_key})
              
print(result.json())    
id2 = result.json()[0]['id']

result = requests.post('http://localhost:8080/simjoin/api/catalog',
                       data=json.dumps({}),
                       headers={'Content-Type':'application/json',
                                'accept': 'application/json',
                                'api_key': api_key})
              
print(result.json()) 

rm_json = { "id": id2}  

result = requests.post('http://localhost:8080/simjoin/api/removesource',
                       data=json.dumps(rm_json),
                       headers={'Content-Type':'application/json',
                                'accept': 'application/json',
                                'api_key': api_key})
              
print(result.json())    

result = requests.post('http://localhost:8080/simjoin/api/catalog',
                       data=json.dumps({}),
                       headers={'Content-Type':'application/json',
                                'accept': 'application/json',
                                'api_key': api_key})
              
print(result.json())     

join_json = {
              "limit": 50,
              "params": {
                "input_dataSource": id1,
                "join_type": "knn",
                "k": 2,
                "max_lines": 100000,
                "threshold": 0
              }
            } 

result = requests.post('http://localhost:8080/simjoin/api/startjoin',
                       data=json.dumps(join_json),
                       headers={'Content-Type':'application/json',
                                'accept': 'application/json',
                                'api_key': api_key})
              
print(result.json())    

get_json = { 'id': result.json()['id']}

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