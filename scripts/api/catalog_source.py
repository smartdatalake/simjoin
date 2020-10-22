import requests
import json
from datasources import datasource1
              
result = requests.post('http://localhost:8080/simjoin/api/addsource',
                       data=json.dumps(datasource1),
                       headers={'Content-Type':'application/json',
                                'accept': 'application/json'})
              
print(result.json())
api_key = result.headers['id']
             
result = requests.post('http://localhost:8080/simjoin/api/catalog',
                       data=json.dumps({}),
                       headers={'Content-Type':'application/json',
                                'accept': 'application/json',
                                'api_key': api_key})
print(result.json())              