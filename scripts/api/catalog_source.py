import requests
import json
from datasources import ds_csv_1
              
result = requests.post('http://localhost:8080/simjoin/api/addsource',
                       data=json.dumps(ds_csv_1),
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