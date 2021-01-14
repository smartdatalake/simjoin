import requests
import json
from datasources import ds_csv_1, ds_jdbc

result = requests.post('http://localhost:8080/simjoin/api/addsource',
                       data=json.dumps(ds_csv_1),
                       headers={'Content-Type':'application/json',
                                'accept': 'application/json'})
              
print(result.json())              

result = requests.post('http://localhost:8080/simjoin/api/addsource',
                       data=json.dumps(ds_jdbc),
                       headers={'Content-Type':'application/json',
                                'accept': 'application/json'})
              
print(result.json())    
