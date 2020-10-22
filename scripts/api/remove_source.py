import requests
import json
from datasources import datasource1
              
result = requests.post('http://localhost:8080/simjoin/api/addsource',
                       data=json.dumps(datasource1),
                       headers={'Content-Type':'application/json',
                                'accept': 'application/json'})
              
print(result.json())     

api_key = result.headers['id']
print('Private API key is {}'.format(api_key))

ds_id = { "id": result.json()[0]['id']}        
 

result = requests.post('http://localhost:8080/simjoin/api/removesource',
                       data=json.dumps(ds_id),
                       headers={'Content-Type':'application/json',
                                'accept': 'application/json',
                                'api_key': api_key})
              
print(result.json())    