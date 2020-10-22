import requests
import json
from datasources import datasource1, datasource2
              
result = requests.post('http://localhost:8080/simjoin/api/addsource',
                       data=json.dumps(datasource1),
                       headers={'Content-Type':'application/json',
                                'accept': 'application/json'})
              
print(result.json())
api_key = result.headers['id']
print('Private API key is {}'.format(api_key))

result = requests.post('http://localhost:8080/simjoin/api/appendsource',
                       data=json.dumps(datasource2),
                       headers={'Content-Type':'application/json',
                                'accept': 'application/json',
                                'api_key': api_key})
              
print(result.json())    
