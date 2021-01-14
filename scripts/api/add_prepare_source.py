import requests
import json
from datasources import ds_csv_2

result = requests.post('http://localhost:8080/simjoin/api/addsource',
                       data=json.dumps(ds_csv_2),
                       headers={'Content-Type':'application/json',
                                'accept': 'application/json'})
              
print(result.json())              
