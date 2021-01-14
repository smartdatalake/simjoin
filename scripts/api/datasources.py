import os

ds_csv_1 = [{
              "csv": {
                "colSetId": 2,
                "colSetTokens": 4,
                "columnDelimiter": ";",
                "file": os.getcwd()+"/dblp_sample.csv",
                "header": "true"
              },
              "mode": "fuzzy",
              "name": "DBLP_CSV_FUZZY",
              "qgram": 0,
              "tokenDelimiter": " ",
              "tokenizer": "word",
              "type": "csv"
              }]
              
ds_jdbc = [{
                "jdbc": {
                "db": "DBLP",
                "keyCol": "author_name",
                "pwd": "root",
                "tokensCol": "paper_title",
                "url": "jdbc:postgresql://localhost:5432/SDL",
                "user": "postgres"
              },
              "mode": "fuzzy",
              "name": "DBLP_POSTGRES_FUZZY",
              "qgram": "0",
              "tokenDelimiter": " ",
              "tokenizer": "word",
              "type": "jdbc"
            }]
              
ds_csv_2 = [{
              "csv": {
                "colSetId": 2,
                "colSetTokens": 4,
                "columnDelimiter": ";",
                "file": os.getcwd()+"/dblp_sample.csv",
                "header": "true"
              },
              "prepare": {
                 "max_lines": 1000,
              },
              "mode": "fuzzy",
              "name": "DBLP_CSV_FUZZY",
              "qgram": 0,
              "tokenDelimiter": " ",
              "tokenizer": "word",
              "type": "csv"
              }]

ds_es = [{
        "es": {
            "url":"localhost:9200",
            "index":"danae-eodp",
            "colSetTokens":"metadata.title",
            },
        
        
        "type": "es",
        "tokenizer":"word",
        "qgram":3,
        "mode":"standard",
        
        
        "tokenDelimiter":" ",
    }]

ds_json = [{
       "type":"json",
       "tokenizer":"word",
       "qgram":3,
       "mode":"standard",
       "tokenDelimiter":" ",
       "json" : {
           "values":[
              {
                 "id":"id1",
                 "set":"this is an example"
              },
              {
                 "id":"id2",
                 "set":"this is another example"
              }
           ]
       }
    }]
