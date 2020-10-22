import os

datasource1 = [{
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
              
datasource2 = [{
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
              