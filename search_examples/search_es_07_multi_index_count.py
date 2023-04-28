from elasticsearch import Elasticsearch
from datetime import datetime

elasticserach_server_ip = "ES_SERVER_IP"
indices_names = ["rc_2015-01", "rc_2016-01"]

# Connect to Elasticsearch
es = Elasticsearch(elasticserach_server_ip)

# Define the date range
start_date = datetime(2015, 1, 1)
end_date = datetime(2016, 2, 1)

timestmap_field = "@timestamp"
search_string = "youtube facebook"

search_field = "body" # name of the field we want to do the searching


# Build the query
query = {
    "bool": {
      "must": [
        {
          "range": {
            timestmap_field: {
              "gte": start_date,
              "lte": end_date,
              # "boost": 2
            }
          }
        },
        {
          "match": {
            search_field: {
              "query": search_string
            }
          }
        }
      ]
    }
}


query_response = es.search(index=indices_names, query=query, track_total_hits=True, size=0)
print("Number of counts: %d" % query_response["hits"]["total"]["value"])
