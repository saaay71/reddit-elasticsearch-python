from elasticsearch import Elasticsearch
from datetime import datetime


elasticserach_server_ip = "ES_SERVER_IP"
index_name = "rc_2015-01"

# Connect to Elasticsearch
es = Elasticsearch(elasticserach_server_ip)

# Define the date range
start_date = datetime(2015, 1, 1)
end_date = datetime(2015, 2, 1)

search_string = "youtube facebook"

timestmap_field = "@timestamp"
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
            "body": {
              "query": search_string
            }
          }
        }
      ]
    }
}

# Search the index
query_response = es.search(index=index_name, query=query, track_total_hits=True, size=10)

# Print the results
print("Got %d Hits:" % query_response['hits']['total']['value'])
for hit in query_response['hits']['hits']:
    print(hit["_source"])
