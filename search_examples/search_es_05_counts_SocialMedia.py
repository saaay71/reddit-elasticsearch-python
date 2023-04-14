from elasticsearch import Elasticsearch
from datetime import datetime
from itertools import combinations


elasticserach_server_ip = "ES_SERVER_IP"
index_name = "rc_2015-01"


# Connect to Elasticsearch
es = Elasticsearch(elasticserach_server_ip)

# Define the date range
start_date = datetime(2015, 1, 1)
end_date = datetime(2015, 2, 1)

timestmap_field = "@timestamp"
search_field = "body" # name of the field we want to do the searching

def get_number_of_occurance(search_string):
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

    # # Search the index using count API
    # query_response = es.count(index=index_name, q=search_field+":"+search_string)
    # print("Number of counts: %d" % query_response["count"])

    # Search the index using search API
    # 'track_total_hits' will always track the number of hits that match the query accurately
    # more info: https://www.elastic.co/guide/en/elasticsearch/reference/7.17/search-your-data.html#track-total-hits
    # 'size=0' means it will not return any of the documents
    query_response = es.search(index=index_name, query=query, track_total_hits=True, size=0)
    # print("Number of counts: %d" % query_response["hits"]["total"]["value"])
    return query_response["hits"]["total"]["value"]


websites = []
with open("social_websites.txt", "r", encoding="utf-8") as f:
    websites =  f.readlines()
websites = [w.strip() for w in websites]

websites_comb = list(combinations(websites, 2))

queries = []
queries += ["\""+w+"\"" for w in websites] # website alone
queries += ["\""+w[0]+"\" \""+w[1]+"\"" for w in websites_comb] # websites together
queries += ["\""+w[0]+"\" -\""+w[1]+"\"" for w in websites_comb] # first website excluding seconding website
queries += ["\""+w[1]+"\" -\""+w[0]+"\"" for w in websites_comb] # second website exclusing first website

print("Total number of queries: %d" % len(queries))

# Make request to ES and get the number of occurances
for i in range(len(queries)):
    query_count = get_number_of_occurance(queries[i])
    print(queries[i], ": ", query_count)
