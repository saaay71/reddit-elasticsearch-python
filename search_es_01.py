from elasticsearch import Elasticsearch

elasticserach_server_ip = "ES_SERVER_IP"
index_name = "rc_2015-01"

# Connect to es
es = Elasticsearch(hosts=elasticserach_server_ip)

# Make query request
query_response = es.search(index=index_name, query={"match_all": {}})

print("Got %d Hits:" % query_response['hits']['total']['value'])
for hit in query_response['hits']['hits']:
    print(hit["_source"])
