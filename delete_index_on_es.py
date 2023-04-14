from elasticsearch import Elasticsearch

elasticserach_server_ip = "ES_SERVER_IP"

# Create an Elasticsearch client instance
es = Elasticsearch(hosts=elasticserach_server_ip)

# Define the name of the index to delete
index_name = 'rc_2015-01'

# Delete the index
es.indices.delete(index=index_name)

# You can check the indices here:
# http://ES_SERVER_IP:9200/_cat/indices
