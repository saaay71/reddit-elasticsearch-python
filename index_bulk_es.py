from zreader import Zreader
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import json
import datetime
from elasticsearch.helpers import BulkIndexError


elasticserach_server_ip = "ES_SERVER_IP"
index_name = "rs_2015-01" # Define the index name that you want elasticsearch to index and store the data

# create Elasticsearch instance
es = Elasticsearch(hosts=elasticserach_server_ip, request_timeout=10000)

# specify zstd zipped file path
zst_file_path = "RS_2015-01.zst"

# set bulk size
bulk_size = 15000

reader = Zreader(zst_file_path, chunk_size=1024*1024*100)

bulk_cnt = 0
docs = []
# Read each line from the reader
for line in reader.readlines():
    reddit_item = json.loads(line)

    reddit_item["@timestamp"] = datetime.datetime.fromtimestamp(reddit_item['created'])

    key_items = ["score", "created", "subreddit_id", "url", "author", "is_self", "num_comments",
                 "over_18", "name", "id", "ups", "permalink", "downs", "title", "created_utc", "subreddit",
                 "domain", "@timestamp"]

    reddit_item_data = dict()
    for key in key_items:
        if key in reddit_item:
            reddit_item_data[key] = reddit_item[key]
    # print(reddit_item_data)

    # create Elasticsearch index request
    index_request = {
        "_index": index_name,
        # "_id": reddit_item_data["name"],
        "_source": reddit_item_data
    }

    # add index request to bulk request
    docs.append(index_request)

    # If the batch size has been reached, index the documents in bulk mode
    if len(docs) == bulk_size:
        try:
            bulk(es, docs)
            print("Sending bulk {}".format(bulk_cnt))
            bulk_cnt += 1
        except BulkIndexError as e:
            print(e)
        docs = []

# If there are any remaining documents to be indexed, index them in bulk mode
if len(docs) > 0:
    bulk(es, docs)
    
print("done!")
