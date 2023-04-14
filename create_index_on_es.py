from zreader import Zreader
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import json
import datetime

elasticserach_server_ip = "ES_SERVER_IP"
index_name = "rs_2015-01" # Define the index name that you want elasticsearch to index and store the data

# create Elasticsearch instance
es = Elasticsearch(hosts=elasticserach_server_ip)

# specify zstd zipped file path
zst_file_path = "RS_2015-01.zst"

reader = Zreader(zst_file_path, chunk_size=16384)

docs = []
# Read each line from the reader
for line in reader.readlines():
    reddit_item = json.loads(line)
    reddit_item["@timestamp"] = datetime.datetime.fromtimestamp(reddit_item['created'])#.strftime("%Y-%m-%d %H:%M:%S")

    key_items = ["score", "created", "subreddit_id", "url", "author", "is_self", "num_comments",
                 "over_18", "name", "id", "ups", "permalink", "downs", "title", "created_utc", "subreddit",
                 "domain", "@timestamp"]

    reddit_item_data = dict()
    for item in key_items:
        reddit_item_data[item] = reddit_post[item]
    # print(reddit_post_data)

    es.index(index=index_name, document=reddit_item_data)
    break #just first item so we create the index and the @timestamp will be considered as "date"

print("done!")
