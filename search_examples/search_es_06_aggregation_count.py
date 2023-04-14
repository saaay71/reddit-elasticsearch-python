from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
import matplotlib.pyplot as plt
plt.style.use('ggplot')


elasticserach_server_ip = "ES_SERVER_IP"
index_name = "rc_2015-01"

# Connect to Elasticsearch
es = Elasticsearch(elasticserach_server_ip)

# Define the query string from the user
query_string = "youtube"

# Define the time range for the query
# start_time = datetime.now() - timedelta(days=7)
# end_time = datetime.now()
start_time = datetime(2015, 1, 1)
end_time = datetime(2015, 2, 1)

# Define the histogram interval in 1-hour timespan
interval = '1h'

# Define the Elasticsearch query
query = {
    "query": {
        "bool": {
            "must": [
                {"match": {"body": query_string}},
                {"range": {"@timestamp": {"gte": start_time, "lte": end_time}}}
            ]
        }
    },
    "aggs": {
        "histogram": {
            "date_histogram": {
                "field": "@timestamp",
                "interval": interval
            }
        }
    }
}

# Execute the query and retrieve the results
response = es.search(index=index_name, body=query, size=0)
# print(response)
# # Print the histogram
# for bucket in response['aggregations']['histogram']['buckets']:
#     print(f"{datetime.fromtimestamp(bucket['key']/1000)}: {bucket['doc_count']}")

# Extract the histogram data
histogram = {}
for bucket in response['aggregations']['histogram']['buckets']:
    timestamp = bucket['key'] / 1000
    count = bucket['doc_count']
    histogram[timestamp] = count

# Create a sorted list of timestamps and counts
timestamps = sorted(histogram.keys())
counts = [histogram[t] for t in timestamps]

# Convert the timestamps to datetime objects
timestamps = [datetime.fromtimestamp(t) for t in timestamps]

# Plot the histogram
fig, ax = plt.subplots()
ax.plot(timestamps, counts, color='blue')
ax.set_xlabel('Time', fontsize=12)
ax.set_ylabel('Count')
title = "Histogram \"" + query_string + "\" " +interval
ax.set_title(title)
fig.autofmt_xdate(rotation=45)
fileName = title.replace("\"", "").replace(" ", "_")
plt.savefig("./plots/"+fileName+".pdf")

plt.show()

