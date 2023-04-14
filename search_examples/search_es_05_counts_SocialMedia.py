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

def get_number_of_occurance(search_string, search_type=""):
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
    if search_type == "AND":
        terms = search_string.split(" ")
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
                        "bool": {
                            "must": [
                                {"match": {search_field: terms[0]}},
                                {"match": {search_field: terms[1]}}
                            ]
                        }
                    }
                ]
            }
        }
    elif search_type == "OR":
        terms = search_string.split(" ")
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
                        "bool": {
                            "should": [
                                {"match": {search_field: terms[0]}},
                                {"match": {search_field: terms[1]}}
                            ]
                        }
                    }
                ]
            }
        }
    elif search_type == "EXCLUDE":
        terms = search_string.split(" ")
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
                        "bool": {
                            "must": [
                                {"match": {search_field: terms[0]}}
                            ],
                            "must_not": [
                                {"match": {search_field: terms[1]}}
                            ]
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

for i in range(len(websites)):
    q = "\""+websites[i]+"\""
    query_count = get_number_of_occurance(q)
    print(q, ": ", query_count)

websites_comb = list(combinations(websites, 2))

for i in range(len(websites_comb)):
    q = "\""+websites_comb[i][0]+"\" \""+websites_comb[i][1]+"\""
    query_count = get_number_of_occurance(q, "AND")
    print(q + " AND", ": ", query_count)

for i in range(len(websites_comb)):
    q = "\""+websites_comb[i][0]+"\" \""+websites_comb[i][1]+"\""
    query_count = get_number_of_occurance(q, "OR")
    print(q + " OR", ": ", query_count)

for i in range(len(websites_comb)):
    q = "\""+websites_comb[i][0]+"\" \""+websites_comb[i][1]+"\""
    query_count = get_number_of_occurance(q, "EXCLUDE")
    print(q + " EXCLUDE", ": ", query_count)

for i in range(len(websites_comb)):
    q = "\""+websites_comb[i][1]+"\" \""+websites_comb[i][0]+"\""
    query_count = get_number_of_occurance(q, "EXCLUDE")
    print(q + " EXCLUDE", ": ", query_count)
