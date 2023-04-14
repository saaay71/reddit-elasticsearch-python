# Why is there a differnce between the search query results (number of hits) in Kibana and the search from elasticsearch python?
When we use the Kibana serach bar, the request made performs a "match_phrase" request to elasticsearch.
This would give a different result when using "match" via python elasticsearch query.
You can check the Inspect section in Kibana to find the exact query sent to elasticsearch.
