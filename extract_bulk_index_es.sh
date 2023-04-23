#!/bin/bash

input_file=$1
es_index=$(basename "$input_file" | cut -f 1 -d '.' | tr '[:upper:]' '[:lower:]')
max_count=15000

# start extracting the zst file and pipe the output to jq
zstdcat --long=31 "$input_file" | jq -c '{score_hidden, name, link_id, body, downs, created_utc, score, author, distinguished, id, archived, parent_id, subreddit, retrieved_on, ups, controversiality, subreddit_id, "@timestamp": (.created_utc | strftime("%Y-%m-%dT%H:%M:%SZ"))}' | {
  i=0
  bulk=""
  while read line; do
    (( i++ ))
    bulk+="{\"index\":{\"_index\":\"$es_index\"}}"
    bulk+=$'\n'
    bulk+="$line"
    bulk+=$'\n'
    if (( i % max_count == 0 )); then
#      echo "$bulk"
      echo "$bulk" | curl -s -H "Content-Type: application/x-ndjson" -XPOST "localhost:9200/_bulk" --data-binary @-
      bulk=""
#      break
    fi
  done
  if [[ $bulk != "" ]]; then
    echo "$bulk" | curl -s -H "Content-Type: application/x-ndjson" -XPOST "localhost:9200/_bulk" --data-binary @-
  fi
} >/dev/null 2>&1
