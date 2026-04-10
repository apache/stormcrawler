# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/bin/bash

STATUS_SHARDS=$(grep -E '^[^#]*solr.status.routing.shards' solr-conf.yaml | sed -e 's/.*: //' | tr -d ' ')
ROUTER_FIELD=$(grep -E '^[^#]*solr.status.routing.fieldname' solr-conf.yaml | sed -e 's/.*: //' | tr -d ' ')

if [ -z "$STATUS_SHARDS" ]; then
  echo -e "\e[1mProperty 'solr.status.routing.shards not set in solr-conf.yaml'. Defaulting to 1 ...\e[0m\n"
  STATUS_SHARDS=1
fi

if [ -z "$ROUTER_FIELD" ]; then
  echo -e "\e[1mProperty 'solr.status.routing.fieldname' not set in solr-conf.yaml. Defaulting to 'key' ...\e[0m\n"
  ROUTER_FIELD="\"key\""
fi

SOLR_PORT=8983
SOLR_HOME=/opt/solr-10.0.0

$SOLR_HOME/bin/solr start -c -p $SOLR_PORT

echo -e "\n\e[1mUploading configsets ...\e[0m\n"

$SOLR_HOME/bin/solr zk upconfig --conf-name "docs" --conf-dir configsets/docs --zk-host localhost:9983
$SOLR_HOME/bin/solr zk upconfig --conf-name "status" --conf-dir configsets/status --zk-host localhost:9983
$SOLR_HOME/bin/solr zk upconfig --conf-name "metrics" --conf-dir configsets/metrics --zk-host localhost:9983

echo -e "\n\n\e[1mCreating 'docs' collection ...\e[0m\n"
curl -X POST "http://localhost:$SOLR_PORT/api/collections" -H "Content-type:application/json" -d '
  {
    "name": "docs",
    "numShards": 1,
    "replicationFactor": 1,
    "config": "docs"
  }'

echo -e "\n\n\e[1mCreating 'status' collection with $STATUS_SHARDS shard(s) and routing based on '$ROUTER_FIELD' ...\e[0m\n"
curl -X POST "http://localhost:$SOLR_PORT/api/collections" -H "Content-type:application/json" -d '
  {
    "name": "status",
    "numShards": '$STATUS_SHARDS',
    "replicationFactor": 1,
    "router": {
        "name": "compositeId",
        "field": '$ROUTER_FIELD'
    },
    "config": "status"
  }'

echo -e "\n\n\e[1mCreating 'metrics' collection ...\e[0m\n"
curl -X POST "http://localhost:$SOLR_PORT/api/collections" -H "Content-type:application/json" -d '
  {
    "name": "metrics",
    "numShards": 1,
    "replicationFactor": 1,
    "config": "metrics"
  }'
