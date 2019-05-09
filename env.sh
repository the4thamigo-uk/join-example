#!/bin/bash
set -e
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

zookeeper_addr='zookeeper:2181'
ksqlserver_url='http://ksql-server:8088'
schema_registry_url='http://schema-registry:8081'
broker_addr='broker:9092'
dc="docker-compose -f $script_dir/docker-compose.yml"
