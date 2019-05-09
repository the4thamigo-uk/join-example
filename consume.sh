#!/bin/bash
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

source $script_dir/env.sh

topic="$1"

$dc exec -T schema-registry kafka-avro-console-consumer --from-beginning  --bootstrap-server "$broker_addr" --topic "$topic" --property print.key=true --property schema.registry.url="$schema_registry_url"

