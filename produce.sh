#!/bin/bash
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

source $script_dir/env.sh

topic="$1"
schema_dir=$script_dir/schema

$script_dir/produce_avro.sh $topic $schema_dir/key.avsc $schema_dir/$topic.avsc
