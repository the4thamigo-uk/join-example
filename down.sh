#!/bin/bash
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

source $script_dir/env.sh

docker stop join-example | true

if [ "$1" == "purge" ]; then
  $dc down -v
else
  $dc down
fi
