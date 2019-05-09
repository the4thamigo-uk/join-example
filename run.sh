#!/bin/bash
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

source $script_dir/env.sh

./down.sh purge
./up.sh

until ./create_topics.sh
do
  echo 'Trying to create topics...'
done

./launch.sh
