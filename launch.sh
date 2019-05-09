#!/bin/bash
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

source $script_dir/env.sh

docker stop join-example || true

docker run --env-file <(cat << EOF
JOIN_WINDOW_BEFORE_SIZE=PT9S
JOIN_WINDOW_AFTER_SIZE=PT0S
JOIN_WINDOW_GRACE=PT10S
EOF
) --name join-example --rm -ti -p 9100:9100 --network join-example_default join-example:latest

