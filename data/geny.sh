#!/bin/bash

let start=1902580000000

for y in {0..5000..10}; do
  let "t = start + y*1000";
  echo -e '"k1"\t{ "event_time": '$t', "y": '$y' }'
done

