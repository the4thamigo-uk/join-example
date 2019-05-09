#!/bin/bash

let start=1902580000000

for x in {0..1000}; do
  let "t = start + x*1000";
  echo -e '"k1"\t{ "event_time": '$t', "x": '$x' }'
done

