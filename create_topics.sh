#!/bin/bash
set -e
cat ./data/y.dat | ./produce.sh y
cat ./data/x.dat | ./produce.sh x
