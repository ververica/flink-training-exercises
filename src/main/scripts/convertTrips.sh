#!/bin/bash

# usage: just pipe raw data into script

tr -d '\r' | awk -F "," '{print NR "," $6 ",START," $11 "," $12 "," $13 "," $14 "," $8 ",-1"}{print NR "," $7 ",END," $11 "," $12 "," $13 "," $14 "," $8 "," $10}' | sort -t ',' -k 2 -S "4G" < /dev/stdin
