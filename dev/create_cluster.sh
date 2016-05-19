#!/bin/sh

redis=$(hostname -I)
redis="$(echo "${redis}" | tr -d '[[:space:]]')"
echo "yes" | ./redis-trib.rb create --replicas 1 $redis:7000 $redis:7001 $redis:7002 $redis:7003 $redis:7004 $redis:7005
