#!/usr/bin/env bash

for i in `seq 7002 7007`;
do
    exec /usr/local/src/redis-3.0.0/src/redis-server /$i/redis.conf
done

echo "yes" | /usr/local/src/redis-3.0.0/src/redis-trib.rb create --replicas 1 127.0.0.1:7002 127.0.0.1:7003 127.0.0.1:7004 127.0.0.1:7005 127.0.0.1:7006 127.0.0.1:7007
#/usr/local/src/redis-3.0.0/src/redis-server /7002/reids.conf
