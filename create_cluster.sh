#!/bin/bash

gem install redis connection_pool
redis=`getent hosts redis | cut -d' ' -f1`
echo $redis
echo "yes" | /redis-trib.rb create --replicas 1 $redis:7002 $redis:7003 $redis:7004 $redis:7005 $redis:7006 $redis:7007
#/redis-trib.rb add-node $redis:7008 $redis:7002
