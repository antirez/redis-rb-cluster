# Redis-rb-cluster

Redis Cluster client work in progress.
It wraps Redis-rb, and eventually should be part of it.

For now the goal is to write a simple (but not too simple) client that works
as a reference implementation, and can be used in order to further develop
and test Redis Cluster, that is a work in progress itself.

## Creating a new instance

In order to create a new Redis Cluster instance use:

    startup_nodes = [
        {:host => "127.0.0.1", :port => 6379},
        {:host => "127.0.0.1", :port => 6380}
    ]
    max_cached_connections = 2
    rc = RedisCluster.new(startup_nodes,max_cached_connections)

The startup nodes are a list of addresses of Cluster Nodes, for the client to
work it is important that at least one address works. Startup nodes are used
in order to:

* Initialize the hash slot -> node cache, using the `CLUSTER NODES` command.
* To contact a random node every time we are not able to talk with the right node currently cached for the specified hash slot we are interested in, in the context of the current request.

The list of nodes provided by the user will be extended once the client
will be able to retrieve the cluster configuration.

The second parameter in the object initialization is the maximum number of
connections that the client is allowed to cache. Ideally this should be at
least equal to the number of nodes you have, in order to avoid closing and
reopening TCP sockets. However if you have very large cluster and want to
optimize for clients resource saving, it is possible to use a smaller value.

## Sending commands

Sending commands is very similar to redis-rb:

    rc.get("foo")

Currently only a subset of commands are implemented (and in general multi-keys
commands are not supported by Redis Cluster), because for every supported
command we need a function able to identify the key among the arguments.

## Disclaimer

Redis Cluster is released as stable. 
This client is a work in progress that might not be suitable to be used in production environments.
