require './cluster'

host = 'redis'
port1 = '7002'
port2 = '7003'

startup_nodes = [
  {:host => host, :port => port1},
  {:host => host, :port => port2},
]
rc = RedisCluster.new(startup_nodes, 2)
#puts rc.info()
rc.set('asdf', 1)
puts rc.get('asdf')
# c = rc.connections.get_connection_by_node('redis:7002')
# puts c.object_id
# c.info()
# #rc.connections.reset('redis:7002')
# c = rc.connections.get_connection_by_node('redis:7002')
# puts c.object_id
# #puts rc.connections.get_pool_by_node('redis:7002').instance_variable_get('@key')
