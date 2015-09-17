require 'connection_pool'
require 'redis'

class ConnectionTable


  def split_node(node)
    host, port = node.split(':')
    [host, port]
  end

  def initialize(max_connections)
    @max_connections = max_connections
    @table = {}
    @slots = {}
  end

  def inspect
   "#<#{self.class.name}: @table=#{@table}, @max_connections=#{@max_connections}>"
  end

  def new_pool(node)
    host, port = split_node(node)
    ConnectionPool.new(size: @max_connections) { Redis.new(:host => host, :port => port)}
  end

  def get_pool_by_node(node)
    if !@table.has_key?(node)
      pool = new_pool(node)
      @table[node] = pool
      return pool
    end
    @table[node]
  end

  def get_connection_by_node(node)
    pool = get_pool_by_node(node)
    pool.with do |conn|
      return conn
    end
  end

  def get_random_connection
    random_node = @table.keys.sample
    get_connection_by_node(random_node)
  end

  def get_connection_by_slot(slot)
    node = @slots[slot]
    get_connection_by_node(node)
  end

  def flush_slots_cache
    @slots = {}
  end

  def update_slot!(newslot, node_name)
    @slots[newslot] = node_name
  end

  def reset(node)
    @table[node] = new_pool(node)
  end

  def check_pid
  end

end
