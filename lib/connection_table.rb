require 'connection_pool'
require 'redis'

class ConnectionTable


  def split_node(node)
    host, port = node.split(':')
    [host, port]
  end

  def initialize(max_connections, read_slave: false, timeout: nil)
    @max_connections = max_connections
    @read_slave = read_slave
    @timeout = timeout
    @master_conns = {}
    @slave_conns = {}
    @slots = {}
    @pid = get_pid
  end

  def inspect
   "#<#{self.class.name}: @master_conns=#{@master_conns.keys}, @slave_conns=#{@slave_conns.keys}, @max_connections=#{@max_connections}, @timeout=#{@timeout}>"
  end

  def new_pool(node, read_only: false)
    host, port = split_node(node)
    ConnectionPool.new(size: @max_connections) {
      r = Redis.new(:host => host, :port => port, :timeout => @timeout)
      if read_only
        r.readonly
      end
      r
    }
  end

  def get_pool_by_node(node)
    @master_conns.fetch(node, nil) or @slave_conns.fetch(node)
  end

  def get_connection_by_node(node)
    pool = get_pool_by_node(node)
    pool.with do |conn|
      return conn
    end
  end

  def get_random_connection(master_only)
    keys = master_only ? @master_conns.keys : @master_conns.keys + @slave_conns.keys
    random_node = keys.sample
    get_connection_by_node(random_node)
  end

  def get_connection_by_slot(slot, master_only)
    nodes = @slots[slot]
    node = @read_slave && !master_only ? nodes.sample: nodes[0]
    get_connection_by_node(node)
  end

  def init_node_pool(nodes)
    nodes.each do |n|
      name = n[:name]
      if @master_conns.has_key?(name) || @slave_conns.has_key?(name)
        next
      end
      if n[:role] == 'master'
        reset_master_node!(name)
        next
      end
      reset_slave_node!(name)
    end
  end

  def update_slot!(newslot, node_names)
    @slots[newslot] = node_names
  end

  def reset_master_node!(node)
    @master_conns[node] = new_pool(node)
  end

  def reset_slave_node!(node)
    @slave_conns[node] = new_pool(node, read_only: true)
  end

  def get_pid
    Process.pid
  end

  def make_fork_safe(nodes)
    if @pid != get_pid
      @master_conns = {}
      @slave_conns = {}
      init_node_pool(nodes)
    end
  end

end
