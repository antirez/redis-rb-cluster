# Copyright (C) 2013 Salvatore Sanfilippo <antirez@gmail.com>
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

require 'resolv'
require 'set'
require 'rubygems'
require 'redis'
require './crc16'
require './lib/connection_table'
require './lib/exceptions'

class RedisCluster

  RedisClusterHashSlots = 16384
  RedisClusterRequestTTL = 16
  RedisClusterDefaultTimeout = 1
  CmdsOnAllNodes = Set.new ["info"]

  def initialize(startup_nodes,connections,opt={})
    @startup_nodes = startup_nodes
    @max_connections = connections
    @connections = ConnectionTable.new(@max_connections)
    @opt = opt
    @refresh_table_asap = false
    initialize_slots_cache
  end

  def get_redis_link(host,port)
    timeout = @opt[:timeout] or RedisClusterDefaultTimeout
    Redis.new(:host => host, :port => port, :timeout => timeout)
  end

  # Given a node (that is just a Ruby hash) give it a name just
  # concatenating the host and port. We use the node name as a key
  # to cache connections to that node.
  def set_node_name!(n)
    if !n[:name]
      ip = Resolv.getaddress(n[:host])
      n[:name] = "#{ip}:#{n[:port]}"
    end
  end

  # Contact the startup nodes and try to fetch the hash slots -> instances
  # map in order to initialize the @slots hash.
  def initialize_slots_cache
    startup_nodes_reachable = false
    @startup_nodes.each{|n|
      begin
        @nodes = []

        r = get_redis_link(n[:host],n[:port])
        r.cluster("slots").each {|r|
          (r[0]..r[1]).each{|slot|
            ip,port = r[2]
            name = "#{ip}:#{port}"
            node = {
              :host => ip, :port => port,
              :name => name
            }
            @nodes << node
            @connections.slots[slot] = name
          }
        }
        populate_startup_nodes
        @refresh_table_asap = false
      rescue
        # Try with the next node on error.
        next
      end
      # Exit the loop as long as the first node replies
      startup_nodes_reachable = true
      break
    }
    if !startup_nodes_reachable
      raise Exceptions::StartupNodesUnreachable
    end
  end

  def add_missing_nodes
    @nodes.each do |n|
      n[:ip] = n[:host]
      n[:host] = Resolv.getname n[:ip]
      @startup_nodes << n
    end
  end

  # Use @nodes to populate @startup_nodes, so that we have more chances
  # if a subset of the cluster fails.
  def populate_startup_nodes
    # Make sure every node has already a name, so that later the
    # Array uniq! method will work reliably.
    @startup_nodes.each do |n|
      set_node_name! n
      n[:ip] = Resolv.getaddress n[:host]
      n[:port] = n[:port].to_i
    end
    add_missing_nodes
    @startup_nodes.uniq!
  end

  # Flush the cache, mostly useful for debugging when we want to force
  # redirection.
  def flush_slots_cache
    @slots = {}
  end

  # Return the hash slot from the key.
  def keyslot(key)
    # Only hash what is inside {...} if there is such a pattern in the key.
    # Note that the specification requires the content that is between
    # the first { and the first } after the first {. If we found {} without
    # nothing in the middle, the whole key is hashed as usually.
    s = key.index "{"
    if s
      e = key.index "}",s+1
      if e && e != s+1
        key = key[s+1..e-1]
      end
    end
    RedisClusterCRC16.crc16(key) % RedisClusterHashSlots
  end

  # Return the first key in the command arguments.
  #
  # Currently we just return argv[1], that is, the first argument
  # after the command name.
  #
  # This is indeed the key for most commands, and when it is not true
  # the cluster redirection will point us to the right node anyway.
  #
  # For commands we want to explicitly bad as they don't make sense
  # in the context of cluster, nil is returned.
  def get_key_from_command(argv)
    case argv[0].to_s.downcase
    when "info","multi","exec","slaveof","config","shutdown"
      return nil
    else
      # Unknown commands, and all the commands having the key
      # as first argument are handled here:
      # set, get, ...
      return argv[1]
    end
  end

  # If the current number of connections is already the maximum number
  # allowed, close a random connection. This should be called every time
  # we cache a new connection in the @connections hash.
  def close_existing_connection
    while @connections.length >= @max_connections
      @connections.each{|n,r|
        @connections.delete(n)
        begin
          r.client.disconnect
        rescue
        end
        break
      }
    end
  end

  # Return a link to a random node, or raise an error if no node can be
  # contacted. This function is only called when we can't reach the node
  # associated with a given hash slot, or when we don't know the right
  # mapping.
  #
  # The function will try to get a successful reply to the PING command,
  # otherwise the next node is tried.
  def get_random_connection
    e = ""
    @startup_nodes.shuffle.each{|n|
      begin
        set_node_name!(n)
        conn = @connections[n[:name]]

        if !conn
          # Connect the node if it is not connected
          conn = get_redis_link(n[:host],n[:port])
          if conn.ping == "PONG"
            close_existing_connection
            @connections[n[:name]] = conn
            return conn
          else
            # If the connection is not good close it ASAP in order
            # to avoid waiting for the GC finalizer. File
            # descriptors are a rare resource.
            conn.client.disconnect
          end
        else
          # The node was already connected, test the connection.
          return conn if conn.ping == "PONG"
        end
      rescue => e
        # Just try with the next node.
      end
    }
    raise "Can't reach a single startup node. #{e}"
  end

  # Given a slot return the link (Redis instance) to the mapped node.
  # Make sure to create a connection with the node if we don't have
  # one.
  # def get_connection_by_slot(slot)
  #   node = @slots[slot]
  #   # If we don't know what the mapping is, return a random node.
  #   return get_random_connection if !node
  #   set_node_name!(node)
  #   if not @connections[node[:name]]
  #     begin
  #       close_existing_connection
  #       @connections[node[:name]] =
  #         get_redis_link(node[:host],node[:port])
  #     rescue
  #       # This will probably never happen with recent redis-rb
  #       # versions because the connection is enstablished in a lazy
  #       # way only when a command is called. However it is wise to
  #       # handle an instance creation error of some kind.
  #       return get_random_connection
  #     end
  #   end
  #   @connections[node[:name]]
  # end

  # Dispatch commands.
  def send_cluster_command(argv)
    initialize_slots_cache if @refresh_table_asap
    ttl = RedisClusterRequestTTL; # Max number of redirections
    e = ""
    asking = false
    try_random_node = false
    while ttl > 0
      ttl -= 1
      key = get_key_from_command(argv)
      raise "No way to dispatch this command to Redis Cluster." if !key
      slot = keyslot(key)
      if try_random_node
        r = @connections.get_random_connection
        try_random_node = false
      else
        r = @connections.get_connection_by_slot(slot)
      end
      begin
        # TODO: use pipelining to send asking and save a rtt.
        r.asking if asking
        asking = false
        return r.send(argv[0].to_sym,*argv[1..-1])
      rescue Errno::ECONNREFUSED, Redis::TimeoutError, Redis::CannotConnectError, Errno::EACCES
        try_random_node = true
        sleep(0.1) if ttl < RedisClusterRequestTTL/2
      rescue => e
        errv = e.to_s.split
        if errv[0] == "MOVED" || errv[0] == "ASK"
          if errv[0] == "ASK"
            asking = true
          else
            # Serve replied with MOVED. It's better for us to
            # ask for CLUSTER NODES the next time.
            @refresh_table_asap = true
          end
          newslot = errv[1].to_i
          node_name = errv[2]
          if !asking
            @connections.slots[newslot] = node_name
          end
        else
          raise e
        end
      end
    end
    raise "Too many Cluster redirections? (last error: #{e})"
  end

  # Currently we handle all the commands using method_missing for
  # simplicity. For a Cluster client actually it will be better to have
  # every single command as a method with the right arity and possibly
  # additional checks (example: RPOPLPUSH with same src/dst key, SORT
  # without GET or BY, and so forth).
  # def method_missing(*argv)
  #   send_cluster_command(argv)
  # end

  def execute_cmd_on_all_nodes(cmd, *argv)
    ret = {}
    @startup_nodes.each do |n|
      node_name = n[:name]
      r = @connections.get_connection_by_node(node_name)
      ret[node_name] = r.public_send(cmd, *argv)
    end
    ret
  end

  def send_command(cmd, *argv)
    if CmdsOnAllNodes.member? cmd
      return execute_cmd_on_all_nodes(cmd, *argv)
    end
    send_cluster_command([cmd] + argv)
  end

  def info(*argv)
    send_command(:info, *argv)
  end

  def get(*argv)
    send_command(:get, *argv)
  end

  # list commands
  def set(*argv)
    send_command(:set,  *argv)
  end

  def blpop(*argv)
    send_command(:blpop, *argv)
  end

  def brpop(*argv)
    send_command(:brpop, *argv)
  end

  def lpush(*argv)
    send_command(:lpush, *argv)
  end

  def rpush(*argv)
    send_command(:rpush, *argv)
  end

 end
