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

  # Return the hash slot from the key.
  def keyslot(key)
    # Only hash what is inside {...} if there is such a pattern in the key.
    # Note that the specification requires the content that is between
    # the first { and the first } after the first {. If we found {} without
    # nothing In the middle, the whole key is hashed as usually.
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
    when "bitop"
      return argv[2]
    else
      # Unknown commands, and all the commands having the key
      # as first argument are handled here:
      # set, get, ...
      return argv[1]
    end
  end

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

  def _check_keys_on_same_slot(keys)
    prev_slot = nil
    keys.each do |k|
      slot = keyslot(k)
      if prev_slot && prev_slot != keyslot(k)
        raise raise Exceptions::CrossSlotsError
      end
      prev_slot = slot
    end
  end

  def info(cmd = nil)
    execute_cmd_on_all_nodes(:info, cmd)
  end

  def flushdb
    execute_cmd_on_all_nodes(:flushdb)
  end

  # string commands
  def append(key, value)
    send_cluster_command([:append, key, value])
  end

  def bitcount(key, start = 0, stop = -1)
    send_cluster_command([:bitcount, key, start, stop])
  end

  def bitop(operation, dest_key, *keys)
    _check_keys_on_same_slot([dest_key] + keys)
    send_cluster_command([:bitop, operation, dest_key, *keys])
  end

  def bitpos(key, bit, start = 0, stop = -1)
    send_cluster_command([:bitpos, key, bit, start, stop])
  end

  def decr(key)
    send_cluster_command([:decr, key])
  end

  def decrby(key, decrement)
    send_cluster_command([:decrby, key, decrement])
  end

  def get(key)
    send_cluster_command([:get, key])
  end

  def getbit(key, offset)
    send_cluster_command([:getbit, key, offset])
  end

  def getrange(key, start, stop)
    send_cluster_command([:getrange, key, start, stop])
  end

  def incr(key)
    send_cluster_command([:incr, key])
  end

  def incrby(key, increment)
    send_cluster_command([:incrby, key, increment])
  end

  def incrbyfloat(key, increment)
    send_cluster_command([:incrbyfloat, key, increment])
  end

  def psetex(key, millisec, value)
    send_cluster_command([:psetex, key, millisec, value])
  end

  def set(key, value)
    send_cluster_command([:set, key, value])
  end

  def setbit(key, offset, value)
    send_cluster_command([:setbit, key, offset, value])
  end

  def setex(key, seconds, value)
    send_cluster_command([:setex, key, seconds, value])
  end

  def setnx(key, value)
    send_cluster_command([:setnx, key, value])
  end

  def setrange(key, offset, value)
    send_cluster_command([:setrange, key, offset, value])
  end

  def strlen(key)
    send_cluster_command([:strlen, key])
  end

  # list commands
  def blpop(*argv)
    keys = argv[0..-2]
    _check_keys_on_same_slot(keys)
    send_cluster_command([:blpop, *argv])
  end

  def brpop(*argv)
    keys = argv[0..-2]
    _check_keys_on_same_slot(keys)
    send_cluster_command([:brpop, *argv])
  end

  def brpoplpush(source, destination, options = {})
    _check_keys_on_same_slot([source, destination])
    send_cluster_command([:brpoplpush, source, destination, options])
  end

  def lindex(key, index)
    send_cluster_command([:lindex, key, index])
  end

  def linsert(key, where, pivot, value)
    send_cluster_command([:linsert, key, where, pivot, value])
  end

  def llen(key)
    send_cluster_command([:llen, key])
  end

  def lpop(key)
    send_cluster_command([:lpop, key])
  end

  def lpush(key, value)
    send_cluster_command([:lpush, key, value])
  end

  def lpushx(key, value)
    send_cluster_command([:lpushx, key, value])
  end

  def lrange(key, start, stop)
    send_cluster_command([:lrange, key, start, stop])
  end

  def lrem(key, count, value)
    send_cluster_command([:lrem, key, count, value])
  end

  def lset(key, index, value)
    send_cluster_command([:lset, key, index, value])
  end

  def ltrim(key, start, stop)
    send_cluster_command([:ltrim, key, start, stop])
  end

  def rpop(key)
    send_cluster_command([:rpop, key])
  end

  def rpoplpush(source, destination)
    _check_keys_on_same_slot([source, destination])
    send_cluster_command([:rpoplpush, source, destination])
  end

  def rpush(key, value)
    send_cluster_command([:rpush, key, value])
  end

  def rpushx(key, value)
    send_cluster_command([:rpushx, key, value])
  end

  # set commands
  def sadd(key, member)
    send_cluster_command([:sadd, key, member])
  end

  def scard(key)
    send_cluster_command([:scard, key])
  end

  def sdiff(*keys)
    _check_keys_on_same_slot(keys)
    send_cluster_command([:sdiff, *keys])
  end

  def sdiffstore(destination, *keys)
    _check_keys_on_same_slot([destination, *keys])
    send_cluster_command([:sdiffstore, destination, *keys])
  end

  def sinter(*keys)
    _check_keys_on_same_slot(keys)
    send_cluster_command([:sinter, *keys])
  end

  def sinterstore(destination, *keys)
    _check_keys_on_same_slot([destination, *keys])
    send_cluster_command([:sinterstore, destination, *keys])
  end

  def sismember(key, member)
    send_cluster_command([:sismember, key, member])
  end

  def smembers(key)
    send_cluster_command([:smembers, key])
  end

  def smove(source, destination, member)
    _check_keys_on_same_slot([source, destination])
    send_cluster_command([:smove, source, destination, member])
  end

  def spop(key)
    send_cluster_command([:spop, key])
  end

  def srandmember(key, count = nil)
    send_cluster_command([:srandmember, key, count])
  end

  def srem(key, member)
    send_cluster_command([:srem, key, member])
  end

  def sunion(*keys)
    _check_keys_on_same_slot(keys)
    send_cluster_command([:sunion, *keys])
  end

  def sunionstore(destination, *keys)
    _check_keys_on_same_slot([destination, *keys])
    send_cluster_command([:sunionstore, destination, *keys])
  end

  def sscan(key, cursor, options = {})
    send_cluster_command([:sscan, key, cursor, options])
  end

end
