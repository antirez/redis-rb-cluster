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

require 'logger'
require 'resolv'
require 'redis'
require_relative 'crc16'
require_relative 'lib/connection_table'
require_relative 'lib/exceptions'


class RedisCluster

  RedisClusterHashSlots = 16384
  RedisClusterRequestTTL = 16
  RedisClusterDefaultTimeout = 1

  def initialize(startup_nodes, max_connections, opt={})
    @startup_nodes = startup_nodes
    @connections = ConnectionTable.new(max_connections)
    @opt = opt
    @refresh_table_asap = false
    @log = Logger.new(STDOUT)
    @log.level = Logger::INFO
    initialize_slots_cache
  end

  def inspect
    "#<#{self.class.name}: @connections=#{@connections.inspect}, @startup_nodes=#{@startup_nodes}>"
  end

  def get_redis_link(host,port)
    timeout = @opt[:timeout] or RedisClusterDefaultTimeout
    Redis.new(:host => host, :port => port, :timeout => timeout)
  end

  # Contact the startup nodes and try to fetch the hash slots -> instances
  # map in order to initialize the @slots hash.
  def initialize_slots_cache
    startup_nodes_reachable = false
    dns_cache = {}
    @startup_nodes.each{|n|
      begin
        @nodes = []
        r = get_redis_link(n[:host],n[:port])
        r.cluster("slots").each {|r|
          (r[0]..r[1]).each{|slot|
            ip,port = r[2]
            host = dns_cache.fetch(ip) {
              |missing_ip|
              host = Resolv.getname(missing_ip)
              dns_cache[ip] = host
              host
            }
            name = "#{host}:#{port}"
            node = {
              :host => host, :port => port,
              :name => name, :ip => ip
            }
            @nodes << node
            @connections.update_slot!(slot, name)
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

  def clean_startup_nodes!(n)
    n[:port] = n[:port].to_i
    n[:ip] = Resolv.getaddress(n[:host])
    n[:host] = Resolv.getname(n[:ip])
  end

  def set_node_name!(n)
    n[:name] = "#{n[:host]}:#{n[:port]}"
  end

  def add_missing_nodes
    @nodes.each do |n|
      @startup_nodes << n
    end
  end

  # Use @nodes to populate @startup_nodes, so that we have more chances
  # if a subset of the cluster fails.
  def populate_startup_nodes
    # Make sure every node has already a name, so that later the
    # Array uniq! method will work reliably.
    # Also make sure we have the same host name given an ip address
    @startup_nodes.each do |n|
      clean_startup_nodes! n
      set_node_name! n
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
            @connections.update_slot!(newslot, node_name)
          end
        else
          raise e
        end
      end
    end
    raise "Too many Cluster redirections? (last error: #{e})"
  end

  # Some commands are not implemented yet
  # If someone tries to use them, a NotImplementedError is thrown
  def method_missing(*argv)
    cmd = argv[0].to_s
    raise NotImplementedError, "#{cmd} command is not implemented now!"
  end

  def execute_cmd_on_all_nodes(cmd, log_required = false, *argv)
    ret = {}
    #log_required = LOG_REQUIRED_COMMANDS.member?(cmd)
    @startup_nodes.each do |n|
      node_name = n[:name]
      r = @connections.get_connection_by_node(node_name)
      ret[node_name] = r.public_send(cmd, *argv)
      if log_required
        all = [cmd] + argv
        @log.info("Successfully sent #{all.to_s} to #{node_name}")
      end
    end
    ret
  end

  def _check_keys_in_same_slot(keys)
    prev_slot = nil
    keys.each do |k|
      slot = keyslot(k)
      if prev_slot && prev_slot != slot
        raise Exceptions::CrossSlotsError
      end
      prev_slot = slot
    end
  end

  # server commands
  def info(cmd = nil)
    execute_cmd_on_all_nodes(:info, cmd)
  end

  def flushdb
    execute_cmd_on_all_nodes(:flushdb)
  end

  def config(action, *argv)
    argv = [action] + argv
    log_required = [:resetstat, :set].member?(action)
    execute_cmd_on_all_nodes(:config, log_required=log_required, *argv)
  end

  # string commands
  def append(key, value)
    send_cluster_command([:append, key, value])
  end

  def bitcount(key, start = 0, stop = -1)
    send_cluster_command([:bitcount, key, start, stop])
  end

  def bitop(operation, dest_key, *keys)
    _check_keys_in_same_slot([dest_key] + keys)
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
    _check_keys_in_same_slot(keys)
    send_cluster_command([:blpop, *argv])
  end

  def brpop(*argv)
    keys = argv[0..-2]
    _check_keys_in_same_slot(keys)
    send_cluster_command([:brpop, *argv])
  end

  def brpoplpush(source, destination, options = {})
    _check_keys_in_same_slot([source, destination])
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
    _check_keys_in_same_slot([source, destination])
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
    _check_keys_in_same_slot(keys)
    send_cluster_command([:sdiff, *keys])
  end

  def sdiffstore(destination, *keys)
    _check_keys_in_same_slot([destination, *keys])
    send_cluster_command([:sdiffstore, destination, *keys])
  end

  def sinter(*keys)
    _check_keys_in_same_slot(keys)
    send_cluster_command([:sinter, *keys])
  end

  def sinterstore(destination, *keys)
    _check_keys_in_same_slot([destination, *keys])
    send_cluster_command([:sinterstore, destination, *keys])
  end

  def sismember(key, member)
    send_cluster_command([:sismember, key, member])
  end

  def smembers(key)
    send_cluster_command([:smembers, key])
  end

  def smove(source, destination, member)
    _check_keys_in_same_slot([source, destination])
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
    _check_keys_in_same_slot(keys)
    send_cluster_command([:sunion, *keys])
  end

  def sunionstore(destination, *keys)
    _check_keys_in_same_slot([destination, *keys])
    send_cluster_command([:sunionstore, destination, *keys])
  end

  def sscan(key, cursor, options = {})
    send_cluster_command([:sscan, key, cursor, options])
  end

  # sorted set commands
  def zadd(key, *argv)
    send_cluster_command([:zadd, key, *argv])
  end

  def zcard(key)
    send_cluster_command([:zcard, key])
  end

  def zcount(key, min, max)
    send_cluster_command([:zcount, key, min, max])
  end

  def zincrby(key, increment, member)
    send_cluster_command([:zincrby, key, increment, member])
  end

  def zinterstore(destination, keys, options = {})
    _check_keys_in_same_slot([destination, *keys])
    send_cluster_command([:zinterstore, destination, keys, options])
  end

  #def zlexcount(key, min, max)
    # redis-rb hasn't implement it yet
    #send_cluster_command([:zlexcount, key, min, max])
  #end

  def zrange(key, start, stop, options = {})
    send_cluster_command([:zrange, key, start, stop, options])
  end

  def zrangebylex(key, min, max, options = {})
    send_cluster_command([:zrangebylex, key, min, max, options])
  end

  def zrevrangebylex(key, max, min, options = {})
    send_cluster_command([:zrevrangebylex, key, max, min, options])
  end

  def zrangebyscore(key, min, max, options = {})
    send_cluster_command([:zrangebyscore, key, min, max, options])
  end

  def zrank(key, member)
    send_cluster_command([:zrank, key, member])
  end

  def zrem(key, member)
    send_cluster_command([:zrem, key, member])
  end

  # def zremrangebylex(key, min, max)
  # end

  def zremrangebyrank(key, start, stop)
    send_cluster_command([:zremrangebyrank, key, start, stop])
  end

  def zremrangebyscore(key, min, max)
    send_cluster_command([:zremrangebystore, key, start, stop])
  end

  def zrevrange(key, start, stop, options = {})
    send_cluster_command([:zrevrange, key, start, stop, options])
  end

  def zrevrangebyscore(key, max, min, options = {})
    send_cluster_command([:zrevrangebyscore, key, max, min, options])
  end

  def zrevrank(key, member)
    send_cluster_command([:zrevrank, key, member])
  end

  def zscore(key, member)
    send_cluster_command([:zscore, key, member])
  end

  def zunionstore(destination, keys, options = {})
    _check_keys_in_same_slot([destination, *keys])
    send_cluster_command([:zunionstore, destination, keys, options])
  end

  def zscan(key, cursor, options = {})
    send_cluster_command([:zscan, key, cursor, options])
  end

  # hash commands
  def hdel(key, field)
    send_cluster_command([:hdel, key, field])
  end

  def hexists(key, field)
    send_cluster_command([:hexists, key, field])
  end

  def hget(key, field)
    send_cluster_command([:hget, key, field])
  end

  def hgetall(key)
    send_cluster_command([:hgetall, key])
  end

  def hincrby(key, field, increment)
    send_cluster_command([:hincrby, key, field, increment])
  end

  def hincrbyfloat(key, field, increment)
    send_cluster_command([:hincrbyfloat, key, field, increment])
  end

  def hkeys(key)
    send_cluster_command([:hkeys, key])
  end

  def hlen(key)
    send_cluster_command([:hlen, key])
  end

  def hmget(key, fields)
    send_cluster_command([:hmget, key, fields])
  end

  def hmset(key, *attrs)
    send_cluster_command([:hmset, key, attrs])
  end

  def hset(key, field, value)
    send_cluster_command([:hset, key, field, value])
  end

  def hsetnx(key, field, value)
    send_cluster_command([:hsetnx, key, field, value])
  end

  # def hstrlen(key, field)
  #   send_cluster_command([:hstrlen, key, field])
  # end

  def hvals(key)
    send_cluster_command([:hvals, key])
  end

  def hscan(key, cursor, options = {})
    send_cluster_command([:hscan, key, cursor, options])
  end

  # keys command
  def del(*keys)
    total = 0
    keys.each do |k|
      total += send_cluster_command([:del, k])
    end
    total
  end

  def exists(key)
    send_cluster_command([:exists, key])
  end

  def expire(key, seconds)
    send_cluster_command([:expire, key, seconds])
  end

  def expireat(key, unix_time)
    send_cluster_command([:expireat, key, unix_time])
  end

  def keys(pattern = "*")
    # only for debugging purpose
    ret = execute_cmd_on_all_nodes(:keys, log_required=false, pattern)
    ret.values.flatten
  end

  def persist(key)
    send_cluster_command([:persist, key])
  end

  def ttl(key)
    send_cluster_command([:ttl, key])
  end

  def pexpire(key, milliseconds)
    send_cluster_command([:pexpire, key, milliseconds])
  end

  def pexpireat(key, ms_unix_time)
    send_cluster_command([:pexpireat, key, ms_unix_time])
  end

  def pttl(key)
    send_cluster_command([:pttl, key])
  end

end
