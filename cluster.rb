require 'rubygems'
require 'redis'
require './crc16'

class RedisCluster

    RedisClusterHashSlots = 16384
    RedisClusterRequestTTL = 16

    def initialize(startup_nodes,connections,opt={})
        @startup_nodes = startup_nodes
        @max_connections = connections
        @connections = {}
        @opt = opt
        initialize_slots_cache
    end

    # Contact the startup nodes and try to fetch the hash slots -> instances
    # map in order to initialize the @slots hash.
    #
    # TODO: Use new nodes to populate the startup nodes array.
    def initialize_slots_cache
        @startup_nodes.each{|n|
            begin
                @slots = {}
                @nodes = []

                r = Redis.new(:host => n[:host], :port => n[:port])
                r.cluster("nodes").each_line{|l|
                    fields = l.split(" ")
                    addr = fields[1]
                    slots = fields[7..-1].join(",")
                    addr = n[:host]+":"+n[:port].to_s if addr == ":0"
                    addr_ip,addr_port = addr.split(":")
                    addr_port = addr_port.to_i
                    addr = {:host => addr_ip, :port => addr_port, :name => addr}
                    @nodes << addr
                    slots.split(",").each{|range|
                        last = nil
                        first,last = range.split("-")
                        last = first if !last
                        ((first.to_i)..(last.to_i)).each{|slot|
                            @slots[slot] = addr
                        }
                    }
                }
            rescue
                # Try with the next node on error.
                next
            end
            # Exit the loop as long as the first node replies
            break
        }
    end

    # Flush the cache, mostly useful for debugging when we want to force
    # redirection.
    def flush_slots_cache
        @slots = {}
    end

    # Return the hash slot from the key.
    def keyslot(key)
        RedisClusterCRC16.crc16(key) % RedisClusterHashSlots
    end

    # Return the first key in the command, or nil if we don't know how to
    # handle the specified command.
    def get_key_from_command(argv)
        case argv[0].to_s.downcase
        when "set"
            return argv[1]
        when "get"
            return argv[1]
        else
            return nil
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
        @startup_nodes.each{|n|
            begin
                r = Redis.new(:host => n[:host], :port => n[:port])
                return r if r.ping == "PONG"
            rescue => e
                # Just try with the next node.
            end
        }
        raise "Can't reach a single startup node. #{e}"
    end

    # Given a slot return the link (Redis instance) to the mapped node.
    # Make sure to create a connection with the node if we don't have
    # one.
    #
    # TODO: Kill not recently used connections if we reached the max
    # number of connections but need to create a new one.
    def get_connection_by_slot(slot)
        node = @slots[slot]
        # If we don't know what the mapping is, return a random node.
        return get_random_connection if !node
        node[:name] = "#{node[:host]}:#{node[:port]}" if not node[:name]
        if not @connections[node[:name]]
            begin
                @connections[node[:name]] = Redis.new(:host => node[:host],
                                                     :port => node[:port])
            rescue
                # This will probably never happen with recent redis-rb
                # versions because the connection is enstablished in a lazy
                # way only when a command is called. However it is wise to
                # handle an instance creation error of some kind.
                return get_random_connection
            end
        end
        @connections[node[:name]]
    end

    # Dispatch commands.
    def send_cluster_command(argv)
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
                r = get_random_connection
                try_random_node = false
            else
                r = get_connection_by_slot(slot)
            end
            begin
                # TODO: use pipelining to send asking and save a rtt.
                r.asking if asking
                asking = false
                return r.send(argv[0].to_sym,*argv[1..-1])
            rescue Errno::ECONNREFUSED, Redis::TimeoutError
                try_random_node = true
                sleep(0.1) if ttl < RedisClusterRequestTTL/2
            rescue => e
                errv = e.to_s.split
                if errv[0] == "MOVED" || errv[0] == "ASK"
                    puts :moved
                    asking = true if errv[0] == "ASK"
                    newslot = errv[1].to_i
                    node_ip,node_port = errv[2].split(":")
                    @slots[newslot] = {:host => node_ip,
                                       :port => node_port.to_i}
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
    def method_missing(*argv)
        send_cluster_command(argv)
    end
end

startup_nodes = [
    {:host => "127.0.0.1", :port => 6379},
    {:host => "127.0.0.1", :port => 6380}
]
rc = RedisCluster.new(startup_nodes,2)
rc.flush_slots_cache
(0..10000000).each{|x|
    rc.set("foo#{x}",x)
    puts rc.get("foo#{x}")
}
