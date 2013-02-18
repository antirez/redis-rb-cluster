require 'rubygems'
require 'redis'
require './crc16'

class RedisCluster

    RedisClusterHashSlots = 16384

    def initialize(startup_nodes,connections)
        @startup_nodes = startup_nodes
        @max_connections = connections
        @connections = {}
        initialize_slots
    end

    # Contact the startup nodes and try to fetch the hash slots -> instances
    # map in order to initialize the @slots hash.
    #
    # TODO:
    #
    # 1) Trap errors inside the loop in order to try with the next
    #    starup node if the previous failed.
    # 2) Use new nodes to populate the startup nodes array.
    def initialize_slots
        @startup_nodes.each{|n|
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
            # Exit the loop as long as the first node replies
            break
        }
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

    # Given a slot return the link (Redis instance) to the mapped node.
    # Make sure to create a connection with the node if we don't have
    # one.
    #
    # TODO: Return a link to a random node if we can't connect to the
    # specified instance, so that if the node is failing we'll be
    # redirected.
    #
    # TODO: Kill not recently used connections if we reached the max
    # number of connections but need to create a new one.
    def get_connection_by_slot(slot)
        node = @slots[slot]
        if not @connections[node[:name]]
            @connections[node[:name]] = Redis.new(:host => node[:host],
                                                 :port => node[:port])
        end
        @connections[node[:name]]
    end

    # Dispatch commands.
    def method_missing(*argv)
        key = get_key_from_command(argv)
        raise "No way to dispatch this command to Redis Cluster." if !key
        slot = keyslot(key)
        r = get_connection_by_slot(slot)
        r.send(argv[0].to_sym,*argv[1..-1])
    end
end

rc = RedisCluster.new([{:host => "127.0.0.1", :port => 6379}],2)
rc.set("foo","bar")
puts rc.get("foo")
