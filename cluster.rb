require 'rubygems'
require 'redis'
require './crc16'

class RedisCluster

    RedisClusterHashSlots = 16384

    def initialize(startup_nodes,connections)
        @startup_nodes = startup_nodes
        @max_connections = connections
        @cur_connections = 0
        @connections = {}
        @slots = {}
        initialize_slots
    end

    # Contact the startup nodes and try to fetch the hash slots -> instances
    # map in order to initialize the @slots hash.
    def initialize_slots
        @startup_nodes.each{|n|
            r = Redis.new(:host => n[:host], :port => n[:port])
            r.cluster("nodes").each_line{|l|
                fields = l.split(" ")
                addr = fields[1]
                slots = fields[7..-1].join(",")
                addr = n[:host]+":"+n[:port].to_s if addr == ":0"
                slots.split(",").each{|range|
                    last = nil
                    first,last = range.split("-")
                    last = first if !last
                    ((first.to_i)..(last.to_i)).each{|slot|
                        @slots[slot] = addr
                    }
                }
            }
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

    # Dispatch commands.
    def method_missing(*argv)
        puts argv.join(",")
    end
end

rc = RedisCluster.new([{:host => "127.0.0.1", :port => 6379}],2)
rc.set("foo","bar")
puts rc.get("foo")
