require './cluster'

startup_nodes = [
    {:host => "127.0.0.1", :port => 6379},
    {:host => "127.0.0.1", :port => 6380}
]
rc = RedisCluster.new(startup_nodes,32)
# rc.flush_slots_cache
(0..1000000000).each{|x|
    begin
        rc.set("foo#{x}",x)
        puts rc.get("foo#{x}")
    rescue
        puts "error"
    end
}
