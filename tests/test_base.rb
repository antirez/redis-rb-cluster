require_relative '../rediscluster'
require 'test/unit'


class TestBase < Test::Unit::TestCase

    OK = "OK"
    KEY = "asdf"

    def setup
        host = 'redis'
        port1 = '7002'
        port2 = '7003'

        startup_nodes = [
            {:host => host, :port => port1},
            {:host => host, :port => port2},
        ]
        conn_opt = {:timeout => 2}
        @rc = RedisCluster.new(startup_nodes, max_connections: 2, read_slave: true, conn_opt: conn_opt)
    end

    def teardown
        @rc.flushdb
    end

    def get_keys_in_same_slot
        ['bbb', 'mngopdrw']
    end

    def get_keys_in_diff_slot
        ['bbb', 'aaa']
    end

end
