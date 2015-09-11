require './cluster'
require 'test/unit'

class TestSimpleNumber < Test::Unit::TestCase

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
    @rc = RedisCluster.new(startup_nodes, 2)
  end

  def teardown
  end

  def test_set
    assert_equal(OK, @rc.set(KEY, 1))
  end

  def test_get
    @rc.set(KEY, "b")
    assert_equal("b", @rc.get(KEY))
  end

  # def test_lpush
  #   assert_equal(OK, @rc.lpush('ff', 'a'))
  # end

end
