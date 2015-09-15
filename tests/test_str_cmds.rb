require './cluster'
require './lib/exceptions'
require 'test/unit'


class TestStrCmds < Test::Unit::TestCase

  KEY = 'asdf'
  OK = 'OK'

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
    @rc.flushdb
  end

  def test_append
    @rc.set(KEY, 'a')
    assert_equal(2, @rc.append(KEY, 'b'))
  end

  def test_bitcount
    @rc.set(KEY, 'a')
    assert_equal(3, @rc.bitcount(KEY))
  end

  def test_bitop_with_raise
    @rc.set('key1', 'foobar')
    @rc.set('key2', 'abcdef')
    assert_raise( Exceptions::CrossSlotsError) { @rc.bitop('and', 'key1', 'key2')}
  end

  def test_bitpos
    @rc.set(KEY, "\xff\xf0\x00")
    assert_equal(12, @rc.bitpos(KEY, 0))
    @rc.set(KEY, "\x00\xff\xf0")
    assert_equal(8, @rc.bitpos(KEY, 1, 0))
  end

  def test_decr
    @rc.set(KEY, 1)
    assert_equal(0, @rc.decr(KEY))
  end

  def test_decrby
    @rc.set(KEY, 3)
    assert_equal(1, @rc.decrby(KEY, 2))
  end

  def test_get
    @rc.set(KEY, 'a')
    assert_equal('a', @rc.get(KEY))
  end

  def test_getbit
    @rc.setbit(KEY, 7, 1)
    assert_equal(1, @rc.getbit(KEY, 7))
  end

  def test_getrange
    @rc.set(KEY, 'abcd')
    assert_equal('bc', @rc.getrange(KEY, 1, 2))
  end

  def test_incr
    @rc.set(KEY, 0)
    assert_equal(1, @rc.incr(KEY))
  end

  def test_incrby
    @rc.set(KEY, 0)
    assert_equal(2, @rc.incrby(KEY, 2))
  end

  def test_incrbyfloat
    @rc.set(KEY, 0.1)
    assert_equal(0.3, @rc.incrbyfloat(KEY, 0.2))
  end

  def test_psetex(*argv)
    assert_equal(OK, @rc.setex(KEY, 100, 10))
  end

  def test_set(*argv)
    assert_equal(OK, @rc.set(KEY, 'a'))
    assert_equal('a', @rc.get(KEY))
  end

  def test_setbit
    assert_equal(0, @rc.setbit(KEY, 7, 1))
  end

  def test_setex
    assert_equal(OK, @rc.setex(KEY, 10, 10))
  end

  def test_setnx
    assert_equal(true, @rc.setnx(KEY, 'hello'))
    assert_equal(false, @rc.setnx(KEY, 'world'))
    assert_equal('hello', @rc.get(KEY))
  end

  def test_setrange
    @rc.set(KEY, 'a')
    assert_equal(3, @rc.setrange(KEY, 1, 'bc'))
  end

  def test_strlen
    @rc.set(KEY, 'ab')
    assert_equal(2, @rc.strlen(KEY))
  end

end
