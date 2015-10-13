require_relative 'test_base'

class TestHashCmds < TestBase

  FIELD1 = 'hello'
  FIELD2 = 'world'

  def test_hdel
    @rc.hset(KEY, FIELD1, 'a')
    assert_equal(1, @rc.hdel(KEY, FIELD1))
  end

  def test_hexists
    @rc.hset(KEY, FIELD1, 'a')
    assert_equal(false, @rc.hexists(KEY, FIELD2))
    assert_equal(true, @rc.hexists(KEY, FIELD1))
  end

  def test_hget
    @rc.hset(KEY, FIELD1, 'a')
    assert_equal('a', @rc.hget(KEY, FIELD1))
  end

  def test_hgetall
    @rc.hset(KEY, FIELD1, 'a')
    @rc.hset(KEY, FIELD2, 'b')
    t = {
      FIELD1 => 'a',
      FIELD2 => 'b'
    }
    assert_equal(t, @rc.hgetall(KEY))
  end

  def test_hincrby
    @rc.hset(KEY, FIELD1, 1)
    assert_equal(2, @rc.hincrby(KEY, FIELD1, 1))
  end

  def test_hincrbyfloat
    @rc.hset(KEY, FIELD1, 1)
    assert_equal(1.1, @rc.hincrbyfloat(KEY, FIELD1, 0.1))
  end

  def test_hkeys
    @rc.hset(KEY, FIELD1, 'a')
    @rc.hset(KEY, FIELD2, 'b')
    assert_equal([FIELD1, FIELD2], @rc.hkeys(KEY).sort!)
  end

  def test_hlen
    @rc.hset(KEY, FIELD1, 'a')
    @rc.hset(KEY, FIELD2, 'b')
    assert_equal(2, @rc.hlen(KEY))
  end

  def test_hmget
    @rc.hset(KEY, FIELD1, 'a')
    @rc.hset(KEY, FIELD2, 'b')
    assert_equal(['a', 'b'], @rc.hmget(KEY, *[FIELD1, FIELD2]).sort!)
  end

  def test_mapped_get
    @rc.hset(KEY, FIELD1, 'a')
    @rc.hset(KEY, FIELD2, 'b')
    ret = {FIELD1 => 'a', FIELD2 => 'b'}
    assert_equal(ret, @rc.mapped_hmget(KEY, *[FIELD1, FIELD2]))
  end

  def test_hmset
    attrs = [FIELD1, 'a', FIELD2, 'b']
    assert_equal(OK, @rc.hmset(KEY, *attrs))
    assert_equal(['a', 'b'], @rc.hmget(KEY, *[FIELD1, FIELD2]).sort!)
  end

  def test_mapped_hmset
    hash = {FIELD1 => 'a', FIELD2 => 'b'}
    assert_equal(OK, @rc.mapped_hmset(KEY, hash))
    assert_equal(['a', 'b'], @rc.hmget(KEY, *[FIELD1, FIELD2]).sort!)
  end

  def test_hsetnx
    @rc.hset(KEY, FIELD1, 'a')
    assert_equal(false, @rc.hsetnx(KEY, FIELD1, 'a'))
  end

  # def test_hstrlen
  #   @rc.hset(KEY, FIELD1, 'a')
  #   assert_equal(1, @rc.hstrlen(KEY, FIELD1))
  # end

  def test_hvals
    @rc.hset(KEY, FIELD1, 'a')
    @rc.hset(KEY, FIELD2, 'b')
    assert_equal(['a', 'b'], @rc.hvals(KEY).sort!)
  end

  def test_hscan
    @rc.hset(KEY, FIELD1, 'a')
    @rc.hset(KEY, FIELD2, 'b')
    assert_equal(["0", [[FIELD1, "a"], [FIELD2, "b"]]], @rc.hscan(KEY, 0))
  end
end
