require_relative '../lib/exceptions'
require_relative 'test_base'


class TestSortedSetCmds < TestBase

  def test_zadd
    assert_equal(true, @rc.zadd(KEY, 1, 'one'))
  end

  def test_zcard
    @rc.zadd(KEY, 1, 'a')
    assert_equal(1, @rc.zcard(KEY))
  end

  def test_zcount
    @rc.zadd(KEY, [[1, 'a'], [2, 'b'], [3, 'c']])
    assert_equal(3, @rc.zcount(KEY, 1, 3))
  end

  def test_zincrby
    @rc.zadd(KEY, [[1, 'a'], [2, 'b'], [3, 'c']])
    assert_equal(4, @rc.zincrby(KEY, 3, 'a'))
  end

  def test_zinterstore_with_raise
    key1, key2 = get_keys_in_diff_slot
    assert_raise(Exceptions::CrossSlotsError) { @rc.zinterstore(key1, [key1, key2]) }
  end

  def test_zinterstore
    key1, key2 = get_keys_in_same_slot
    @rc.zadd(key1, 1, 'a')
    @rc.zadd(key2, 2, 'a')
    assert_equal(1, @rc.zinterstore(key1, [key1, key2]))
  end

  # def test_zlexcount
  #   @rc.zadd(KEY, [[0, 'a'], [0, 'b'])
  #   assert_equal(2, @rc.zlexcount(KEY, 'a', 'b'))
  # end

  def test_zrange
    @rc.zadd(KEY, [[1, 'a'], [2, 'b'], [3, 'c']])
    assert_equal(['a', 'b'], @rc.zrange(KEY, 0, 1))
    options = {:withscores => true}
    assert_equal([['a', 1], ['b', 2]], @rc.zrange(KEY, 0, 1, options))
  end

  def test_zrangebylex
    @rc.zadd(KEY, [[0, 'a'], [0, 'b'], [0, 'c']])
    assert_equal(['a', 'b', 'c'], @rc.zrangebylex(KEY, '[a', '[c'))
  end

  def test_zrevrangebylex
    @rc.zadd(KEY, [[0, 'a'], [0, 'b'], [0, 'c']])
    assert_equal(['c', 'b', 'a'], @rc.zrevrangebylex(KEY, '[c', '[a'))
  end

  def test_zrangebyscore
    @rc.zadd(KEY, [[2, 'a'], [1, 'b'], [3, 'c']])
    assert_equal(['b', 'a', 'c'], @rc.zrangebyscore(KEY, 1, 3))
  end

  def test_zrank
    @rc.zadd(KEY, [[2, 'a'], [1, 'b'], [3, 'c']])
    assert_equal(1, @rc.zrank(KEY, 'a'))
  end

  def test_zrem
    @rc.zadd(KEY, [[2, 'a'], [1, 'b'], [3, 'c']])
    assert_equal(true, @rc.zrem(KEY, 'a'))
  end

  # def test_zremrangebylex
  # end

  def test_zremrangebyrank
    @rc.zadd(KEY, [[2, 'a'], [1, 'b'], [3, 'c']])
    assert_equal(1, @rc.zremrangebyrank(KEY, 0, 0))
  end

  def test_zrevrange
    @rc.zadd(KEY, [[2, 'a'], [1, 'b'], [3, 'c']])
    assert_equal(['c', 'a', 'b'], @rc.zrevrange(KEY, 0, 2))
  end

  def test_zrevrangebyscore
    @rc.zadd(KEY, [[2, 'a'], [1, 'b'], [3, 'c']])
    assert_equal(['c', 'a', 'b'], @rc.zrevrangebyscore(KEY, 3, 1))
  end

  def test_zrevrank
    @rc.zadd(KEY, [[2, 'a'], [1, 'b'], [3, 'c']])
    assert_equal(0, @rc.zrevrank(KEY, 'c'))
  end

  def test_zscore
    @rc.zadd(KEY, 1, 'a')
    assert_equal(1, @rc.zscore(KEY, 'a'))
  end

  def test_zunionstore_with_raise
    key1, key2 = get_keys_in_diff_slot
    assert_raise(Exceptions::CrossSlotsError) { @rc.zunionstore(key1, [key1, key2]) }
  end

  def test_zunionstore
    key1, key2 = get_keys_in_same_slot
    @rc.zadd(key1, [[1, 'a'], [2, 'b']])
    @rc.zadd(key2, [[1, 'a'], [2, 'b'], [3, 'c']])
    ret = @rc.zunionstore(key1, [key1, key2], :weights => [2, 3], :aggregate => "sum")
    assert_equal(3, ret)
  end

  def test_zscan
    @rc.zadd(KEY, [[2, 'a'], [1, 'b']])
    assert_equal(["0", [["b", 1.0], ["a", 2.0]]], @rc.zscan(KEY, 0))
  end

end
