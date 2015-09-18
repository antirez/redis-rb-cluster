require_relative '../lib/exceptions'
require_relative 'test_base'
require 'test/unit'


class TestListCmds < TestBase

  def _push_items_to_list
    @rc.lpush(KEY, 'b')
    @rc.lpush(KEY, 'a')
  end

  def test_blpop
    _push_items_to_list
    assert_equal(['asdf', 'a'], @rc.blpop(KEY))
  end

  def test_brpop
    _push_items_to_list
    assert_equal(['asdf', 'b'], @rc.brpop(KEY))
  end

  def test_brpoplpush_with_raise
    key1, key2 = get_keys_in_diff_slot
    assert_raise(Exceptions::CrossSlotsError) { @rc.brpoplpush(key1, key2, 1) }
  end

  def test_brpoplpush
    key1, key2 = get_keys_in_same_slot
    @rc.lpush(key1, 'a')
    @rc.lpush(key2, 'b')
    assert_equal('a', @rc.brpoplpush(key1, key2, 1))
  end

  def test_lindex
    @rc.lpush(KEY, 'a')
    assert_equal('a', @rc.lindex(KEY, 0))
  end

  def test_linsert
    _push_items_to_list
    assert_equal(3, @rc.linsert(KEY, 'before', 'b', 'c'))
    assert_equal(['a', 'c', 'b'], @rc.lrange(KEY, 0, -1))
  end

  def test_llen
    @rc.lpush(KEY, 'a')
    assert_equal(1, @rc.llen(KEY))
  end

  def test_lpop
    _push_items_to_list
    assert_equal('a', @rc.lpop(KEY))
  end

  def test_lpush
    assert_equal(1, @rc.lpush(KEY, 'a'))
  end

  def test_lpushx
    assert_equal(0, @rc.lpushx(KEY, 'a'))
    _push_items_to_list
    assert_equal(3, @rc.lpushx(KEY, 'a'))
  end

  def test_lrange
    _push_items_to_list
    assert_equal(['a', 'b'], @rc.lrange(KEY, 0, 2))
  end

  def test_lrem
    _push_items_to_list
    @rc.lpush(KEY, 'a')
    assert_equal(2, @rc.lrem(KEY, 2, 'a'))
  end

  def test_lset
    _push_items_to_list
    assert_equal(OK, @rc.lset(KEY, 0, 'a'))
  end

  def test_ltrim
    _push_items_to_list
    assert_equal(OK, @rc.ltrim(KEY, 1, -1))
    assert_equal(['b'], @rc.lrange(KEY, 0, -1))
  end

  def test_rpop
    _push_items_to_list
    assert_equal('b', @rc.rpop(KEY))
  end

  def test_rpoplpush_with_raise
    key1, key2 = get_keys_in_diff_slot
    assert_raise(Exceptions::CrossSlotsError) { @rc.rpoplpush(key1, key2) }
  end

  def test_rpoplpush
    key1, key2 = get_keys_in_same_slot
    @rc.lpush(key1, 'a')
    @rc.lpush(key2, 'b')
    assert_equal('a', @rc.rpoplpush(key1, key2))
  end

  def test_rpush
    assert_equal(1, @rc.rpush(KEY, 'a'))
    assert_equal(3, @rc.rpush(KEY, ['b', 'c']))
  end

  def test_rpushx
    assert_equal(0, @rc.rpushx(KEY, 'a'))
    _push_items_to_list
    assert_equal(3, @rc.rpushx(KEY, 'a'))
  end

end
