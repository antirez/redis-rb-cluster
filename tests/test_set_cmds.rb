require_relative 'test_base'

class TestSetCmds < TestBase

  def test_sadd
    assert_equal(true, @rc.sadd(KEY, 'a'))
  end

  def test_sadd_with_array
    value = ['a', 'b']
    assert_equal(2, @rc.sadd(KEY, value))
  end

  def test_scard
    @rc.sadd(KEY, ['a', 'b'])
    assert_equal(2, @rc.scard(KEY))
  end

  def test_sdiff_with_raise
    key1, key2 = get_keys_in_diff_slot
    assert_raise(Exceptions::CrossSlotsError) { @rc.sdiff(key1, key2) }
  end

  def test_sdiff
    key1, key2 = get_keys_in_same_slot
    @rc.sadd(key1, ['a', 'b'])
    @rc.sadd(key2, 'b')
    assert_equal(['a'], @rc.sdiff(key1, key2))
  end

  def test_sdiffstore_with_raise
    key1, key2 = get_keys_in_diff_slot
    assert_raise(Exceptions::CrossSlotsError) { @rc.sdiffstore(key1, key1, key2) }
  end

  def test_sdiffstore
    key1, key2 = get_keys_in_same_slot
    @rc.sadd(key1, ['a', 'b'])
    @rc.sadd(key2, 'b')
    assert_equal(1, @rc.sdiffstore(key1, key1, key2))
  end

  def test_sinter_with_raise
    key1, key2 = get_keys_in_diff_slot
    assert_raise(Exceptions::CrossSlotsError) { @rc.sinter(key1, key2) }
  end

  def test_sinter
    key1, key2 = get_keys_in_same_slot
    @rc.sadd(key1, ['a', 'b'])
    @rc.sadd(key2, 'b')
    assert_equal(['b'], @rc.sinter(key1, key2))
  end

  def test_sinterstore_with_raise
    key1, key2 = get_keys_in_diff_slot
    assert_raise(Exceptions::CrossSlotsError) { @rc.sinterstore(key1, key1, key2) }
  end

  def test_sinterstore
    key1, key2 = get_keys_in_same_slot
    @rc.sadd(key1, ['a', 'b'])
    @rc.sadd(key2, 'b')
    assert_equal(1, @rc.sinterstore(key1, key1, key2))
  end

  def test_sismember
    @rc.sadd(KEY, 'a')
    assert_equal(false, @rc.sismember(KEY, 'b'))
    assert_equal(true, @rc.sismember(KEY, 'a'))
  end

  def test_smembers
    value = ['a', 'b']
    @rc.sadd(KEY, value)
    ret = @rc.smembers(KEY)
    assert_equal(value, ret.sort!)
  end

  def test_smove_with_raise
    key1, key2 = get_keys_in_diff_slot
    assert_raise(Exceptions::CrossSlotsError) { @rc.smove(key1, key2, 'a') }
  end

  def test_smove
    key1, key2 = get_keys_in_same_slot
    @rc.sadd(key1, ['a', 'b'])
    @rc.sadd(key2, 'c')
    assert_equal(true, @rc.smove(key2, key1, 'c'))
    assert_equal(['a', 'b', 'c'], @rc.smembers(key1).sort!)
  end

  def test_spop
    value = ['a', 'b']
    @rc.sadd(KEY, ['a', 'b'])
    ret = @rc.spop(KEY)
    assert_equal(true, value.member?(ret))
  end

  def test_srandmember
    value = ['a', 'b']
    @rc.sadd(KEY, value)
    assert_equal(value, @rc.srandmember(KEY, 2).sort!)
  end

  def test_srem
    value = ['a', 'b']
    @rc.sadd(KEY, value)
    assert_equal(2, @rc.srem(KEY, ['a', 'b']))
    value = ['a', 'b']
    @rc.sadd(KEY, value)
    assert_equal(true, @rc.srem(KEY, 'a'))
  end

  def test_sunion_with_raise
    key1, key2 = get_keys_in_diff_slot
    assert_raise(Exceptions::CrossSlotsError) { @rc.sunion(key1, key2) }
  end

  def test_sunion
    key1, key2 = get_keys_in_same_slot
    @rc.sadd(key1, ['a', 'b'])
    @rc.sadd(key2, 'c')
    assert_equal(['a', 'b', 'c'], @rc.sunion(key1, key2).sort!)
  end

  def test_sunionstore_with_raise
    key1, key2 = get_keys_in_diff_slot
    assert_raise(Exceptions::CrossSlotsError) { @rc.sunionstore(key1, key1, key2) }
  end

  def test_sunionstore
    key1, key2 = get_keys_in_same_slot
    @rc.sadd(key1, ['a', 'b'])
    @rc.sadd(key2, 'c')
    assert_equal(3, @rc.sunionstore(key1, key1, key2))
  end

  def test_sscan
    @rc.sadd(KEY, ['a', 'b'])
    r = @rc.sscan(KEY, 0)
    r[1].sort!
    assert_equal(['0', ['a', 'b']], r)
  end

end
