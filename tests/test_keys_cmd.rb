require_relative '../lib/exceptions'
require_relative 'test_base'

class TestKeysCmd < TestBase

  def test_del
    key1, key2 = get_keys_in_diff_slot
    @rc.set(key1, 1)
    assert_equal(1, @rc.del(key1, key2))
  end

  def test_exists
    assert_equal(false, @rc.exists(KEY))
  end

  def test_expire_and_ttl
    @rc.set(KEY, 1)
    assert_equal(true, @rc.expire(KEY, 100))
    assert_equal(100, @rc.ttl(KEY))
  end

  def test_expireat
    @rc.set(KEY, 1)
    assert_equal(true, @rc.expireat(KEY, Time.now.to_i))
  end

  def test_keys
    @rc.set('two', 1)
    @rc.set('one', 1)
    assert_equal(['one', 'two'], @rc.keys("*o*").sort!)
  end

  def test_persist
    @rc.set(KEY, 1)
    @rc.expire(KEY, 100)
    @rc.persist(KEY)
    assert_equal(-1, @rc.ttl(KEY))
  end

  def test_pexpire
    @rc.set(KEY, 1)
    assert_equal(true, @rc.pexpire(KEY, 1000))
    assert_equal(1, @rc.ttl(KEY))
  end

  def test_pexpireat
    @rc.set(KEY, 1)
    assert_equal(true, @rc.pexpireat(KEY, (Time.now.to_f * 1000).to_i))
  end

  def test_pttl
    @rc.set(KEY, 1)
    @rc.pexpire(KEY, 1000)
    assert_instance_of(Fixnum, @rc.pttl(KEY))
  end

  def test_monitor
    assert_raise(NotImplementedError) { @rc.monitor }
  end

end
