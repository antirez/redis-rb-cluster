require_relative '../lib/exceptions'
require_relative 'test_base'
require 'test/unit'


class TestStrCmds < TestBase

  def test_append
    @rc.set(KEY, 'a')
    assert_equal(2, @rc.append(KEY, 'b'))
  end

  def test_bitcount
    @rc.set(KEY, 'a')
    assert_equal(3, @rc.bitcount(KEY))
  end

  def test_bitop_with_raise
    key1, key2 = get_keys_in_diff_slot
    assert_raise( Exceptions::CrossSlotsError) { @rc.bitop('and', 'key1', 'key2')}
  end

  def test_bitop
    key1, key2 = get_keys_in_same_slot
    @rc.set(key1, 'foobar')
    @rc.set(key2, 'abcdef')
    assert_equal(6, @rc.bitop('and', key1, key1, key2))
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

  def test_mget
    key1, key2 = get_keys_in_same_slot
    @rc.set(key1, 'a')
    @rc.set(key2, 'b')
    assert_equal(['a', 'b'], @rc.mget(key1, key2))
  end

  def test_mapped_mget
    key1, key2 = get_keys_in_same_slot
    @rc.set(key1, 'a')
    @rc.set(key2, 'b')
    ret = {
      key1 => 'a',
      key2 => 'b'
    }
    assert_equal(ret, @rc.mapped_mget(key1, key2))
  end

  def test_psetex
    assert_equal(OK, @rc.setex(KEY, 100, 10))
  end

  def test_set
    assert_equal(OK, @rc.set(KEY, 'a'))
    assert_equal('a', @rc.get(KEY))
  end

  def test_mset
    key1, key2 = get_keys_in_same_slot
    @rc.mset(key1, 'a', key2, 'b')
    assert_equal(['a', 'b'], @rc.mget(key1, key2))
  end

  def test_mapped_mset
    key1, key2 = get_keys_in_same_slot
    hash = {
      key1 => 'a',
      key2 => 'b'
    }
    assert_equal(OK, @rc.mapped_mset(hash))
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

  def test_msetnx
    key1, key2 = get_keys_in_same_slot
    assert_equal(true, @rc.msetnx(key1, 'a', key2, 'b'))
    assert_equal(false, @rc.msetnx(key1, 'a', key2, 'b'))
  end

  def test_mapped_msetnx
    key1, key2 = get_keys_in_same_slot
    hash = {
      key1 => 'a',
      key2 => 'b'
    }
    assert_equal(true, @rc.mapped_msetnx(hash))
    assert_equal(false, @rc.mapped_msetnx(hash))
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
