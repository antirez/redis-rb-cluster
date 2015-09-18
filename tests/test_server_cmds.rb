require_relative 'test_base'

class TestSeverCmds < TestBase

  def test_config
    time = 60
    @rc.config(:set, 'tcp-keepalive', time)
    cfg = @rc.config(:get, 'tcp-keepalive')
    cfg.each do |node, c|
      assert_equal(time.to_s, c['tcp-keepalive'])
    end
  end

  def test_dbsize
    assert_instance_of(Hash, @rc.dbsize)
  end

  def test_flushdb
    ret = @rc.flushdb
    ret.each do |node, res|
      assert_equal(OK, res)
    end
  end

  def test_flushall
    ret = @rc.flushall
    ret.each do |node, res|
      assert_equal(OK, res)
    end
  end

  def test_info
    assert_instance_of(Hash, @rc.info)
  end

  def test_shutdown
    # Something cannot be easily tested...
  end

  def test_slowlog
    assert_instance_of(Hash, @rc.slowlog(:get, 2))
  end

  def test_time
    assert_instance_of(Hash, @rc.time)
  end

end
