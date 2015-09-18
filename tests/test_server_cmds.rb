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

  def test_flushdb
    ret = @rc.flushdb
    ret.each do |node, res|
      assert_equal(OK, res)
    end
  end

end
