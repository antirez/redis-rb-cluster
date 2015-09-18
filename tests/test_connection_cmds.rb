require_relative 'test_base'

class TestConnectionCmds < TestBase

  def test_ping
    r = @rc.ping
    r.each do |node, res|
      assert_equal('PONG', res)
    end
  end

end
