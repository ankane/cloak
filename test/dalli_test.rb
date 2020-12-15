require_relative "test_helper"

class DalliTest < Minitest::Test
  def setup
    dalli.flush_all
  end

  def test_set_get
    dalli.set("hello", "world")
    assert_equal "world", dalli.get("hello")
  end

  def test_get_multi
    dalli.set("k1", "v1")
    dalli.set("k2", "v2")
    dalli.set("k3", nil)
    expected = {"k1" => "v1", "k2" => "v2", "k3" => nil}
    assert_equal expected, dalli.get_multi("k1", "k2", "k3", "missing")
  end

  def test_fetch
    assert_equal "v1", dalli.fetch("k1") { "v1" }
    assert_equal "v1", dalli.fetch("k1") { "v2" }
  end

  def test_fetch_missing
    assert_nil dalli.fetch("missing")
    assert_nil dalli.fetch("missing")
  end

  def test_add
    dalli.add("hello", "world")
    dalli.add("hello", "space")
    assert_equal "world", dalli.get("hello")
  end

  def test_replace
    dalli.replace("hello", "world")
    assert_nil dalli.get("hello")
    dalli.set("hello", "world")
    dalli.replace("hello", "space")
    assert_equal "space", dalli.get("hello")
  end

  def test_delete
    dalli.set("hello", "world")
    assert_equal "world", dalli.get("hello")
    dalli.delete("hello")
    assert_nil dalli.get("hello")
  end

  def test_incr_set
    dalli.set("hello", 3)
    error = assert_raises(Dalli::DalliError) do
      dalli.incr("hello")
    end
    assert_equal "Response error 6: Incr/decr on a non-numeric value", error.message
  end

  def test_incr_decr
    assert_equal 4, dalli.incr("hello", 1, nil, 4)
    assert_equal 7, dalli.incr("hello", 3)
    assert_equal 6, dalli.decr("hello")
    assert_equal 3, dalli.decr("hello", 3)
  end

  def test_touch
    dalli.touch("mykey")
  end

  def test_flush
    dalli.flush
  end

  def test_stats
    dalli.stats
    dalli.reset_stats
  end

  def dalli
    @dalli ||= Cloak::Dalli.new(key: Cloak.generate_key)
  end
end
