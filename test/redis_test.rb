require_relative "test_helper"

class RedisTest < Minitest::Test
  def setup
    redis.flushall
  end

  def test_debug_object
    skip "ERR DEBUG command not allowed" if server_version >= 7

    redis.set("hello", "world")
    assert_match " refcount:1 ", redis.debug("object", "hello")
  end

  def test_debug_object_missing
    skip "ERR DEBUG command not allowed" if server_version >= 7

    error = assert_raises(Redis::CommandError) do
      redis.debug("object", "missing")
    end
    assert_equal "ERR no such key", error.message
  end

  def test_ping
    assert_equal "PONG", redis.ping
    assert_equal "hello", redis.ping("hello")
  end

  def test_echo
    assert_equal "hello", redis.echo("hello")
  end

  def test_del
    redis.set("hello", "world")
    assert_equal "world", redis.get("hello")
    redis.del("hello")
    assert_nil redis.get("hello")
  end

  def test_keys
    redis.set("k1", "v1")
    redis.set("k2", "v2")
    assert_equal ["k1", "k2"], redis.keys.sort
    assert_equal ["k1", "k2"], redis.keys("*").sort
  end

  def test_move
    redis.set("foo", "bar")
    assert redis.move("foo", 2)
    refute redis.exists?("foo")
    redis.select(2)
    assert redis.exists?("foo")
    assert_equal "bar", redis.get("foo")
  end

  def test_object
    redis.lpush("mylist", "hello")
    assert redis.object("help")
    assert_equal 1, redis.object("refcount", "mylist")
    assert_equal "quicklist", redis.object("encoding", "mylist")
    assert_equal 0, redis.object("idletime", "mylist")
  end

  def test_randomkey
    assert_nil redis.randomkey
    redis.set("hello", "world")
    assert_equal "hello", redis.randomkey
  end

  def test_rename
    redis.set("foo", "bar")
    redis.rename("foo", "baz")
    refute redis.exists?("foo")
    assert_equal "bar", redis.get("baz")
  end

  def test_incr_decr
    assert_equal 1, redis.incr("hello")
    assert_equal 4, redis.incrby("hello", 3)
    assert_equal 3, redis.decr("hello")
    assert_equal 1, redis.decrby("hello", 2)
  end

  def test_incr_set
    redis.set("hello", 3)
    error = assert_raises(Redis::CommandError) do
      redis.incr("hello")
    end
    assert_equal "ERR value is not an integer or out of range", error.message
  end

  def test_incrbyfloat
    assert_equal 1.5, redis.incrbyfloat("hello", 1.5)
  end

  def test_set_get
    redis.set("hello", "world")
    assert_equal "world", redis.get("hello")
  end

  def test_set_nil
    redis.set("hello", nil)
    assert_equal "", redis.get("hello")
  end

  def test_setex
    redis.setex("hello", 10, nil)
    assert_equal 10, redis.ttl("hello")
  end

  def test_mset_mget
    redis.mset("k1", "v1", "k2", "v2", "k3", nil)
    assert_equal ["v1", "v2", "", nil], redis.mget("k1", "k2", "k3", "missing")
    expected = {"k1" => "v1", "k2" => "v2", "k3" => "", "missing" => nil}
    assert_equal expected, redis.mapped_mget("k1", "k2", "k3", "missing")
  end

  def test_get_missing
    assert_nil redis.get("hello")
  end

  def test_getrange
    redis.set("mykey", "This is a string")
    assert_equal "This", redis.getrange("mykey", 0, 3)
    assert_equal "ing", redis.getrange("mykey", -3, -1)
    assert_equal "This is a string", redis.getrange("mykey", 0, -1)
    assert_equal "string", redis.getrange("mykey", 10, 100)
  end

  def test_getbit
    redis.set("hello", "world")
    assert_equal 0, redis.getbit("hello", 0)
    assert_equal 1, redis.getbit("hello", 1)
    assert_equal 1, redis.getbit("hello", 2)
    assert_equal 1, redis.getbit("hello", 3)
    assert_equal 0, redis.getbit("hello", 4)
    assert_equal 0, redis.getbit("hello", 1000)
  end

  def test_getbit_missing
    assert_equal 0, redis.getbit("missing", 0)
  end

  def test_bitcount
    redis.set("mykey", "foobar")
    assert_equal 26, redis.bitcount("mykey")
    assert_equal 4, redis.bitcount("mykey", 0, 0)
    assert_equal 6, redis.bitcount("mykey", 1, 1)
  end

  def test_bitpos
    redis.set("mykey", "\xff\xf0\x00")
    assert_equal 12, redis.bitpos("mykey", 0)
    redis.set("mykey", "\x00\xff\xf0")
    assert_equal 8, redis.bitpos("mykey", 1, 0)
    assert_equal 16, redis.bitpos("mykey", 1, 2)
    redis.set("mykey", "\x00\x00\x00")
    assert_equal(-1, redis.bitpos("mykey", 1))
  end

  def test_getset
    redis.set("hello", "world")
    assert_equal "world", redis.getset("hello", "space")
    assert_equal "space", redis.get("hello")
  end

  def test_strlen
    redis.set("hello", "world")
    assert_equal 5, redis.strlen("hello")
    assert_equal 0, redis.strlen("missing")
  end

  def test_list
    redis.rpush("mylist", "v1")
    redis.rpush("mylist", "v2")
    assert_equal "v1", redis.lindex("mylist", 0)
  end

  def test_brpop
    redis.rpush("mylist", "v1")
    assert_equal ["mylist", "v1"], redis.brpop("mylist")
    redis.rpush("mylist2", "v1")
    assert_equal ["mylist2", "v1"], redis.brpop(["mylist", "mylist2"])
    assert_nil redis.brpop("mylist", timeout: 1)
  end

  def test_set
    redis.sadd("myset", "hello")
    redis.sadd("myset", "world")
    redis.sadd("myset", "world")
    assert_equal 2, redis.scard("myset")
    assert_equal ["hello", "world"], redis.smembers("myset").sort
    assert redis.sismember("myset", "world")
    redis.srem("myset", "world")
    refute redis.sismember("myset", "world")
    assert_equal ["hello"], redis.smembers("myset")
  end

  def test_sdiff
    ["a", "b", "c"].map { |v| redis.sadd("key1", v) }
    ["c", "d", "e"].map { |v| redis.sadd("key2", v) }
    assert_equal ["a", "b"], redis.sdiff("key1", "key2").sort
  end

  def test_sdiffstore
    ["a", "b", "c"].map { |v| redis.sadd("key1", v) }
    ["c", "d", "e"].map { |v| redis.sadd("key2", v) }
    redis.sdiffstore("key", "key1", "key2")
    assert_equal ["a", "b"], redis.smembers("key").sort
  end

  def test_sinter
    ["a", "b", "c"].map { |v| redis.sadd("key1", v) }
    ["c", "d", "e"].map { |v| redis.sadd("key2", v) }
    assert_equal ["c"], redis.sinter("key1", "key2").sort
  end

  def test_sinterstore
    ["a", "b", "c"].map { |v| redis.sadd("key1", v) }
    ["c", "d", "e"].map { |v| redis.sadd("key2", v) }
    redis.sinterstore("key", "key1", "key2")
    assert_equal ["c"], redis.smembers("key").sort
  end

  def test_sunion
    ["a", "b", "c"].map { |v| redis.sadd("key1", v) }
    ["c", "d", "e"].map { |v| redis.sadd("key2", v) }
    assert_equal ["a", "b", "c", "d", "e"], redis.sunion("key1", "key2").sort
  end

  def test_sunionstore
    ["a", "b", "c"].map { |v| redis.sadd("key1", v) }
    ["c", "d", "e"].map { |v| redis.sadd("key2", v) }
    redis.sunionstore("key", "key1", "key2")
    assert_equal ["a", "b", "c", "d", "e"], redis.smembers("key").sort
  end

  def test_hash
    redis.hset("myhash", "hello", "world")
    assert_equal "world", redis.hget("myhash", "hello")
    assert_equal ["world"], redis.hmget("myhash", "hello")
    assert_nil redis.hget("myhash", "missing")
    assert_equal ["hello"], redis.hkeys("myhash")
    assert_equal ["world"], redis.hvals("myhash")
    expected = {"hello" => "world"}
    assert_equal expected, redis.hgetall("myhash")
    assert_equal expected, redis.mapped_hmget("myhash", "hello")
    assert_equal 1, redis.hlen("myhash")
  end

  def test_hmset
    redis.hmset("myhash", "f1", "v1", "f2", "v2")
    expected = {"f1" => "v1", "f2" => "v2"}
    assert_equal expected, redis.hgetall("myhash")
  end

  def test_bzpopmax
    redis.zadd("zset1", [[0, "a"], [1, "b"], [2, "c"]])
    assert_equal ["zset1", "c", "2"], redis.bzpopmax("zset1", "zset2")
  end

  def test_bzpopmin
    redis.zadd("zset1", [[0, "a"], [1, "b"], [2, "c"]])
    assert_equal ["zset1", "a", "0"], redis.bzpopmin("zset1", "zset2")
  end

  def test_zset
    redis.zadd("myzset", 1, "one")
    redis.zadd("myzset", 1, "one")
    redis.zadd("myzset", [[2, "two"], [3, "three"]])
    assert_equal 3, redis.zcard("myzset")
    assert_equal ["one", "two", "three"], redis.zrange("myzset", 0, -1)
    expected = [["one", 1.0], ["two", 2.0], ["three", 3.0]]
    assert_equal expected, redis.zrange("myzset", 0, -1, with_scores: true)
    redis.zincrby("myzset", 3, "one")
    assert_equal ["two", "three", "one"], redis.zrange("myzset", 0, -1)
    assert_equal 4, redis.zscore("myzset", "one")
    assert_equal 2, redis.zrank("myzset", "one")
    assert_equal 0, redis.zrevrank("myzset", "one")
    assert_equal ["one", 4], redis.zpopmax("myzset")
    assert_equal ["two", 2], redis.zpopmin("myzset")
  end

  def test_zrange
    redis.zadd("myzset", [[1, "a"], [1, "b"], [1, "c"], [0, "d"], [0, "e"], [2, "f"], [2, "g"]])
    res = redis.zrange("myzset", 0, -1)
    assert_equal ["d", "e"], res[0..1].sort
    assert_equal ["a", "b", "c"], res[2..4].sort
    assert_equal ["f", "g"], res[5..6].sort
  end

  def test_zrange_with_scores
    redis.zadd("myzset", [[1, "a"], [1, "b"], [1, "c"], [0, "d"], [0, "e"], [2, "f"], [2, "g"]])
    res = redis.zrange("myzset", 0, -1, with_scores: true)
    assert_equal [["d", 0], ["e", 0]], res[0..1].sort
    assert_equal [["a", 1], ["b", 1], ["c", 1]], res[2..4].sort
    assert_equal [["f", 2], ["g", 2]], res[5..6].sort
  end

  # can't guarantee order, even in memory
  def test_zrange_order
    redis.zadd("myzset", [[1, "a"], [1, "b"], [1, "c"], [0, "d"], [0, "e"]])
    res = redis.zrange("myzset", 0, 2)
    assert_equal ["d", "e"], res[0..1].sort
    assert_includes ["a", "b", "c"], res[2]
  end

  def test_zrevrange
    redis.zadd("myzset", [[1, "a"], [1, "b"], [1, "c"], [0, "d"], [0, "e"], [2, "f"], [2, "g"]])
    res = redis.zrevrange("myzset", 0, -1)
    assert_equal ["f", "g"], res[0..1].sort
    assert_equal ["a", "b", "c"], res[2..4].sort
    assert_equal ["d", "e"], res[5..6].sort
  end

  def test_zrevrange_with_scores
    redis.zadd("myzset", [[1, "a"], [1, "b"], [1, "c"], [0, "d"], [0, "e"], [2, "f"], [2, "g"]])
    res = redis.zrevrange("myzset", 0, -1, with_scores: true)
    assert_equal [["f", 2], ["g", 2]], res[0..1].sort
    assert_equal [["a", 1], ["b", 1], ["c", 1]], res[2..4].sort
    assert_equal [["d", 0], ["e", 0]], res[5..6].sort
  end

  def test_zrangebyscore
    redis.zadd("myzset", [[1, "one"], [2, "two"], [3, "three"]])
    assert_equal ["one", "two"], redis.zrangebyscore("myzset", 1, 2)
    assert_equal [["one", 1], ["two", 2]], redis.zrangebyscore("myzset", 1, 2, with_scores: true)
  end

  def test_zrevrangebyscore
    redis.zadd("myzset", [[1, "one"], [2, "two"], [3, "three"]])
    assert_equal ["two", "one"], redis.zrevrangebyscore("myzset", 2, 1)
    assert_equal [["two", 2], ["one", 1]], redis.zrevrangebyscore("myzset", 2, 1, with_scores: true)
  end

  def test_zremrangebyscore
    redis.zadd("myzset", [[1, "one"], [2, "two"], [3, "three"]])
    assert_equal 1, redis.zremrangebyscore("myzset", "-inf", "(2")
    assert_equal ["two", "three"], redis.zrange("myzset", 0, -1)
  end

  def test_zcount
    ["one", "two", "three"].each_with_index { |v, i| redis.zadd("myzset", i + 1, v) }
    assert_equal 3, redis.zcount("myzset", "-inf", "+inf")
    assert_equal 2, redis.zcount("myzset", "(1", "3")
  end

  def test_zinterstore
    redis.zadd("zset1", [[1, "one"], [2, "two"]])
    redis.zadd("zset2", [[1, "one"], [2, "two"], [3, "three"]])
    redis.zinterstore("out", ["zset1", "zset2"], weights: [2, 3])
    assert_equal [["one", 5], ["two", 10]], redis.zrange("out", 0, -1, with_scores: true)
  end

  def test_zunionstore
    redis.zadd("zset1", [[1, "one"], [2, "two"]])
    redis.zadd("zset2", [[1, "one"], [2, "two"], [3, "three"]])
    redis.zunionstore("out", ["zset1", "zset2"], weights: [2, 3])
    assert_equal [["one", 5], ["three", 9], ["two", 10]], redis.zrange("out", 0, -1, with_scores: true)
  end

  def test_pipelined
    res = redis.pipelined do
      redis.set("hello", "world")
    end
    assert_equal ["OK"], res
    assert_equal "world", redis.get("hello")
  end

  def test_pipelined_block_parameter
    error = assert_raises(Cloak::Error) do
      redis.pipelined do |pipeline|
      end
    end
    assert_equal "pipelined with block parameter not supported yet", error.message
  end

  def test_pipelined_futures
    val = nil
    redis.pipelined do
      redis.set("hello", "world")
      val = redis.get("hello")
    end
    assert_equal "world", val.value
  end

  def test_multi
    res = redis.multi do
      redis.set("hello", "world")
    end
    assert_equal ["OK"], res
    assert_equal "world", redis.get("hello")
  end

  def test_multi_block_parameter
    error = assert_raises(Cloak::Error) do
      redis.multi do |transaction|
      end
    end
    assert_equal "multi with block parameter not supported yet", error.message
  end

  def test_scan
    redis.mset("a", "v1", "b", "v2", "c", "v3")
    res = redis.scan(0)
    assert_equal "0", res[0]
    assert_equal ["a", "b", "c"], res[1].sort
    assert_equal ["a", "b", "c"], redis.scan_each.to_a.sort
  end

  def test_scan_empty
    res = redis.scan(0)
    assert_equal "0", res[0]
    assert_empty res[1]
  end

  def test_hscan
    redis.hmset("myhash", "a", "v1", "b", "v2", "c", "v3")
    res = redis.hscan("myhash", 0)
    assert_equal "0", res[0]
    assert_equal [["a", "v1"], ["b", "v2"], ["c", "v3"]], res[1].sort
    assert_equal [["a", "v1"], ["b", "v2"], ["c", "v3"]], redis.hscan_each("myhash").to_a.sort
  end

  def test_hscan_empty
    res = redis.hscan("myhash", 0)
    assert_equal "0", res[0]
    assert_empty res[1]
  end

  def test_zscan
    ["a", "b", "c"].each_with_index { |v, i| redis.zadd("myzset", i, v) }
    res = redis.zscan("myzset", 0)
    assert_equal "0", res[0]
    assert_equal [["a", 0.0], ["b", 1.0], ["c", 2.0]], res[1].sort
    assert_equal [["a", 0.0], ["b", 1.0], ["c", 2.0]], redis.zscan_each("myzset").to_a.sort
  end

  def test_zscan_empty
    res = redis.sscan("myzset", 0)
    assert_equal "0", res[0]
    assert_empty res[1]
  end

  def test_sscan
    ["a", "b", "c"].each { |v| redis.sadd("myset", v) }
    res = redis.sscan("myset", 0)
    assert_equal "0", res[0]
    assert_equal ["a", "b", "c"], res[1].sort
    assert_equal ["a", "b", "c"], redis.sscan_each("myset").to_a.sort
  end

  def test_sscan_empty
    res = redis.sscan("myset", 0)
    assert_equal "0", res[0]
    assert_empty res[1]
  end

  def test_pfadd
    assert_equal true, redis.pfadd("hll", "a")
    assert_equal true, redis.pfadd("hll", %w(a b c))
  end

  def test_pfcount
    assert_equal true, redis.pfadd("hll", %w(a b c d e f g))
    assert_equal 7, redis.pfcount("hll")
  end

  def test_pfmerge
    redis.pfadd("hll1", %w(foo bar zap a))
    redis.pfadd("hll2", %w(a b c foo))
    redis.pfmerge("hll3", "hll1", "hll2")
    assert_equal 6, redis.pfcount("hll3")
  end

  def test_decryption_failed
    redis.incr("mykey")
    error = assert_raises(Cloak::Error) do
      redis.get("mykey")
    end
    assert_equal "Decryption failed", error.message
  end

  def redis
    @redis ||= begin
      options = {}
      options[:logger] = $logger if Redis::VERSION.to_i < 5
      Cloak::Redis.new(key: Cloak.generate_key, **options)
    end
  end

  def server_version
    redis.info["redis_version"].to_f
  end
end
