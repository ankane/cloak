require "redis"

module Cloak
  # don't extend Redis so we can confirm operations are safe before adding
  class Redis
    extend Forwardable
    include Utils

    # client setname and getname not encrypted
    def_delegators :@redis, :auth, :select, :quit, :bgrewriteaof, :bgsave,
      :config, :client, :dbsize, :flushall, :flushdb, :info, :lastsave,
      :monitor, :save, :shutdown, :slaveof, :slowlog, :sync, :time,
      :unwatch, :pipelined, :multi, :exec, :discard

    def initialize(key: nil, **options)
      @redis = ::Redis.new(**options)
      create_encryptor(key)
    end

    def debug(*args)
      args[1] = encrypt_key(args[1]) if args[0] == "object"
      @redis.debug(*args)
    end

    def ping(message = nil)
      if message.nil?
        @redis.ping
      else
        on_result(@redis.ping(encrypt_value(message))) do |res|
          decrypt_value(res)
        end
      end
    end

    def echo(value)
      on_result(@redis.echo(encrypt_value(value))) do |res|
        decrypt_value(res)
      end
    end

    def persist(key)
      @redis.persist(encrypt_key(key))
    end

    def expire(key, seconds)
      @redis.expire(encrypt_key(key), seconds)
    end

    def expireat(key, unix_time)
      @redis.expireat(encrypt_key(key), unix_time)
    end

    def ttl(key)
      @redis.ttl(encrypt_key(key))
    end

    def pexpire(key, milliseconds)
      @redis.pexpire(encrypt_key(key), milliseconds)
    end

    def pexpireat(key, ms_unix_time)
      @redis.pexpireat(encrypt_key(key), ms_unix_time)
    end

    def pttl(key)
      @redis.pttl(encrypt_key(key))
    end

    def dump(key)
      @redis.dump(encrypt_key(key))
    end

    def restore(key, ttl, serialized_value, replace: nil)
      @redis.restore(encrypt_key(key), ttl, serialized_value, replace: replace)
    end

    def del(*keys)
      @redis.del(*keys.map { |k| encrypt_key(k) })
    end

    def unlink(*keys)
      @redis.unlink(*keys.map { |k| encrypt_key(k) })
    end

    def exists(*keys)
      @redis.exists(*keys.map { |k| encrypt_key(k) })
    end

    def exists?(*keys)
      @redis.exists?(*keys.map { |k| encrypt_key(k) })
    end

    # could match in-memory
    def keys(pattern = "*")
      raise "Only * pattern supported" if pattern != "*"
      on_result(@redis.keys(pattern)) do |res|
        res.map { |k| decrypt_key(k) }
      end
    end

    def move(key, db)
      @redis.move(encrypt_key(key), db)
    end

    def object(*args)
      args[1] = encrypt_key(args[1]) if args.size > 1
      @redis.object(*args)
    end

    def randomkey
      on_result(@redis.randomkey) do |res|
        res.nil? ? res : decrypt_key(res)
      end
    end

    def rename(old_name, new_name)
      @redis.rename(encrypt_key(old_name), encrypt_key(new_name))
    end

    def renamenx(old_name, new_name)
      @redis.renamenx(encrypt_key(old_name), encrypt_key(new_name))
    end

    # sort not supported

    def type(key)
      @redis.type(encrypt_key(key))
    end

    def decr(key)
      @redis.decr(encrypt_key(key))
    end

    def decrby(key, decrement)
      @redis.decrby(encrypt_key(key), decrement)
    end

    def incr(key)
      @redis.incr(encrypt_key(key))
    end

    def incrby(key, increment)
      @redis.incrby(encrypt_key(key), increment)
    end

    def incrbyfloat(key, increment)
      @redis.incrbyfloat(encrypt_key(key), increment)
    end

    def set(key, value, **options)
      @redis.set(encrypt_key(key), encrypt_value(value), **options)
    end

    def setex(key, ttl, value)
      @redis.setex(encrypt_key(key), ttl, encrypt_value(value))
    end

    def psetex(key, ttl, value)
      @redis.psetex(encrypt_key(key), ttl, encrypt_value(value))
    end

    def setnx(key, value)
      @redis.setnx(encrypt_key(key), ttl, encrypt_value(value))
    end

    def mset(*args)
      @redis.mset(args.map.with_index { |v, i| i % 2 == 0 ? encrypt_key(v) : encrypt_value(v) })
    end

    # match redis
    def mapped_mset(hash)
      mset(hash.to_a.flatten)
    end

    def msetnx(*args)
      @redis.msetnx(args.map.with_index { |v, i| i % 2 == 0 ? encrypt_key(v) : encrypt_value(v) })
    end

    # match redis
    def mapped_msetnx(hash)
      msetnx(hash.to_a.flatten)
    end

    def get(key)
      on_result(@redis.get(encrypt_key(key))) do |res|
        decrypt_value(res)
      end
    end

    def mget(*keys, &blk)
      on_result(@redis.mget(*keys.map { |k| encrypt_key(k) }, &blk)) do |res|
        res.map { |v| decrypt_value(v) }
      end
    end

    def mapped_mget(*keys)
      on_result(@redis.mapped_mget(*keys.map { |k| encrypt_key(k) })) do |res|
        res.map { |k, v| [decrypt_key(k), decrypt_value(v)] }.to_h
      end
    end

    # setrange not supported

    def getrange(key, start, stop)
      on_result(@redis.get(encrypt_key(key))) do |res|
        decrypt_value(res)[start..stop]
      end
    end

    # setbit not supported

    # TODO raise "ERR bit offset is not an integer or out of range" when needed
    def getbit(key, offset)
      on_result(@redis.get(encrypt_key(key))) do |res|
        v = decrypt_value(res)
        v.nil? ? 0 : v.unpack1("B*")[offset].to_i
      end
    end

    # append not supported

    def bitcount(key, start = 0, stop = -1)
      on_result(@redis.get(encrypt_key(key))) do |res|
        decrypt_value(res)[start..stop].unpack1("B*").count("1")
      end
    end

    # bitop not supported

    def bitpos(key, bit, start = nil, stop = nil)
      on_result(@redis.get(encrypt_key(key))) do |res|
        pos = decrypt_value(res)[(start || 0)..(stop || -1)].unpack1("B*").index(bit.to_s)
        pos ? pos + (start.to_i * 8) : -1
      end
    end

    def getset(key, value)
      on_result(@redis.getset(encrypt_key(key), encrypt_value(value))) do |res|
        decrypt_value(res)
      end
    end

    # subtract nonce size (16) and auth tag (16)
    def strlen(key)
      on_result(@redis.strlen(encrypt_key(key))) do |res|
        res == 0 ? 0 : res - 32
      end
    end

    def llen(key)
      @redis.llen(encrypt_key(key))
    end

    def lpush(key, value)
      @redis.lpush(encrypt_key(key), value.is_a?(Array) ? value.map { |v| encrypt_element(v) } : encrypt_element(value))
    end

    def lpushx(key, value)
      @redis.lpushx(encrypt_key(key), encrypt_element(value))
    end

    def rpush(key, value)
      @redis.rpush(encrypt_key(key), value.is_a?(Array) ? value.map { |v| encrypt_element(v) } : encrypt_element(value))
    end

    def rpushx(key, value)
      @redis.rpushx(encrypt_key(key), encrypt_element(value))
    end

    def lpop(key)
      @redis.lpop(encrypt_key(key))
    end

    def rpop(key)
      @redis.rpop(encrypt_key(key))
    end

    def rpoplpush(source, destination)
      @redis.rpoplpush(encrypt_key(source), encrypt_key(destination))
    end

    def blpop(*args)
      _bpop(:blpop, args)
    end

    def brpop(*args)
      _bpop(:brpop, args)
    end

    def brpoplpush(source, destination, deprecated_timeout = 0, timeout: deprecated_timeout)
      @redis.brpoplpush(encrypt_key(source), encrypt_key(destination), timeout: timeout)
    end

    def lindex(key, index)
      on_result(@redis.lindex(encrypt_key(key), index)) do |res|
        decrypt_element(res)
      end
    end

    def linsert(key, where, pivot, value)
      @redis.linsert(encrypt_key(key), where, pivot, encrypt_element(value))
    end

    def lrange(key, start, stop)
      @redis.lrange(encrypt_key(key), start, stop)
    end

    # lrem not possible with random nonce

    def lset(key, index, value)
      @redis.lset(encrypt_key(key), index, encrypt_element(value))
    end

    def ltrim(key, start, stop)
      @redis.ltrim(encrypt_key(key), start, stop)
    end

    def scard(key)
      @redis.scard(encrypt_key(key))
    end

    def sadd(key, member)
      @redis.sadd(encrypt_key(key), encrypt_member(member))
    end

    def srem(key, member)
      @redis.srem(encrypt_key(key), encrypt_member(member))
    end

    def spop(key, count = nil)
      on_result(@redis.spop(encrypt_key(key))) do |res|
        if count.nil?
          decrypt_member(res)
        else
          res.map { |v| decrypt_member(v) }
        end
      end
    end

    def srandmember(key, count = nil)
      on_result(@redis.srandmember(encrypt_key(key))) do |res|
        if count.nil?
          decrypt_member(res)
        else
          res.map { |v| decrypt_member(v) }
        end
      end
    end

    def smove(source, destination, member)
      @redis.smove(encrypt_key(source), encrypt_key(destination), encrypt_member(member))
    end

    def sismember(key, member)
      @redis.sismember(encrypt_key(key), encrypt_member(member))
    end

    def smembers(key)
      on_result(@redis.smembers(encrypt_key(key))) do |res|
        res.map { |v| decrypt_member(v) }
      end
    end

    def sdiff(*keys)
      on_result(@redis.sdiff(*keys.map { |k| encrypt_key(k) })) do |res|
        res.map { |v| decrypt_member(v) }
      end
    end

    def sdiffstore(destination, *keys)
      @redis.sdiffstore(encrypt_key(destination), *keys.map { |k| encrypt_key(k) })
    end

    def sinter(*keys)
      on_result(@redis.sinter(*keys.map { |k| encrypt_key(k) })) do |res|
        res.map { |v| decrypt_member(v) }
      end
    end

    def sinterstore(destination, *keys)
      @redis.sinterstore(encrypt_key(destination), *keys.map { |k| encrypt_key(k) })
    end

    def sunion(*keys)
      on_result(@redis.sunion(*keys.map { |k| encrypt_key(k) })) do |res|
        res.map { |v| decrypt_member(v) }
      end
    end

    def sunionstore(destination, *keys)
      @redis.sunionstore(encrypt_key(destination), *keys.map { |k| encrypt_key(k) })
    end

    def zcard(key)
      @redis.zcard(encrypt_key(key))
    end

    def zadd(key, *args, **options)
      if args.size == 1 && args[0].is_a?(Array)
        args = args[0]
      elsif args.size == 2
        args = [args]
      else
        raise ArgumentError, "wrong number of arguments"
      end

      # convert score to numeric to avoid data leakage
      # if there's an issue with arguments
      @redis.zadd(encrypt_key(key), args.map { |v| [to_score(v[0]), encrypt_member(v[1])] }, **options)
    end

    def zincrby(key, increment, member)
      @redis.zincrby(encrypt_key(key), increment, encrypt_member(member))
    end

    def zrem(key, member)
      @redis.zrem(encrypt_key(key), member.is_a?(Array) ? member.map { |v| encrypt_member(v) } : encrypt_member(member))
    end

    def zpopmax(key, count = nil)
      on_result(@redis.zpopmax(encrypt_key(key), count)) do |res|
        if count.to_i > 1
          res.map { |v, s| [decrypt_member(v), s] }
        else
          [decrypt_member(res[0]), res[1]]
        end
      end
    end

    def zpopmin(key, count = nil)
      on_result(@redis.zpopmin(encrypt_key(key), count)) do |res|
        if count.to_i > 1
          res.map { |v, s| [decrypt_member(v), s] }
        else
          [decrypt_member(res[0]), res[1]]
        end
      end
    end

    def bzpopmax(*args)
      _bpop(:bzpopmax, args, zset: true)
    end

    def bzpopmin(*args)
      _bpop(:bzpopmin, args, zset: true)
    end

    def zscore(key, member)
      @redis.zscore(encrypt_key(key), encrypt_member(member))
    end

    # can't guarantee lexographical order without potentially fetching all elements
    def zrange(key, start, stop, withscores: false, with_scores: withscores)
      on_result(@redis.zrange(encrypt_key(key), start, stop, with_scores: with_scores)) do |res|
        if with_scores
          res.map { |v, s| [decrypt_member(v), s] }
        else
          res.map { |v| decrypt_member(v) }
        end
      end
    end

    # can't guarantee lexographical order without potentially fetching all elements
    def zrevrange(key, start, stop, withscores: false, with_scores: withscores)
      on_result(@redis.zrevrange(encrypt_key(key), start, stop, with_scores: with_scores)) do |res|
        if with_scores
          res.map { |v, s| [decrypt_member(v), s] }
        else
          res.map { |v| decrypt_member(v) }
        end
      end
    end

    def zrank(key, member)
      @redis.zrank(encrypt_key(key), encrypt_member(member))
    end

    def zrevrank(key, member)
      @redis.zrevrank(encrypt_key(key), encrypt_member(member))
    end

    def zremrangebyrank(key, start, stop)
      @redis.zremrangebyrank(encrypt_key(key), start, stop)
    end

    # zlexcount not supported (could support - + range)
    # zrangebylex not supported
    # zrevrangebylex not supported

    # could guarantee lexographical order when limit not used
    def zrangebyscore(key, min, max, withscores: false, with_scores: withscores, limit: nil)
      on_result(@redis.zrangebyscore(encrypt_key(key), min, max, with_scores: with_scores, limit: limit)) do |res|
        if with_scores
          res.map { |v, s| [decrypt_member(v), s] }
        else
          res.map { |v| decrypt_member(v) }
        end
      end
    end

    # could guarantee lexographical order when limit not used
    def zrevrangebyscore(key, max, min, withscores: false, with_scores: withscores, limit: nil)
      on_result(@redis.zrevrangebyscore(encrypt_key(key), max, min, with_scores: with_scores, limit: limit)) do |res|
        if with_scores
          res.map { |v, s| [decrypt_member(v), s] }
        else
          res.map { |v| decrypt_member(v) }
        end
      end
    end

    def zremrangebyscore(key, min, max)
      @redis.zremrangebyscore(encrypt_key(key), min, max)
    end

    def zcount(key, min, max)
      @redis.zcount(encrypt_key(key), min, max)
    end

    def zinterstore(destination, keys, weights: nil, aggregate: nil)
      @redis.zinterstore(encrypt_key(destination), keys.map { |k| encrypt_key(k) }, weights: weights, aggregate: aggregate)
    end

    def zunionstore(destination, keys, weights: nil, aggregate: nil)
      @redis.zunionstore(encrypt_key(destination), keys.map { |k| encrypt_key(k) }, weights: weights, aggregate: aggregate)
    end

    def hlen(key)
      @redis.hlen(encrypt_key(key))
    end

    def hset(key, *attrs)
      attrs = attrs.first.flatten if attrs.size == 1 && attrs.first.is_a?(Hash)

      ek = encrypt_key(key)
      @redis.hset(ek, attrs.map.with_index { |v, i| i % 2 == 0 ? encrypt_field(ek, v) : encrypt_value(v) })
    end

    def hsetnx(key, field, value)
      ek = encrypt_key(key)
      @redis.hsetnx(ek, encrypt_field(ek, field), encrypt_value(value))
    end

    def hmset(key, *attrs)
      ek = encrypt_key(key)
      @redis.hset(ek, attrs.map.with_index { |v, i| i % 2 == 0 ? encrypt_field(ek, v) : encrypt_value(v) })
    end

    # match redis
    def mapped_hmset(key, hash)
      hmset(key, hash.to_a.flatten)
    end

    def hget(key, field)
      ek = encrypt_key(key)
      on_result(@redis.hget(ek, encrypt_field(ek, field))) do |res|
        decrypt_value(res)
      end
    end

    def hmget(key, *fields, &blk)
      ek = encrypt_key(key)
      on_result(@redis.hmget(ek, *fields.map { |f| encrypt_field(ek, f) }, &blk)) do |res|
        res.map { |v| decrypt_value(v) }
      end
    end

    def mapped_hmget(key, *fields)
      ek = encrypt_key(key)
      on_result(@redis.mapped_hmget(ek, *fields.map { |f| encrypt_field(ek, f) })) do |res|
        res.map { |f, v| [decrypt_field(ek, f), decrypt_value(v)] }.to_h
      end
    end

    def hdel(key, *fields)
      ek = encrypt_key(key)
      @redis.hdel(ek, *fields.map { |v| encrypt_field(ek, v) })
    end

    def hexists(key, field)
      ek = encrypt_key(key)
      @redis.hexists(ek, encrypt_field(ek, field))
    end

    def hincrby(key, field, increment)
      ek = encrypt_key(key)
      @redis.hincrby(ek, encrypt_field(ek, field), increment)
    end

    def hincrbyfloat(key, field, increment)
      ek = encrypt_key(key)
      @redis.hincrbyfloat(ek, encrypt_field(ek, field), increment)
    end

    def hkeys(key)
      ek = encrypt_key(key)
      on_result(@redis.hkeys(ek)) do |res|
        res.map { |v| decrypt_field(ek, v) }
      end
    end

    def hvals(key)
      ek = encrypt_key(key)
      on_result(@redis.hvals(ek)) do |res|
        res.map { |v| decrypt_value(v) }
      end
    end

    def hgetall(key)
      ek = encrypt_key(key)
      on_result(@redis.hgetall(ek)) do |res|
        res.map { |f, v| [decrypt_field(ek, f), decrypt_value(v)] }.to_h
      end
    end

    # TODO pubsub
    # TODO watch

    # match option not supported
    def scan(cursor, count: nil)
      on_result(@redis.scan(cursor, count: count)) do |res|
        [res[0], res[1].map { |v| decrypt_key(v) }]
      end
    end

    # match redis
    def scan_each(**options, &block)
      return to_enum(:scan_each, **options) unless block_given?

      cursor = 0
      loop do
        cursor, keys = scan(cursor, **options)
        keys.each(&block)
        break if cursor == "0"
      end
    end

    # match option not supported
    def hscan(key, cursor, count: nil)
      ek = encrypt_key(key)
      on_result(@redis.hscan(ek, cursor, count: count)) do |res|
        [res[0], res[1].map { |v| [decrypt_field(ek, v[0]), decrypt_value(v[1])] }]
      end
    end

    # match redis
    def hscan_each(key, **options, &block)
      return to_enum(:hscan_each, key, **options) unless block_given?

      cursor = 0
      loop do
        # hscan encrypts key
        cursor, values = hscan(key, cursor, **options)
        values.each(&block)
        break if cursor == "0"
      end
    end

    # match option not supported
    def zscan(key, cursor, count: nil)
      on_result(@redis.zscan(encrypt_key(key), cursor, count: count)) do |res|
        [res[0], res[1].map { |v| [decrypt_member(v[0]), v[1]] }]
      end
    end

    # match redis
    def zscan_each(key, **options, &block)
      return to_enum(:zscan_each, key, **options) unless block_given?

      cursor = 0
      loop do
        # zscan encrypts key
        cursor, values = zscan(key, cursor, **options)
        values.each(&block)
        break if cursor == "0"
      end
    end

    # match option not supported
    def sscan(key, cursor, count: nil)
      on_result(@redis.sscan(encrypt_key(key), cursor, count: count)) do |res|
        [res[0], res[1].map { |v| decrypt_member(v) }]
      end
    end

    # match redis
    def sscan_each(key, **options, &block)
      return to_enum(:sscan_each, key, **options) unless block_given?

      cursor = 0
      loop do
        # sscan encrypts key
        cursor, keys = sscan(key, cursor, **options)
        keys.each(&block)
        break if cursor == "0"
      end
    end

    def pfadd(key, member)
      @redis.pfadd(encrypt_key(key), member.is_a?(Array) ? member.map { |v| encrypt_hll_element(v) } : encrypt_hll_element(member))
    end

    def pfcount(*keys)
      @redis.pfcount(*keys.map { |k| encrypt_key(k) })
    end

    def pfmerge(dest_key, *source_key)
      @redis.pfmerge(encrypt_key(dest_key), *source_key.map { |k| encrypt_key(k) })
    end

    # geo not supported
    # streams not supported

    private

    def on_result(res, &block)
      if res.is_a?(::Redis::Future)
        res.instance_exec do
          if @transformation
            raise "Not implemented yet. Please create an issue."
          else
            @transformation = block
          end
        end
        res
      else
        block.call(res)
      end
    end

    def _bpop(cmd, args, zset: false, &blk)
      # match redis
      timeout = if args.last.is_a?(Hash)
        options = args.pop
        options[:timeout]
      elsif args.last.respond_to?(:to_int)
        args.pop.to_int
      end

      keys = args.flatten.map { |k| encrypt_key(k) }

      on_result(@redis._bpop(cmd, [keys, {timeout: timeout}], &blk)) do |res|
        if res.nil?
          res
        elsif zset
          [decrypt_key(res[0]), decrypt_member(res[1]), res[2]]
        else
          [decrypt_key(res[0]), decrypt_element(res[1])]
        end
      end
    end

    def to_score(v)
      v.is_a?(Numeric) ? v : v.to_f
    end
  end
end
