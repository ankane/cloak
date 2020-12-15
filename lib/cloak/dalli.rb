require "dalli"

module Cloak
  # don't extend Dalli::Client so we can confirm operations are safe before adding
  class Dalli
    extend Forwardable
    include Utils

    def_delegators :@dalli, :flush, :flush_all, :stats, :reset_stats, :alive!, :version, :reset, :close

    # need to use servers = nil instead of *args for Ruby < 2.7
    def initialize(servers = nil, key: nil, **options)
      @dalli = ::Dalli::Client.new(servers, options)
      create_encryptor(key)
    end

    def get(key, options = nil)
      decrypt_value(@dalli.get(encrypt_key(key), options))
    end

    def get_multi(*keys)
      res = {}
      @dalli.get_multi(*keys.map { |k| encrypt_key(k) }).each do |k, v|
        res[decrypt_key(k)] = decrypt_value(v)
      end
      res
    end

    def fetch(key, ttl = nil, options = nil, &blk)
      wrapped_blk = proc { encrypt_value(blk.call) } if blk
      decrypt_value(@dalli.fetch(encrypt_key(key), ttl, options, &wrapped_blk))
    end

    def set(key, value, ttl = nil, options = nil)
      @dalli.set(encrypt_key(key), encrypt_value(value), ttl, options)
    end

    def add(key, value, ttl = nil, options = nil)
      @dalli.add(encrypt_key(key), encrypt_value(value), ttl, options)
    end

    def replace(key, value, ttl = nil, options = nil)
      @dalli.replace(encrypt_key(key), encrypt_value(value), ttl, options)
    end

    def delete(key)
      @dalli.delete(encrypt_key(key))
    end

    def incr(key, amt = 1, ttl = nil, default = nil)
      @dalli.incr(encrypt_key(key), amt, ttl, default)
    end

    def decr(key, amt = 1, ttl = nil, default = nil)
      @dalli.decr(encrypt_key(key), amt, ttl, default)
    end

    def touch(key, ttl = nil)
      @dalli.touch(encrypt_key(key), ttl)
    end
  end
end
