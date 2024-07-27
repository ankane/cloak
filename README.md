# Cloak

:fire: Application-level encryption for Redis and Memcached

Encrypts keys, values, list elements, set members, and hash fields while still being able to perform a majority of operations :tada:

See [technical details](#technical-details) for more info.

[![Build Status](https://github.com/ankane/cloak/actions/workflows/build.yml/badge.svg)](https://github.com/ankane/cloak/actions)

## Installation

Add this line to your application’s Gemfile:

```ruby
gem "cloak-rb"
```

## Getting Started

Generate a key

```ruby
Cloak.generate_key
```

Store the key with your other secrets. This is typically an environment variable ([dotenv](https://github.com/bkeepers/dotenv) is great for this) or Rails credentials. Be sure to use different keys in development and production. Set the following environment variable with your key (you can use this one in development)

```sh
CLOAK_KEY=0000000000000000000000000000000000000000000000000000000000000000
```

or add it to your credentials for each environment (`rails credentials:edit --environment <env>` for Rails 6+)

```yml
cloak_key: "0000000000000000000000000000000000000000000000000000000000000000"
```

Then follow the instructions for your key-value store.

- [Redis](#redis)
- [Memcached](#memcached)

## Redis

*Requires the [redis](https://github.com/redis/redis-rb) gem*

Create a client

```ruby
redis = Cloak::Redis.new(key: key)
```

And use it in place of a `Redis` instance.

```ruby
redis.set("hello", "world")
redis.get("hello")
```

A few methods aren’t supported:

- `lrem` since encrypted list elements aren’t comparable
- `setrange`, `setbit`, `append`, and `bitop` since encrypted strings can’t be modified in-place

Also, for sorted sets, members having the same score are not guaranteed to be returned in lexographical order.

## Memcached

*Requires the [dalli](https://github.com/petergoldstein/dalli) gem*

Create a client

```ruby
dalli = Cloak::Dalli.new(key: key)
```

And use it in place of a `Dalli::Client` instance.

```ruby
dalli.set("hello", "world")
dalli.get("hello")
```

## Technical Details

Cloak uses [AES-SIV](https://github.com/miscreant/meta/wiki/AES-SIV), which supports deterministic encryption. Unlike most encryption algorithms, AES-SIV supports nonce reuse without catastrophic failure (like AES-GCM) or leaking prefix information (like AES-CBC).

- Items that need to be comparable across keys use a fixed nonce (keys, set members, HyperLogLog elements)
- Items that need to be comparable within a key use a key-specific nonce (hash fields)
- Other items use a random nonce (string values, list elements, hash values)

The fixed nonces are `\x00` bytes for keys, `\x01` bytes for set members, and `\x02` bytes for HyperLogLog elements. Key-specific nonces for hash fields are the first 16 bytes of encrypted key.

Commands, expiration times, increment/decrement values, and sorted set scores are not encrypted.

## Key Rotation

Key rotation is not supported right now, but may be possible in a limited capacity in the future.

## Credits

Thanks to [Miscreant](https://github.com/miscreant/miscreant.rb) for AES-SIV encryption.

## History

View the [changelog](https://github.com/ankane/cloak/blob/master/CHANGELOG.md)

## Contributing

Everyone is encouraged to help improve this project. Here are a few ways you can help:

- [Report bugs](https://github.com/ankane/cloak/issues)
- Fix bugs and [submit pull requests](https://github.com/ankane/cloak/pulls)
- Write, clarify, or fix documentation
- Suggest or add new features

To get started with development:

```sh
git clone https://github.com/ankane/cloak.git
cd cloak
bundle install
bundle exec rake test
```
