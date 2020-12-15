# dependencies
require "miscreant"

# stdlib
require "forwardable"

# modules
require "cloak/utils"
require "cloak/version"

module Cloak
  class Error < StandardError; end

  autoload :Dalli, "cloak/dalli"
  autoload :Redis, "cloak/redis"

  def self.generate_key
    Miscreant::AEAD.generate_key.unpack("H*").first
  end
end
