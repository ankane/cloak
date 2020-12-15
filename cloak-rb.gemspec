require_relative "lib/cloak/version"

Gem::Specification.new do |spec|
  spec.name          = "cloak-rb"
  spec.version       = Cloak::VERSION
  spec.summary       = "Application-level encryption for Redis and Memcached"
  spec.homepage      = "https://github.com/ankane/cloak"
  spec.license       = "MIT"

  spec.author        = "Andrew Kane"
  spec.email         = "andrew@chartkick.com"

  spec.files         = Dir["*.{md,txt}", "{lib}/**/*"]
  spec.require_path  = "lib"

  spec.required_ruby_version = ">= 2.5"

  spec.add_dependency "miscreant"
end
