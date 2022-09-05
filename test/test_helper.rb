require "bundler/setup"
Bundler.require(:default)
require "minitest/autorun"
require "minitest/pride"
require "logger"

$logger = Logger.new(ENV["VERBOSE"] ? STDOUT : nil)
$logger.formatter = ->(severity, datetime, progname, msg) { "#{msg}\n" }

Dalli.logger = $logger

module RedisInstrumentation
  def call(command, redis_config)
    $logger.info(command.inspect)
    super
  end

  def call_pipelined(commands, redis_config)
    $logger.info(commands.inspect)
    super
  end
end
RedisClient.register(RedisInstrumentation) if defined?(RedisClient)
