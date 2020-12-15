require "bundler/setup"
Bundler.require(:default)
require "minitest/autorun"
require "minitest/pride"
require "logger"

$logger = Logger.new(ENV["VERBOSE"] ? STDOUT : nil)
$logger.formatter = ->(severity, datetime, progname, msg) { "#{msg}\n" }

Dalli.logger = $logger
