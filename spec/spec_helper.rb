require "kraps"

FakeDriver = Kraps::Drivers::FakeDriver.new(bucket: "bucket", prefix: "prefix")
RedisClient = Redis.new

RSpec.configure do |config|
  # Enable flags like --only-failures and --next-failure
  config.example_status_persistence_file_path = ".rspec_status"

  # Disable RSpec exposing methods globally on `Module` and `main`
  config.disable_monkey_patching!

  config.expect_with :rspec do |c|
    c.syntax = :expect
  end

  config.before do
    Kraps.configure(driver: FakeDriver, redis: RedisClient)
  end

  config.after do
    FakeDriver.driver.flush
    RedisClient.flushdb
  end
end
