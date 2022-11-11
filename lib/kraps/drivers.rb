require "attachie"
require "attachie/s3_driver"
require "attachie/fake_driver"

module Kraps
  module Drivers
    module Driver
      def with_prefix(path)
        File.join(*[@prefix, path].compact)
      end

      def list(prefix: nil)
        driver.list(bucket, prefix: prefix)
      end

      def value(name)
        driver.value(name, bucket)
      end

      def download(name, path)
        driver.download(name, bucket, path)
      end

      def exists?(name)
        driver.exists?(name, bucket)
      end

      def store(name, data_or_io, options = {})
        driver.store(name, data_or_io, bucket, options)
      end
    end

    class S3Driver
      include Driver

      attr_reader :driver, :bucket, :prefix

      def initialize(s3_client:, bucket:, prefix: nil)
        @driver = Attachie::S3Driver.new(s3_client)
        @bucket = bucket
        @prefix = prefix
      end
    end

    class FakeDriver
      include Driver

      attr_reader :driver, :bucket, :prefix

      def initialize(bucket:, prefix: nil)
        @driver = Attachie::FakeDriver.new
        @bucket = bucket
        @prefix = prefix
      end
    end
  end
end
