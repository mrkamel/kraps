require "attachie"
require "attachie/s3_driver"
require "attachie/fake_driver"

module Kraps
  module Drivers
    module Driver
      def with_prefix(path)
        File.join(*[@prefix, path].compact)
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

      def initialize(bucket:, prefix:)
        @driver = Attachie::FakeDriver.new
        @bucket = bucket
        @prefix = prefix
      end
    end
  end
end
