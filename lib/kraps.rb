require "distributed_job"
require "ruby-progressbar"
require "ruby-progressbar/outputs/null"
require "map_reduce"
require "redis"

require_relative "kraps/version"
require_relative "kraps/drivers"
require_relative "kraps/actions"
require_relative "kraps/parallelizer"
require_relative "kraps/hash_partitioner"
require_relative "kraps/temp_path"
require_relative "kraps/temp_paths"
require_relative "kraps/timeout_queue"
require_relative "kraps/interval"
require_relative "kraps/job"
require_relative "kraps/runner"
require_relative "kraps/step"
require_relative "kraps/frame"
require_relative "kraps/worker"

module Kraps
  class Error < StandardError; end
  class InvalidAction < Error; end
  class InvalidStep < Error; end
  class JobStopped < Error; end

  def self.configure(driver:, redis: Redis.new, namespace: nil, job_ttl: 24 * 60 * 60, show_progress: true, enqueuer: ->(worker, json) { worker.perform_async(json) })
    @driver = driver
    @distributed_job_client = DistributedJob::Client.new(redis: redis, namespace: namespace, default_ttl: job_ttl)
    @show_progress = show_progress
    @enqueuer = enqueuer
  end

  def self.driver
    @driver
  end

  def self.distributed_job_client
    @distributed_job_client
  end

  def self.show_progress?
    @show_progress
  end

  def self.enqueuer
    @enqueuer
  end
end
