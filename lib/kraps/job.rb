module Kraps
  class Job
    attr_reader :steps

    def initialize(worker:)
      @worker = worker
      @steps = []
      @partitions = 0
      @partitioner = HashPartitioner.new
    end

    def parallelize(partitions:, partitioner: HashPartitioner.new, worker: @worker, &block)
      fresh.tap do |job|
        job.instance_eval do
          @partitions = partitions
          @partitioner = partitioner

          @steps << Step.new(action: Actions::PARALLELIZE, args: { partitions: @partitions, partitioner: @partitioner, worker: worker }, block: block)
        end
      end
    end

    def map(partitions: nil, partitioner: nil, worker: @worker, &block)
      fresh.tap do |job|
        job.instance_eval do
          @partitions = partitions if partitions
          @partitioner = partitioner if partitioner

          @steps << Step.new(action: Actions::MAP, args: { partitions: @partitions, partitioner: @partitioner, worker: worker }, block: block)
        end
      end
    end

    def reduce(worker: @worker, &block)
      fresh.tap do |job|
        job.instance_eval do
          @steps << Step.new(action: Actions::REDUCE, args: { partitions: @partitions, partitioner: @partitioner, worker: worker }, block: block)
        end
      end
    end

    def each_partition(worker: @worker, &block)
      fresh.tap do |job|
        job.instance_eval do
          @steps << Step.new(action: Actions::EACH_PARTITION, args: { partitions: @partitions, partitioner: @partitioner, worker: worker }, block: block)
        end
      end
    end

    def repartition(partitions:, partitioner: nil, worker: @worker)
      map(partitions: partitions, partitioner: partitioner, worker: worker) do |key, value, collector|
        collector.call(key, value)
      end
    end

    def fresh
      dup.tap do |job|
        job.instance_variable_set(:@steps, @steps.dup)
      end
    end
  end
end
