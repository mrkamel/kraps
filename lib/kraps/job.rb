module Kraps
  class Job
    attr_reader :steps

    def initialize(worker:)
      @worker = worker
      @steps = []
      @partitions = 0
      @partitioner = MapReduce::HashPartitioner.new(@partitions)
    end

    def parallelize(partitions:, partitioner: MapReduce::HashPartitioner.new(partitions), worker: @worker, &block)
      # TODO: how to avoid partitions and partitioner to run out of sync

      fresh.tap do |job|
        job.instance_eval do
          @partitions = partitions
          @partitioner = partitioner

          @steps << Step.new(action: Actions::PARALLELIZE,
                             args: { partitions: @partitions, partitioner: @partitioner, worker: worker }, block: block)
        end
      end
    end

    def map(partitions: nil, partitioner: nil, worker: @worker, &block)
      fresh.tap do |job|
        job.instance_eval do
          @partitions = partitions if partitions
          @partitioner = partitioner || MapReduce::HashPartitioner.new(partitions) if partitioner || partitions

          @steps << Step.new(action: Actions::MAP,
                             args: { partitions: @partitions, partitioner: @partitioner, worker: worker }, block: block)
        end
      end
    end

    def reduce(worker: @worker, &block)
      fresh.tap do |job|
        job.instance_eval do
          @steps << Step.new(action: Actions::REDUCE,
                             args: { partitions: @partitions, partitioner: @partitioner, worker: worker }, block: block)
        end
      end
    end

    def each_partition(worker: @worker, &block)
      fresh.tap do |job|
        job.instance_eval do
          @steps << Step.new(action: Actions::EACH_PARTITION,
                             args: { partitions: @partitions, partitioner: @partitioner, worker: worker }, block: block)
        end
      end
    end

    def repartition(partitions:, partitioner: nil, worker: @worker, &block)
      map(partitions: partitions, partitioner: partitioner, worker: worker, &block)
    end

    def fresh
      dup.tap do |job|
        job.instance_variable_set(:@steps, @steps.dup)
      end
    end
  end
end
