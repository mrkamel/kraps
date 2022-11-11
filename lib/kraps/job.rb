module Kraps
  class Job
    attr_reader :steps

    def initialize(worker:)
      @worker = worker
      @steps = []
      @partitions = 0
      @partitioner = HashPartitioner.new
    end

    def parallelize(partitions:, partitioner: HashPartitioner.new, worker: @worker, before: nil, &block)
      fresh.tap do |job|
        job.instance_eval do
          @partitions = partitions
          @partitioner = partitioner

          @steps << Step.new(
            action: Actions::PARALLELIZE,
            partitions: @partitions,
            partitioner: @partitioner,
            worker: worker,
            before: before,
            block: block
          )
        end
      end
    end

    def map(partitions: nil, partitioner: nil, worker: @worker, before: nil, &block)
      fresh.tap do |job|
        job.instance_eval do
          @partitions = partitions if partitions
          @partitioner = partitioner if partitioner

          @steps << Step.new(
            action: Actions::MAP,
            partitions: @partitions,
            partitioner: @partitioner,
            worker: worker,
            before: before,
            block: block
          )
        end
      end
    end

    def map_partitions(partitions: nil, partitioner: nil, worker: @worker, before: nil, &block)
      fresh.tap do |job|
        job.instance_eval do
          @partitions = partitions if partitions
          @partitioner = partitioner if partitioner

          @steps << Step.new(
            action: Actions::MAP_PARTITIONS,
            partitions: @partitions,
            partitioner: @partitioner,
            worker: worker,
            before: before,
            block: block
          )
        end
      end
    end

    def reduce(worker: @worker, before: nil, &block)
      fresh.tap do |job|
        job.instance_eval do
          @steps << Step.new(
            action: Actions::REDUCE,
            partitions: @partitions,
            partitioner: @partitioner,
            worker: worker,
            before: before,
            block: block
          )
        end
      end
    end

    def combine(other_job, worker: @worker, before: nil, &block)
      fresh.tap do |job|
        job.instance_eval do
          @steps << Step.new(
            action: Actions::COMBINE,
            partitions: @partitions,
            partitioner: @partitioner,
            worker: worker,
            before: before,
            block: block,
            dependency: other_job,
            options: { combine_step_index: other_job.steps.size - 1 }
          )
        end
      end
    end

    def each_partition(worker: @worker, before: nil, &block)
      fresh.tap do |job|
        job.instance_eval do
          @steps << Step.new(
            action: Actions::EACH_PARTITION,
            partitions: @partitions,
            partitioner: @partitioner,
            worker: worker,
            before: before,
            block: block
          )
        end
      end
    end

    def repartition(partitions:, partitioner: nil, worker: @worker, before: nil)
      map(partitions: partitions, partitioner: partitioner, worker: worker, before: before) do |key, value, collector|
        collector.call(key, value)
      end
    end

    def dump(prefix:, worker: @worker)
      each_partition(worker: worker) do |partition, pairs|
        tempfile = Tempfile.new

        pairs.each do |pair|
          tempfile.puts(JSON.generate(pair))
        end

        Kraps.driver.driver.store(File.join(prefix, partition.to_s, "chunk.json"), tempfile.tap(&:rewind), Kraps.driver.bucket)
      ensure
        tempfile&.close(true)
      end
    end

    def load(prefix:, partitions:, partitioner:, worker: @worker)
      job = parallelize(partitions: partitions, partitioner: proc { |key, _| key }, worker: worker) do |collector|
        (0...partitions).each do |partition|
          collector.call(partition)
        end
      end

      job.map_partitions(partitioner: partitioner, worker: worker) do |pairs, collector|
        tempfile = Tempfile.new

        path = File.join(prefix, pairs.first[0].to_s, "chunk.json")
        next unless Kraps.driver.driver.exists?(path, Kraps.driver.bucket)

        Kraps.driver.driver.download(path, Kraps.driver.bucket, tempfile.path)

        tempfile.each_line do |line|
          key, value = JSON.parse(line)

          collector.call(key, value)
        end
      ensure
        tempfile&.close(true)
      end
    end

    def fresh
      dup.tap do |job|
        job.instance_variable_set(:@steps, @steps.dup)
      end
    end
  end
end
