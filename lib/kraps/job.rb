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

    def map(partitions: nil, partitioner: nil, jobs: nil, worker: @worker, before: nil, &block)
      fresh.tap do |job|
        job.instance_eval do
          jobs = [jobs, @partitions].compact.min

          @partitions = partitions if partitions
          @partitioner = partitioner if partitioner

          @steps << Step.new(
            action: Actions::MAP,
            jobs: jobs,
            partitions: @partitions,
            partitioner: @partitioner,
            worker: worker,
            before: before,
            block: block
          )
        end
      end
    end

    def map_partitions(partitions: nil, partitioner: nil, jobs: nil, worker: @worker, before: nil, &block)
      fresh.tap do |job|
        job.instance_eval do
          jobs = [jobs, @partitions].compact.min

          @partitions = partitions if partitions
          @partitioner = partitioner if partitioner

          @steps << Step.new(
            action: Actions::MAP_PARTITIONS,
            jobs: jobs,
            partitions: @partitions,
            partitioner: @partitioner,
            worker: worker,
            before: before,
            block: block
          )
        end
      end
    end

    def reduce(jobs: nil, worker: @worker, before: nil, &block)
      fresh.tap do |job|
        job.instance_eval do
          @steps << Step.new(
            action: Actions::REDUCE,
            jobs: [jobs, @partitions].compact.min,
            partitions: @partitions,
            partitioner: @partitioner,
            worker: worker,
            before: before,
            block: block
          )
        end
      end
    end

    def combine(other_job, jobs: nil, worker: @worker, before: nil, &block)
      fresh.tap do |job|
        job.instance_eval do
          @steps << Step.new(
            action: Actions::COMBINE,
            jobs: [jobs, @partitions].compact.min,
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

    def each_partition(jobs: nil, worker: @worker, before: nil, &block)
      fresh.tap do |job|
        job.instance_eval do
          @steps << Step.new(
            action: Actions::EACH_PARTITION,
            jobs: [jobs, @partitions].compact.min,
            partitions: @partitions,
            partitioner: @partitioner,
            worker: worker,
            before: before,
            block: block
          )
        end
      end
    end

    def repartition(partitions:, jobs: nil, partitioner: nil, worker: @worker, before: nil)
      map(jobs: jobs, partitions: partitions, partitioner: partitioner, worker: worker, before: before) do |key, value, collector|
        collector.call(key, value)
      end
    end

    def dump(prefix:, worker: @worker)
      each_partition(worker: worker) do |partition, pairs|
        tempfile = Tempfile.new

        pairs.each do |pair|
          tempfile.puts(JSON.generate(pair))
        end

        Kraps.driver.store(File.join(prefix, partition.to_s, "chunk.json"), tempfile.tap(&:rewind))
      ensure
        tempfile&.close(true)
      end
    end

    def load(prefix:, partitions:, partitioner:, concurrency:, worker: @worker)
      job = parallelize(partitions: partitions, partitioner: proc { |key, _| key }, worker: worker) do |collector|
        (0...partitions).each do |partition|
          collector.call(partition)
        end
      end

      job.map_partitions(partitioner: partitioner, worker: worker) do |partition, _, collector|
        temp_paths = Downloader.download_all(prefix: File.join(prefix, partition.to_s, "/"), concurrency: concurrency)

        temp_paths.each do |temp_path|
          File.open(temp_path.path) do |stream|
            stream.each_line do |line|
              key, value = JSON.parse(line)

              collector.call(key, value)
            end
          end
        end
      ensure
        temp_paths&.delete
      end
    end

    def fresh
      dup.tap do |job|
        job.instance_variable_set(:@steps, @steps.dup)
      end
    end
  end
end
