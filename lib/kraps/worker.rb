module Kraps
  class Worker
    include MapReduce::Mergeable

    def initialize(json, memory_limit:, chunk_limit:, concurrency:, logger: Logger.new("/dev/null"))
      @args = JSON.parse(json)
      @memory_limit = memory_limit
      @chunk_limit = chunk_limit
      @concurrency = concurrency
      @logger = logger
    end

    def call(retries: 3)
      return if redis_queue.stopped?

      raise(InvalidAction, "Invalid action #{step.action}") unless Actions::ALL.include?(step.action)

      dequeue do |payload|
        with_retries(retries) do # TODO: allow to use queue based retries
          step.before&.call

          send(:"perform_#{step.action}", payload)
        end
      end
    end

    private

    def perform_parallelize(payload)
      implementation = Class.new do
        def map(key)
          yield(key, nil)
        end
      end

      mapper = MapReduce::Mapper.new(implementation.new, partitioner: partitioner, memory_limit: @memory_limit)
      mapper.map(payload["item"])

      mapper.shuffle(chunk_limit: @chunk_limit) do |partitions|
        Parallelizer.each(partitions.to_a, @concurrency) do |partition, path|
          File.open(path) do |stream|
            Kraps.driver.store(Kraps.driver.with_prefix("#{@args["token"]}/#{partition}/chunk.#{payload["part"]}.json"), stream)
          end
        end
      end
    end

    def perform_map(payload)
      temp_paths = download_all(token: @args["frame"]["token"], partition: payload["partition"])

      current_step = step

      implementation = Object.new
      implementation.define_singleton_method(:map) do |key, value, &block|
        current_step.block.call(key, value, block)
      end

      subsequent_step = next_step

      if subsequent_step&.action == Actions::REDUCE
        implementation.define_singleton_method(:reduce) do |key, value1, value2|
          subsequent_step.block.call(key, value1, value2)
        end
      end

      mapper = MapReduce::Mapper.new(implementation, partitioner: partitioner, memory_limit: @memory_limit)

      temp_paths.each do |temp_path|
        File.open(temp_path.path) do |stream|
          stream.each_line do |line|
            key, value = JSON.parse(line)

            mapper.map(key, value)
          end
        end
      end

      mapper.shuffle(chunk_limit: @chunk_limit) do |partitions|
        Parallelizer.each(partitions.to_a, @concurrency) do |partition, path|
          File.open(path) do |stream|
            Kraps.driver.store(Kraps.driver.with_prefix("#{@args["token"]}/#{partition}/chunk.#{payload["partition"]}.json"), stream)
          end
        end
      end
    ensure
      temp_paths&.delete
    end

    def perform_map_partitions(payload)
      temp_paths = download_all(token: @args["frame"]["token"], partition: payload["partition"])

      current_step = step
      current_partition = payload["partition"]

      implementation = Object.new
      implementation.define_singleton_method(:map) do |enum, &block|
        current_step.block.call(current_partition, enum, block)
      end

      subsequent_step = next_step

      if subsequent_step&.action == Actions::REDUCE
        implementation.define_singleton_method(:reduce) do |key, value1, value2|
          subsequent_step.block.call(key, value1, value2)
        end
      end

      mapper = MapReduce::Mapper.new(implementation, partitioner: partitioner, memory_limit: @memory_limit)
      mapper.map(k_way_merge(temp_paths.each.to_a, chunk_limit: @chunk_limit))

      mapper.shuffle(chunk_limit: @chunk_limit) do |partitions|
        Parallelizer.each(partitions.to_a, @concurrency) do |partition, path|
          File.open(path) do |stream|
            Kraps.driver.store(Kraps.driver.with_prefix("#{@args["token"]}/#{partition}/chunk.#{payload["partition"]}.json"), stream)
          end
        end
      end
    ensure
      temp_paths&.delete
    end

    def perform_reduce(payload)
      current_step = step

      implementation = Object.new
      implementation.define_singleton_method(:reduce) do |key, value1, value2|
        current_step.block.call(key, value1, value2)
      end

      reducer = MapReduce::Reducer.new(implementation)

      Parallelizer.each(Kraps.driver.list(prefix: Kraps.driver.with_prefix("#{@args["frame"]["token"]}/#{payload["partition"]}/")), @concurrency) do |file|
        Kraps.driver.download(file, reducer.add_chunk)
      end

      tempfile = Tempfile.new

      reducer.reduce(chunk_limit: @chunk_limit) do |key, value|
        tempfile.puts(JSON.generate([key, value]))
      end

      Kraps.driver.store(Kraps.driver.with_prefix("#{@args["token"]}/#{payload["partition"]}/chunk.#{payload["partition"]}.json"), tempfile.tap(&:rewind))
    ensure
      tempfile&.close(true)
    end

    def perform_combine(payload)
      temp_paths1 = download_all(token: @args["frame"]["token"], partition: payload["partition"])
      temp_paths2 = download_all(token: payload["combine_frame"]["token"], partition: payload["partition"])

      enum1 = k_way_merge(temp_paths1.each.to_a, chunk_limit: @chunk_limit)
      enum2 = k_way_merge(temp_paths2.each.to_a, chunk_limit: @chunk_limit)

      combine_method = method(:combine)
      current_step = step

      implementation = Object.new
      implementation.define_singleton_method(:map) do |&block|
        combine_method.call(enum1, enum2) do |key, value1, value2|
          block.call(key, current_step.block.call(key, value1, value2))
        end
      end

      mapper = MapReduce::Mapper.new(implementation, partitioner: partitioner, memory_limit: @memory_limit)
      mapper.map

      mapper.shuffle(chunk_limit: @chunk_limit) do |partitions|
        Parallelizer.each(partitions.to_a, @concurrency) do |partition, path|
          File.open(path) do |stream|
            Kraps.driver.store(Kraps.driver.with_prefix("#{@args["token"]}/#{partition}/chunk.#{payload["partition"]}.json"), stream)
          end
        end
      end
    ensure
      temp_paths1&.delete
      temp_paths2&.delete
    end

    def combine(enum1, enum2)
      current1 = begin; enum1.next; rescue StopIteration; nil; end
      current2 = begin; enum2.next; rescue StopIteration; nil; end

      loop do
        return if current1.nil? && current2.nil?
        return if current1.nil?

        if current2.nil?
          yield(current1[0], current1[1], nil)

          current1 = begin; enum1.next; rescue StopIteration; nil; end
        elsif current1[0] == current2[0]
          loop do
            yield(current1[0], current1[1], current2[1])

            current1 = begin; enum1.next; rescue StopIteration; nil; end

            break if current1.nil?
            break if current1[0] != current2[0]
          end

          current2 = begin; enum2.next; rescue StopIteration; nil; end
        else
          res = current1[0] <=> current2[0]

          if res < 0
            yield(current1[0], current1[1], nil)

            current1 = begin; enum1.next; rescue StopIteration; nil; end
          else
            current2 = begin; enum2.next; rescue StopIteration; nil; end
          end
        end
      end
    end

    def perform_each_partition(payload)
      temp_paths = TempPaths.new

      files = Kraps.driver.list(prefix: Kraps.driver.with_prefix("#{@args["frame"]["token"]}/#{payload["partition"]}/")).sort

      temp_paths_index = files.each_with_object({}) do |file, hash|
        hash[file] = temp_paths.add
      end

      Parallelizer.each(files, @concurrency) do |file|
        Kraps.driver.download(file, temp_paths_index[file].path)
      end

      step.block.call(payload["partition"], k_way_merge(temp_paths.each.to_a, chunk_limit: @chunk_limit))
    ensure
      temp_paths&.delete
    end

    def with_retries(num_retries)
      retries = 0

      begin
        yield
      rescue Kraps::Error
        redis_queue.stop
        raise
      rescue StandardError => e
        if retries >= num_retries
          redis_queue.stop
          raise
        end
        @logger.error(e)

        sleep(5)
        retries += 1

        retry
      end
    end

    def dequeue
      loop do
        break if redis_queue.stopped?

        redis_queue.dequeue do |payload|
          return unless payload

          yield(payload)
        end
      end
    end

    def redis_queue
      @redis_queue ||= RedisQueue.new(redis: Kraps.redis, token: @args["token"], namespace: Kraps.namespace, ttl: Kraps.job_ttl)
    end

    def download_all(token:, partition:)
      temp_paths = TempPaths.new

      files = Kraps.driver.list(prefix: Kraps.driver.with_prefix("#{token}/#{partition}/")).sort

      temp_paths_index = files.each_with_object({}) do |file, hash|
        hash[file] = temp_paths.add
      end

      Parallelizer.each(files, @concurrency) do |file|
        Kraps.driver.download(file, temp_paths_index[file].path)
      end

      temp_paths
    end

    def jobs
      @jobs ||= JobResolver.new.call(@args["klass"].constantize.new.call(*@args["args"], **@args["kwargs"].transform_keys(&:to_sym)))
    end

    def job
      @job ||= begin
        job_index = @args["job_index"]

        jobs[job_index] || raise(InvalidJob, "Can't find job #{job_index}")
      end
    end

    def steps
      @steps ||= job.steps
    end

    def step
      @step ||= begin
        step_index = @args["step_index"]

        steps[step_index] || raise(InvalidStep, "Can't find step #{step_index}")
      end
    end

    def next_step
      @next_step ||= steps[@args["step_index"] + 1]
    end

    def partitioner
      @partitioner ||= proc { |key| step.partitioner.call(key, step.partitions) }
    end
  end
end
