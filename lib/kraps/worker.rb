module Kraps
  class Worker
    def initialize(json, memory_limit:, chunk_limit:, concurrency:)
      @args = JSON.parse(json)
      @memory_limit = memory_limit
      @chunk_limit = chunk_limit
      @concurrency = concurrency
    end

    def call(retries: 3)
      return if distributed_job.stopped?

      raise(InvalidAction, "Invalid action #{step.action}") unless Actions::ALL.include?(step.action)

      with_retries(retries) do # TODO: allow to use queue based retries
        send(:"perform_#{step.action}")

        distributed_job.done(@args["part"])
      end
    end

    private

    def perform_parallelize
      implementation = Class.new do
        def map(key)
          yield(key, nil)
        end
      end

      mapper = MapReduce::Mapper.new(implementation.new, partitioner: partitioner, memory_limit: @memory_limit)
      mapper.map(@args["item"])

      mapper.shuffle(chunk_limit: @chunk_limit) do |partitions|
        Parallelizer.each(partitions.to_a, @concurrency) do |partition, path|
          File.open(path) do |stream|
            Kraps.driver.driver.store(Kraps.driver.with_prefix("#{@args["token"]}/#{partition}/chunk.#{@args["part"]}.json"), stream, Kraps.driver.bucket)
          end
        end
      end
    end

    def perform_map
      temp_paths = TempPaths.new

      files = Kraps.driver.driver.list(Kraps.driver.bucket, prefix: Kraps.driver.with_prefix("#{@args["frame"]["token"]}/#{@args["partition"]}/")).sort

      temp_paths_index = files.each_with_object({}) do |file, hash|
        hash[file] = temp_paths.add
      end

      Parallelizer.each(files, @concurrency) do |file|
        Kraps.driver.driver.download(file, Kraps.driver.bucket, temp_paths_index[file].path)
      end

      current_step = step

      implementation = Object.new
      implementation.define_singleton_method(:map) do |key, value, &block|
        current_step.block.call(key, value, block)
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
            Kraps.driver.driver.store(
              Kraps.driver.with_prefix("#{@args["token"]}/#{partition}/chunk.#{@args["part"]}.json"), stream, Kraps.driver.bucket
            )
          end
        end
      end
    ensure
      temp_paths&.unlink
    end

    def perform_reduce
      current_step = step

      implementation = Object.new
      implementation.define_singleton_method(:reduce) do |key, value1, value2|
        current_step.block.call(key, value1, value2)
      end

      reducer = MapReduce::Reducer.new(implementation)

      Parallelizer.each(Kraps.driver.driver.list(Kraps.driver.bucket, prefix: Kraps.driver.with_prefix("#{@args["frame"]["token"]}/#{@args["partition"]}/")), @concurrency) do |file|
        Kraps.driver.driver.download(file, Kraps.driver.bucket, reducer.add_chunk)
      end

      tempfile = Tempfile.new

      reducer.reduce(chunk_limit: @chunk_limit) do |key, value|
        tempfile.puts(JSON.generate([key, value]))
      end

      Kraps.driver.driver.store(Kraps.driver.with_prefix("#{@args["token"]}/#{@args["partition"]}/chunk.#{@args["part"]}.json"), tempfile.tap(&:rewind), Kraps.driver.bucket)
    ensure
      tempfile&.close(true)
    end

    def perform_each_partition
      temp_paths = TempPaths.new

      files = Kraps.driver.driver.list(Kraps.driver.bucket, prefix: Kraps.driver.with_prefix("#{@args["frame"]["token"]}/#{@args["partition"]}/")).sort

      temp_paths_index = files.each_with_object({}) do |file, hash|
        hash[file] = temp_paths.add
      end

      Parallelizer.each(files, @concurrency) do |file|
        Kraps.driver.driver.download(file, Kraps.driver.bucket, temp_paths_index[file].path)
      end

      enum = Enumerator::Lazy.new(temp_paths) do |yielder, temp_path|
        File.open(temp_path.path) do |stream|
          stream.each_line do |line|
            yielder << JSON.parse(line)
          end
        end
      end

      step.block.call(@args["partition"], enum)
    ensure
      temp_paths&.unlink
    end

    def with_retries(num_retries)
      retries = 0

      begin
        yield
      rescue Kraps::Error
        distributed_job.stop
      rescue StandardError
        sleep(5)
        retries += 1

        if retries >= num_retries
          distributed_job.stop
          raise
        end

        retry
      end
    end

    def jobs
      @jobs ||= Array(@args["klass"].constantize.new.call(*@args["args"], **@args["kwargs"].transform_keys(&:to_sym)))
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

    def partitioner
      @partitioner ||= step.args[:partitioner]
    end

    def distributed_job
      @distributed_job ||= Kraps.distributed_job_client.build(token: @args["token"])
    end
  end
end
