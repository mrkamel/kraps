module Kraps
  class Runner
    def initialize(klass)
      @klass = klass
    end

    def call(*args, **kwargs)
      JobResolver.new.call(@klass.new.call(*args, **kwargs)).tap do |jobs|
        jobs.each_with_index do |job, job_index|
          job.steps.each_with_index.inject(nil) do |frame, (_, step_index)|
            StepRunner.new(
              klass: @klass,
              args: args,
              kwargs: kwargs,
              jobs: jobs,
              job_index: job_index,
              step_index: step_index,
              frame: frame
            ).call
          end
        end
      end
    end

    class StepRunner
      def initialize(klass:, args:, kwargs:, jobs:, job_index:, step_index:, frame:)
        @klass = klass
        @args = args
        @kwargs = kwargs
        @jobs = jobs
        @job_index = job_index
        @job = @jobs[@job_index]
        @step_index = step_index
        @step = @job.steps[@step_index]
        @frame = frame
      end

      def call
        raise(InvalidAction, "Invalid action #{@step.action}") unless Actions::ALL.include?(@step.action)

        @step.frame ||= send(:"perform_#{@step.action}")
      end

      private

      def perform_parallelize
        enum = Enumerator.new do |yielder|
          collector = proc { |item| yielder << item }

          @step.block.call(collector)
        end

        with_distributed_job do |distributed_job|
          push_and_wait(distributed_job, enum) do |item, part|
            enqueue(token: distributed_job.token, part: part, item: item)
          end

          Frame.new(token: distributed_job.token, partitions: @step.partitions)
        end
      end

      def perform_map
        with_distributed_job do |distributed_job|
          push_and_wait(distributed_job, 0...@frame.partitions) do |partition, part|
            enqueue(token: distributed_job.token, part: part, partition: partition)
          end

          Frame.new(token: distributed_job.token, partitions: @step.partitions)
        end
      end

      def perform_map_partitions
        with_distributed_job do |distributed_job|
          push_and_wait(distributed_job, 0...@frame.partitions) do |partition, part|
            enqueue(token: distributed_job.token, part: part, partition: partition)
          end

          Frame.new(token: distributed_job.token, partitions: @step.partitions)
        end
      end

      def perform_reduce
        with_distributed_job do |distributed_job|
          push_and_wait(distributed_job, 0...@frame.partitions) do |partition, part|
            enqueue(token: distributed_job.token, part: part, partition: partition)
          end

          Frame.new(token: distributed_job.token, partitions: @step.partitions)
        end
      end

      def perform_combine
        combine_job = @step.dependency
        combine_step = combine_job.steps[@step.options[:combine_step_index]]

        raise(IncompatibleFrame, "Incompatible number of partitions") if combine_step.partitions != @step.partitions

        with_distributed_job do |distributed_job|
          push_and_wait(distributed_job, 0...@frame.partitions) do |partition, part|
            enqueue(token: distributed_job.token, part: part, partition: partition, combine_frame: combine_step.frame.to_h)
          end

          Frame.new(token: distributed_job.token, partitions: @step.partitions)
        end
      end

      def perform_each_partition
        with_distributed_job do |distributed_job|
          push_and_wait(distributed_job, 0...@frame.partitions) do |partition, part|
            enqueue(token: distributed_job.token, part: part, partition: partition)
          end

          @frame
        end
      end

      def enqueue(token:, part:, **rest)
        Kraps.enqueuer.call(
          @step.worker,
          JSON.generate(
            job_index: @job_index,
            step_index: @step_index,
            frame: @frame.to_h,
            token: token,
            part: part,
            klass: @klass,
            args: @args,
            kwargs: @kwargs,
            **rest
          )
        )
      end

      def with_distributed_job
        distributed_job = Kraps.distributed_job_client.build(token: SecureRandom.hex)

        yield(distributed_job)
      rescue Interrupt
        distributed_job&.stop
        raise
      end

      def push_and_wait(distributed_job, enum)
        progress_bar = build_progress_bar("#{@klass}: job #{@job_index + 1}/#{@jobs.size}, step #{@step_index + 1}/#{@job.steps.size}, token #{distributed_job.token}, %a, %c/%C (%p%) => #{@step.action}")

        begin
          total = 0

          interval = Interval.new(1) do
            progress_bar.total = total
          end

          distributed_job.push_each(enum) do |item, part|
            total += 1
            interval.fire(timeout: 1)

            yield(item, part)
          end
        ensure
          interval&.stop
        end

        loop do
          progress_bar.total = distributed_job.total
          progress_bar.progress = progress_bar.total - distributed_job.count

          break if distributed_job.finished? || distributed_job.stopped?

          sleep(1)
        end

        raise(JobStopped, "The job was stopped") if distributed_job.stopped?
      ensure
        progress_bar&.stop
      end

      def build_progress_bar(format)
        options = { format: format, total: 1, autofinish: false }
        options[:output] = ProgressBar::Outputs::Null unless Kraps.show_progress?

        ProgressBar.create(options)
      end
    end
  end
end
