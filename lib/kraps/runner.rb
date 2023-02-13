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
          collector = proc { |item| yielder << { item: item } }

          @step.block.call(collector)
        end

        token = push_and_wait(enum: enum)

        Frame.new(token: token, partitions: @step.partitions)
      end

      def perform_map
        enum = (0...@frame.partitions).map { |partition| { partition: partition } }
        token = push_and_wait(job_count: @step.jobs, enum: enum)

        Frame.new(token: token, partitions: @step.partitions)
      end

      def perform_map_partitions
        enum = (0...@frame.partitions).map { |partition| { partition: partition } }
        token = push_and_wait(job_count: @step.jobs, enum: enum)

        Frame.new(token: token, partitions: @step.partitions)
      end

      def perform_reduce
        enum = (0...@frame.partitions).map { |partition| { partition: partition } }
        token = push_and_wait(job_count: @step.jobs, enum: enum)

        Frame.new(token: token, partitions: @step.partitions)
      end

      def perform_combine
        combine_job = @step.dependency
        combine_step = combine_job.steps[@step.options[:combine_step_index]]

        raise(IncompatibleFrame, "Incompatible number of partitions") if combine_step.partitions != @step.partitions

        enum = (0...@frame.partitions).map do |partition|
          { partition: partition, combine_frame: combine_step.frame.to_h }
        end

        token = push_and_wait(job_count: @step.jobs, enum: enum)

        Frame.new(token: token, partitions: @step.partitions)
      end

      def perform_each_partition
        enum = (0...@frame.partitions).map { |partition| { partition: partition } }
        push_and_wait(job_count: @step.jobs, enum: enum)

        @frame
      end

      def push_and_wait(enum:, job_count: nil)
        redis_queue = RedisQueue.new(redis: Kraps.redis, token: SecureRandom.hex, namespace: Kraps.namespace, ttl: Kraps.job_ttl)
        progress_bar = build_progress_bar("#{@klass}: job #{@job_index + 1}/#{@jobs.size}, step #{@step_index + 1}/#{@job.steps.size}, #{@step.jobs || "?"} jobs, token #{redis_queue.token}, %a, %c/%C (%p%) => #{@step.action}")

        total = 0

        interval = Interval.new(1) do
          # The interval is used to continously update the progress bar even
          # when push_all is used and to avoid sessions being terminated due
          # to inactivity etc

          progress_bar.total = total
          progress_bar.progress = [progress_bar.total - redis_queue.size, 0].max
        end

        enum.each_with_index do |item, part|
          total += 1

          redis_queue.enqueue(item.merge(part: part))
        end

        (job_count || total).times do
          break if redis_queue.stopped?

          Kraps.enqueuer.call(@step.worker, JSON.generate(job_index: @job_index, step_index: @step_index, frame: @frame.to_h, token: redis_queue.token, klass: @klass, args: @args, kwargs: @kwargs))
        end

        loop do
          break if redis_queue.size.zero?
          break if redis_queue.stopped?

          sleep(1)
        end

        raise(JobStopped, "The job was stopped") if redis_queue.stopped?

        interval.fire(timeout: 1)

        redis_queue.token
      ensure
        redis_queue&.stop
        interval&.stop
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
