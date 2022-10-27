module Kraps
  class Runner
    def initialize(klass)
      @klass = klass
    end

    def call(*args, **kwargs)
      Array(@klass.new.call(*args, **kwargs)).tap do |jobs|
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

          Frame.new(token: distributed_job.token, partitions: @step.args[:partitions])
        end
      end

      def perform_map
        with_distributed_job do |distributed_job|
          push_and_wait(distributed_job, 0...@frame.partitions) do |partition, part|
            enqueue(token: distributed_job.token, part: part, partition: partition)
          end

          Frame.new(token: distributed_job.token, partitions: @step.args[:partitions])
        end
      end

      def perform_reduce
        with_distributed_job do |distributed_job|
          push_and_wait(distributed_job, 0...@frame.partitions) do |partition, part|
            enqueue(token: distributed_job.token, part: part, partition: partition)
          end

          Frame.new(token: distributed_job.token, partitions: @step.args[:partitions])
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
          @step.args[:worker],
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

      def push_and_wait(distributed_job, enum, &block)
        push_each(distributed_job, enum, &block)
        wait(distributed_job)
      end

      def push_each(distributed_job, enum)
        progress_bar = build_progress_bar("#{@klass}, job #{@job_index + 1}/#{@jobs.size}, step #{@step_index + 1}/#{@job.steps.size}, #{@step.action}/enqueue, token #{distributed_job.token}, %a %c")

        distributed_job.push_each(enum) do |item, part|
          progress_bar.total = progress_bar.progress + 2 # Always keep the progress bar going until manually stopped
          progress_bar.progress = progress_bar.progress + 1

          yield(item, part)
        end
      ensure
        progress_bar&.stop
      end

      def wait(distributed_job)
        progress_bar = build_progress_bar("#{@klass}, job #{@job_index + 1}/#{@jobs.size}, step #{@step_index + 1}/#{@job.steps.size}, #{@step.action}/process, token #{distributed_job.token}, %a %c/%C (%p%)")

        loop do
          total = distributed_job.total

          progress_bar.total = total
          progress_bar.progress = [total, total - distributed_job.count].min

          break if distributed_job.finished? || distributed_job.stopped?

          sleep(1)
        end

        raise(JobStopped, "The job was stopped") if distributed_job.stopped?
      ensure
        progress_bar&.stop
      end

      def build_progress_bar(format)
        Kraps.show_progress? ? ProgressBar.create(format: format) : ProgressBar.create(format: format, output: ProgressBar::Outputs::Null)
      end
    end
  end
end
