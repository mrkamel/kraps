module Kraps
  class Runner
    def initialize(klass)
      @klass = klass
    end

    def call(*args, **kwargs)
      @args = args
      @kwargs = kwargs

      Array(@klass.new.call(*@args, **@kwargs)).each_with_index do |job, job_index|
        job.steps.each_with_index.inject(nil) do |frame, (step, step_index)|
          raise(InvalidAction, "Invalid action #{step.action}") unless Actions::ALL.include?(step.action)

          step.frame ||= send(:"perform_#{step.action}", job_index: job_index, step_index: step_index, frame: frame, step: step, **step.args, &step.block)
        end
      end
    end

    private

    def perform_parallelize(job_index:, step_index:, frame:, step:, partitions:, **_rest, &block)
      enum = block.call

      distributed_job = Kraps.distributed_job_client.build(token: SecureRandom.hex)

      distributed_job.push_each(enum) do |item, part|
        enqueue(
          job_index: job_index,
          step_index: step_index,
          frame: frame,
          step: step,
          action: Actions::PARALLELIZE,
          token: distributed_job.token,
          part: part,
          partitions: partitions,
          item: item
        )
      end

      wait(distributed_job, name: "parallelize", job_index: job_index, step_index: step_index)

      Frame.new(token: distributed_job.token, partitions: partitions)
    end

    def perform_map(job_index:, step_index:, frame:, step:, partitions: frame.partitions, **_rest)
      distributed_job = Kraps.distributed_job_client.build(token: SecureRandom.hex)

      distributed_job.push_each(0...frame.partitions) do |partition, part|
        enqueue(
          job_index: job_index,
          step_index: step_index,
          frame: frame,
          step: step,
          action: Actions::MAP,
          token: distributed_job.token,
          part: part,
          partitions: partitions,
          partition: partition
        )
      end

      wait(distributed_job, name: "map", job_index: job_index, step_index: step_index)

      Frame.new(token: distributed_job.token, partitions: partitions)
    end

    def perform_reduce(job_index:, step_index:, frame:, step:, partitions: frame.partitions, **_rest)
      distributed_job = Kraps.distributed_job_client.build(token: SecureRandom.hex)

      distributed_job.push_each(0...frame.partitions) do |partition, part|
        enqueue(
          job_index: job_index,
          step_index: step_index,
          frame: frame,
          step: step,
          action: Actions::REDUCE,
          token: distributed_job.token,
          part: part,
          partitions: partitions,
          partition: partition
        )
      end

      wait(distributed_job, name: "reduce", job_index: job_index, step_index: step_index)

      Frame.new(token: distributed_job.token, partitions: partitions)
    end

    def perform_each_partition(job_index:, step_index:, frame:, step:, partitions: frame.partitions, **_rest)
      distributed_job = Kraps.distributed_job_client.build(token: SecureRandom.hex)

      distributed_job.push_each(0...frame.partitions) do |partition, part|
        enqueue(
          job_index: job_index,
          step_index: step_index,
          frame: frame,
          step: step,
          action: Actions::EACH_PARTITION,
          token: distributed_job.token,
          part: part,
          partitions: partitions,
          partition: partition
        )
      end

      wait(distributed_job, name: "each_partition", job_index: job_index, step_index: step_index)

      frame
    end

    def enqueue(job_index:, step_index:, frame:, step:, action:, token:, part:, **rest)
      Kraps.enqueuer.call(
        step.args[:worker],
        JSON.generate(
          job_index: job_index,
          step_index: step_index,
          frame: frame.to_h,
          action: action,
          token: token,
          part: part,
          klass: @klass,
          args: @args,
          kwargs: @kwargs,
          **rest
        )
      )
    end

    def wait(distributed_job, name:, job_index:, step_index:)
      format = "#{name}, job #{job_index + 1}, step #{step_index + 1}, token #{distributed_job.token}: %a %c/%C (%p%)"
      progress_bar = Kraps.show_progress? ? ProgressBar.create(format: format) : ProgressBar.create(format: format, output: ProgressBar::Outputs::Null)

      loop do
        total = distributed_job.total

        progress_bar.total = total
        progress_bar.progress = [total, total - distributed_job.count].min

        break if distributed_job.finished? || distributed_job.stopped?

        sleep 5
      end

      raise(JobStopped, "The job was stopped") if distributed_job.stopped?
    rescue Interrupt
      distributed_job&.stop
      raise
    ensure
      progress_bar&.stop
    end
  end
end
