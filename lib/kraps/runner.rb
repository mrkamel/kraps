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
      enum = Enumerator.new do |yielder|
        collector = proc { |item| yielder << item }

        block.call(collector)
      end

      distributed_job = Kraps.distributed_job_client.build(token: SecureRandom.hex)

      push_and_wait(distributed_job, enum, name: "parallelize", job_index: job_index, step_index: step_index) do |item, part|
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

      Frame.new(token: distributed_job.token, partitions: partitions)
    end

    def perform_map(job_index:, step_index:, frame:, step:, partitions: frame.partitions, **_rest)
      distributed_job = Kraps.distributed_job_client.build(token: SecureRandom.hex)

      push_and_wait(distributed_job, 0...frame.partitions, name: "map", job_index: job_index, step_index: step_index) do |partition, part|
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

      Frame.new(token: distributed_job.token, partitions: partitions)
    end

    def perform_reduce(job_index:, step_index:, frame:, step:, partitions: frame.partitions, **_rest)
      distributed_job = Kraps.distributed_job_client.build(token: SecureRandom.hex)

      push_and_wait(distributed_job, 0...frame.partitions, name: "reduce", job_index: job_index, step_index: step_index) do |partition, part|
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

      Frame.new(token: distributed_job.token, partitions: partitions)
    end

    def perform_each_partition(job_index:, step_index:, frame:, step:, partitions: frame.partitions, **_rest)
      distributed_job = Kraps.distributed_job_client.build(token: SecureRandom.hex)

      push_and_wait(distributed_job, 0...frame.partitions, name: "each_partition", job_index: job_index, step_index: step_index) do |partition, part|
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

    def push_and_wait(distributed_job, enum, name:, job_index:, step_index:, &block)
      push_each(distributed_job, enum, name: name, job_index: job_index, step_index: step_index, &block)
      wait(distributed_job, name: name, job_index: job_index, step_index: step_index)
    end

    def push_each(distributed_job, enum, name:, job_index:, step_index:)
      progress_bar = build_progress_bar("#{@klass}, job #{job_index + 1}, step #{step_index + 1}, #{name}/enqueue, token #{distributed_job.token}: %c")

      distributed_job.push_each(enum) do |item, part|
        progress_bar.total = progress_bar.progress + 2 # Always keep the progress bar going until manually stopped
        progress_bar.progress = progress_bar.progress + 1

        yield(item, part)
      end
    ensure
      progress_bar&.stop
    end

    def wait(distributed_job, name:, job_index:, step_index:)
      progress_bar = build_progress_bar("#{@klass}, job #{job_index + 1}, step #{step_index + 1}, #{name}/process, token #{distributed_job.token}: %a %c/%C (%p%)")

      loop do
        total = distributed_job.total

        progress_bar.total = total
        progress_bar.progress = [total, total - distributed_job.count].min

        break if distributed_job.finished? || distributed_job.stopped?

        sleep(1)
      end

      raise(JobStopped, "The job was stopped") if distributed_job.stopped?
    rescue Interrupt
      distributed_job&.stop
      raise
    ensure
      progress_bar&.stop
    end

    def build_progress_bar(format)
      progress_bar = Kraps.show_progress? ? ProgressBar.create(format: format) : ProgressBar.create(format: format, output: ProgressBar::Outputs::Null)
    end
  end
end
