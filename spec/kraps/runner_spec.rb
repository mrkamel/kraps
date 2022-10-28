class TestRunnerWorker
  def self.perform_async(json)
    Kraps::Worker.new(json, memory_limit: 128 * 1024 * 1024, chunk_limit: 64, concurrency: 8).call
  end
end

class TestRunner; end

module Kraps
  RSpec.describe Runner do
    describe "#call" do
      it "requests the job spec, iterates and runs the jobs and steps" do
        store = {}

        TestRunner.define_method(:call) do
          job = Kraps::Job.new(worker: TestRunnerWorker)

          job = job.parallelize(partitions: 8) do |collector|
            ("key1".."key9").each { |item| collector.call(item) }
          end

          job = job.map do |key, _, collector|
            3.times do
              collector.call(key, key.gsub(/key/, "").to_i)
            end
          end

          job = job.reduce do |_, value1, value2|
            value1 + value2
          end

          job = job.each_partition do |_, pairs|
            pairs.each do |key, value|
              store[key] = value
            end
          end

          job
        end

        described_class.new(TestRunner).call

        expect(store).to eq("key1" => 3, "key2" => 6, "key3" => 9, "key4" => 12, "key5" => 15, "key6" => 18, "key7" => 21, "key8" => 24, "key9" => 27)
      end

      it "accepts positional and keyword arguments" do
        store = {}

        TestRunner.define_method(:call) do |multiplier, divisor:|
          job = Kraps::Job.new(worker: TestRunnerWorker)

          job = job.parallelize(partitions: 8) do |collector|
            ("key1".."key9").each { |item| collector.call(item) }
          end

          job = job.map do |key, _, collector|
            3.times do
              collector.call(key, key.gsub(/key/, "").to_i * multiplier)
            end
          end

          job = job.reduce do |_, value1, value2|
            value1 + value2
          end

          job = job.each_partition do |_, pairs|
            pairs.each do |key, value|
              store[key] = value / divisor
            end
          end

          job
        end

        described_class.new(TestRunner).call(2, divisor: 1.5)

        expect(store).to eq("key1" => 4, "key2" => 8, "key3" => 12, "key4" => 16, "key5" => 20, "key6" => 24, "key7" => 28, "key8" => 32, "key9" => 36)
      end

      it "runs all jobs returned by the call method" do
        store1 = {}
        store2 = {}

        TestRunner.define_method(:call) do |multiplier1:, multiplier2:|
          job = Kraps::Job.new(worker: TestRunnerWorker)

          job = job.parallelize(partitions: 8) do |collector|
            ("key1".."key9").each { |item| collector.call(item) }
          end

          job = job.map do |key, _, collector|
            3.times do
              collector.call(key, key.gsub(/key/, "").to_i)
            end
          end

          job = job.reduce do |_, value1, value2|
            value1 + value2
          end

          job1 = job.each_partition do |_, pairs|
            pairs.each do |key, value|
              store1[key] = value * multiplier1
            end
          end

          job2 = job.each_partition do |_, pairs|
            pairs.each do |key, value|
              store2[key] = value * multiplier2
            end
          end

          [job1, job2]
        end

        described_class.new(TestRunner).call(multiplier1: 2, multiplier2: 3)

        expect(store1).to eq("key1" => 6, "key2" => 12, "key3" => 18, "key4" => 24, "key5" => 30, "key6" => 36, "key7" => 42, "key8" => 48, "key9" => 54)
        expect(store2).to eq("key1" => 9, "key2" => 18, "key3" => 27, "key4" => 36, "key5" => 45, "key6" => 54, "key7" => 63, "key8" => 72, "key9" => 81)
      end

      it "does not run the same step multiple times" do
        parallelize_calls = 0
        map_calls = 0
        reduce_calls = 0

        TestRunner.define_method(:call) do
          job = Kraps::Job.new(worker: TestRunnerWorker)

          job = job.parallelize(partitions: 8) do |collector|
            parallelize_calls += 1

            collector.call("key")
          end

          job = job.map do |key, _, collector|
            map_calls += 1

            collector.call(key, 1)
            collector.call(key, 1)
          end

          job = job.reduce do |_key, value1, value2|
            reduce_calls += 1

            value1 + value2
          end

          job1 = job.each_partition {}
          job2 = job.each_partition {}

          [job1, job2]
        end

        described_class.new(TestRunner).call

        expect(parallelize_calls).to eq(1)
        expect(map_calls).to eq(1)
        expect(reduce_calls).to eq(1)
      end

      it "enqueues the worker jobs using the configured enqueuer" do
        enqueuer = double
        allow(enqueuer).to receive(:call)

        Kraps.configure(driver: FakeDriver, redis: RedisConnection, enqueuer: enqueuer)

        allow(SecureRandom).to receive(:hex).and_return("token1", "token2", "token3")

        TestRunner.define_method(:call) do
          job = Kraps::Job.new(worker: TestRunnerWorker)

          job = job.parallelize(partitions: 4) do |collector|
            ["item1", "item2"].each { |item| collector.call(item) }
          end

          job.map { |key, _, collector| collector.call(key, 1) }
             .reduce { |_key, value1, value2| value1 + value2 }
        end

        allow_any_instance_of(DistributedJob::Job).to receive(:finished?).and_return(true)

        described_class.new(TestRunner).call

        expect(enqueuer).to have_received(:call)
          .with(TestRunnerWorker, JSON.generate(job_index: 0, step_index: 0, frame: {}, token: "token1", part: "0", klass: "TestRunner", args: [], kwargs: {}, item: "item1"))
          .with(TestRunnerWorker, JSON.generate(job_index: 0, step_index: 0, frame: {}, token: "token1", part: "1", klass: "TestRunner", args: [], kwargs: {}, item: "item2"))
          .with(TestRunnerWorker, JSON.generate(job_index: 0, step_index: 1, frame: { token: "token1", partitions: 4 }, token: "token2", part: "0", klass: "TestRunner", args: [], kwargs: {}, partition: 0))
          .with(TestRunnerWorker, JSON.generate(job_index: 0, step_index: 1, frame: { token: "token1", partitions: 4 }, token: "token2", part: "1", klass: "TestRunner", args: [], kwargs: {}, partition: 1))
          .with(TestRunnerWorker, JSON.generate(job_index: 0, step_index: 1, frame: { token: "token1", partitions: 4 }, token: "token2", part: "2", klass: "TestRunner", args: [], kwargs: {}, partition: 2))
          .with(TestRunnerWorker, JSON.generate(job_index: 0, step_index: 1, frame: { token: "token1", partitions: 4 }, token: "token2", part: "3", klass: "TestRunner", args: [], kwargs: {}, partition: 3))
          .with(TestRunnerWorker, JSON.generate(job_index: 0, step_index: 2, frame: { token: "token2", partitions: 4 }, token: "token3", part: "0", klass: "TestRunner", args: [], kwargs: {}, partition: 0))
          .with(TestRunnerWorker, JSON.generate(job_index: 0, step_index: 2, frame: { token: "token2", partitions: 4 }, token: "token3", part: "1", klass: "TestRunner", args: [], kwargs: {}, partition: 1))
          .with(TestRunnerWorker, JSON.generate(job_index: 0, step_index: 2, frame: { token: "token2", partitions: 4 }, token: "token3", part: "2", klass: "TestRunner", args: [], kwargs: {}, partition: 2))
          .with(TestRunnerWorker, JSON.generate(job_index: 0, step_index: 2, frame: { token: "token2", partitions: 4 }, token: "token3", part: "3", klass: "TestRunner", args: [], kwargs: {}, partition: 3))
      end

      it "stops and raises a JobStopped error when a distributed job was stopped" do
        allow_any_instance_of(DistributedJob::Job).to receive(:stopped?).and_return(true)

        TestRunner.define_method(:call) do
          job = Kraps::Job.new(worker: TestRunnerWorker)

          job = job.parallelize(partitions: 8) do |collector|
            ("key1".."key9").each { |item| collector.call(item) }
          end

          job = job.map {}
          job
        end

        expect { described_class.new(TestRunner).call }.to raise_error(JobStopped, "The job was stopped")
      end

      it "stops the distributed job when an interrupt exception is raised" do
        distributed_job = Kraps.distributed_job_client.build(token: SecureRandom.hex)
        allow(Kraps.distributed_job_client).to receive(:build).and_return(distributed_job)
        allow(ProgressBar).to receive(:create).and_raise(Interrupt)

        TestRunner.define_method(:call) do
          Kraps::Job.new(worker: TestRunnerWorker).parallelize(partitions: 8) { |collector| collector.call("key") }
        end

        expect { described_class.new(TestRunner).call }.to raise_error(Interrupt)
        expect(distributed_job.stopped?).to eq(true)
      end

      it "shows a progress bar" do
        allow(ProgressBar).to receive(:create).and_call_original

        TestRunner.define_method(:call) do
          Kraps::Job.new(worker: TestRunnerWorker).parallelize(partitions: 8) { |collector| collector.call("item") }
        end

        described_class.new(TestRunner).call

        expect(ProgressBar).to have_received(:create)
      end

      it "does not show the progress when disabled" do
        Kraps.configure(driver: FakeDriver, redis: RedisConnection, show_progress: false)

        allow(ProgressBar).to receive(:create).and_call_original

        TestRunner.define_method(:call) do
          Kraps::Job.new(worker: TestRunnerWorker).parallelize(partitions: 8) { |collector| collector.call("item") }
        end

        described_class.new(TestRunner).call

        expect(ProgressBar).to have_received(:create).with(hash_including(output: ProgressBar::Outputs::Null))
      end
    end
  end
end
