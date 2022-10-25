class TestWorker; end

module Kraps
  RSpec.describe Worker do
    def build_worker(args:, memory_limit: 128 * 1024 * 1024, chunk_limit: 32, concurrency: 8)
      described_class.new(JSON.generate(args), memory_limit: memory_limit, chunk_limit: chunk_limit, concurrency: concurrency)
    end

    let(:distributed_job) { Kraps.distributed_job_client.build(token: "token") }

    it "executes the specified parallelize action" do
      TestWorker.define_method(:call) do
        Job.new(worker: TestWorker).parallelize(partitions: 8) { ["item1", "item2", "item3"] }
      end

      build_worker(
        args: {
          token: distributed_job.token,
          part: "0",
          action: Actions::PARALLELIZE,
          klass: "TestWorker",
          args: [],
          kwargs: {},
          job_index: 0,
          step_index: 0,
          item: "item1"
        }
      ).call

      expect(Kraps.driver.driver.list(Kraps.driver.bucket).to_a).to eq(["prefix/token/7/chunk.0.json"])
      expect(Kraps.driver.driver.value("prefix/token/7/chunk.0.json", Kraps.driver.bucket).strip).to eq(JSON.generate(["item1", nil]))
    end

    it "executes the specified map action" do
      TestWorker.define_method(:call) do
        Job.new(worker: TestWorker)
           .parallelize(partitions: 4) {}
           .map do |key, _, collector|
             collector.call(key + "a", 1)
             collector.call(key + "b", 1)
             collector.call(key + "c", 1)
           end
      end

      chunk1 = [
        JSON.generate(["item1", nil]),
        JSON.generate(["item2", nil])
      ].join("\n")

      chunk2 = [
        JSON.generate(["item3", nil])
      ].join("\n")

      Kraps.driver.driver.store("prefix/previous_token/0/chunk.0.json", chunk1, Kraps.driver.bucket)
      Kraps.driver.driver.store("prefix/previous_token/0/chunk.1.json", chunk2, Kraps.driver.bucket)

      build_worker(
        args: {
          token: distributed_job.token,
          part: "0",
          action: Actions::MAP,
          frame: { token: "previous_token", partitions: 4 },
          klass: "TestWorker",
          args: [],
          kwargs: {},
          job_index: 0,
          step_index: 1,
          partition: 0
        }
      ).call

      expect(Kraps.driver.driver.list(Kraps.driver.bucket).to_a).to eq(
        ["prefix/previous_token/0/chunk.0.json", "prefix/previous_token/0/chunk.1.json", "prefix/token/0/chunk.0.json", "prefix/token/1/chunk.0.json", "prefix/token/3/chunk.0.json"]
      )
      expect(Kraps.driver.driver.value("prefix/token/0/chunk.0.json", Kraps.driver.bucket).strip).to eq(
        [JSON.generate(["item1a", 1]), JSON.generate(["item1b", 1]), JSON.generate(["item3c", 1])].join("\n")
      )
      expect(Kraps.driver.driver.value("prefix/token/1/chunk.0.json", Kraps.driver.bucket).strip).to eq(
        [JSON.generate(["item1c", 1]), JSON.generate(["item2a", 1]), JSON.generate(["item2c", 1]), JSON.generate(["item3a", 1])].join("\n")
      )
      expect(Kraps.driver.driver.value("prefix/token/3/chunk.0.json", Kraps.driver.bucket).strip).to eq(
        [JSON.generate(["item2b", 1]), JSON.generate(["item3b", 1])].join("\n")
      )
    end

    it "executes the specified reduce action" do
      TestWorker.define_method(:call) do
        Job.new(worker: TestWorker)
           .parallelize(partitions: 4) {}
           .map {}
           .reduce { |_, value1, value2| value1 + value2 }
      end

      chunk1 = [
        JSON.generate(["item1", 1]),
        JSON.generate(["item1", 2]),
        JSON.generate(["item2", 3]),
        JSON.generate(["item3", 4])
      ].join("\n")

      chunk2 = [
        JSON.generate(["item2", 1]),
        JSON.generate(["item3", 2]),
        JSON.generate(["item4", 2])
      ].join("\n")

      Kraps.driver.driver.store("prefix/previous_token/0/chunk.0.json", chunk1, Kraps.driver.bucket)
      Kraps.driver.driver.store("prefix/previous_token/0/chunk.1.json", chunk2, Kraps.driver.bucket)

      build_worker(
        args: {
          token: distributed_job.token,
          part: "0",
          action: Actions::REDUCE,
          frame: { token: "previous_token", partitions: 4 },
          klass: "TestWorker",
          args: [],
          kwargs: {},
          job_index: 0,
          step_index: 2,
          partition: 0
        }
      ).call

      expect(Kraps.driver.driver.list(Kraps.driver.bucket).to_a).to eq(
        ["prefix/previous_token/0/chunk.0.json", "prefix/previous_token/0/chunk.1.json", "prefix/token/0/chunk.0.json"]
      )
      expect(Kraps.driver.driver.value("prefix/token/0/chunk.0.json", Kraps.driver.bucket).strip).to eq(
        [JSON.generate(["item1", 3]), JSON.generate(["item2", 4]), JSON.generate(["item3", 6]), JSON.generate(["item4", 2])].join("\n")
      )
    end

    it "executes the specified each partition action" do
      store = {}

      TestWorker.define_method(:call) do
        Job.new(worker: TestWorker)
           .parallelize(partitions: 4) {}
           .each_partition do |partition|
             partition.each do |key, value|
               (store[key] ||= []) << value
             end
           end
      end

      chunk1 = [
        JSON.generate(["item1", 1]),
        JSON.generate(["item1", 2]),
        JSON.generate(["item2", 3]),
        JSON.generate(["item3", 4])
      ].join("\n")

      chunk2 = [
        JSON.generate(["item2", 1]),
        JSON.generate(["item3", 2]),
        JSON.generate(["item4", 2])
      ].join("\n")

      Kraps.driver.driver.store("prefix/previous_token/0/chunk.0.json", chunk1, Kraps.driver.bucket)
      Kraps.driver.driver.store("prefix/previous_token/0/chunk.1.json", chunk2, Kraps.driver.bucket)

      build_worker(
        args: {
          token: distributed_job.token,
          part: "0",
          action: Actions::EACH_PARTITION,
          frame: { token: "previous_token", partitions: 4 },
          klass: "TestWorker",
          args: [],
          kwargs: {},
          job_index: 0,
          step_index: 1,
          partition: 0
        }
      ).call

      expect(store).to eq("item1" => [1, 2], "item2" => [3, 1], "item3" => [4, 2], "item4" => [2])
    end

    it "passes the spcified args and kwargs" do
      passed_args = nil
      passed_kwargs = nil

      TestWorker.define_method(:call) do |*args, **kwargs|
        passed_args = args
        passed_kwargs = kwargs

        Job.new(worker: TestWorker).parallelize(partitions: 4) {}
      end

      build_worker(
        args: {
          token: distributed_job.token,
          part: "0",
          action: Actions::PARALLELIZE,
          klass: "TestWorker",
          args: ["arg1", "arg2"],
          kwargs: { "kwarg1" => "value1", "kwarg2" => "value2" },
          job_index: 0,
          step_index: 0,
          item: "item"
        }
      ).call

      expect(passed_args).to eq(["arg1", "arg2"])
      expect(passed_kwargs).to eq(kwarg1: "value1", kwarg2: "value2")
    end

    it "retries for the specified amount of times" do
      TestWorker.define_method(:call) do
        Job.new(worker: TestWorker)
           .parallelize(partitions: 4) {}
           .map { raise("error") }
      end

      chunk = [
        JSON.generate(["item1", nil]),
        JSON.generate(["item2", nil]),
        JSON.generate(["item3", nil])
      ].join("\n")

      Kraps.driver.driver.store("prefix/previous_token/0/chunk.0.json", chunk, Kraps.driver.bucket)

      worker = build_worker(
        args: {
          token: distributed_job.token,
          part: "0",
          action: Actions::MAP,
          frame: { token: "previous_token", partitions: 4 },
          klass: "TestWorker",
          args: [],
          kwargs: {},
          job_index: 0,
          step_index: 1,
          partition: 0
        }
      )

      allow(worker).to receive(:sleep)

      expect { worker.call(retries: 5) }.to raise_error("error")

      expect(worker).to have_received(:sleep).with(5).exactly(5).times
      expect(distributed_job.stopped?).to eq(true)
    end

    it "marks the distributed job part as done" do
      TestWorker.define_method(:call) do
        Job.new(worker: TestWorker).parallelize(partitions: 4) {}
      end

      distributed_job.push_all(["0", "1", "2", "3"])

      build_worker(
        args: {
          token: distributed_job.token,
          part: "0",
          action: Actions::PARALLELIZE,
          klass: "TestWorker",
          args: [],
          kwargs: {},
          job_index: 0,
          step_index: 0,
          item: "item1"
        }
      ).call

      expect(distributed_job.open_parts.to_a).to eq(["1", "2", "3"])
    end

    it "respects the specified memory limit in parallelize" do
      TestWorker.define_method(:call) do
        Job.new(worker: TestWorker).parallelize(partitions: 4) {}
      end

      allow(MapReduce::Mapper).to receive(:new).and_call_original

      build_worker(
        args: {
          token: distributed_job.token,
          part: "0",
          action: Actions::PARALLELIZE,
          klass: "TestWorker",
          args: [],
          kwargs: {},
          job_index: 0,
          step_index: 0,
          item: "item1"
        },
        memory_limit: 5000
      ).call

      expect(MapReduce::Mapper).to have_received(:new).with(anything, partitioner: anything, memory_limit: 5000)
    end

    it "respects the specified memory limit in map" do
      TestWorker.define_method(:call) do
        Job.new(worker: TestWorker)
           .parallelize(partitions: 4) {}
           .map {}
      end

      allow(MapReduce::Mapper).to receive(:new).and_call_original

      chunk = [
        JSON.generate(["item1", nil]),
        JSON.generate(["item2", nil]),
        JSON.generate(["item3", nil])
      ].join("\n")

      Kraps.driver.driver.store("prefix/previous_token/0/chunk.0.json", chunk, Kraps.driver.bucket)

      build_worker(
        args: {
          token: distributed_job.token,
          part: "0",
          action: Actions::MAP,
          frame: { token: "previous_token", partitions: 4 },
          klass: "TestWorker",
          args: [],
          kwargs: {},
          job_index: 0,
          step_index: 1,
          partition: 0
        },
        memory_limit: 5000
      ).call

      expect(MapReduce::Mapper).to have_received(:new).with(anything, partitioner: anything, memory_limit: 5000)
    end

    it "respects the specified chunk limit" do
      TestWorker.define_method(:call) do
        Job.new(worker: TestWorker)
           .parallelize(partitions: 4) {}
           .map {}
           .reduce { |_, value1, value2| value1 + value2 }
      end

      chunk = [
        JSON.generate(["item1", 1]),
        JSON.generate(["item2", 2]),
        JSON.generate(["item3", 3])
      ].join("\n")

      Kraps.driver.driver.store("prefix/previous_token/0/chunk.0.json", chunk, Kraps.driver.bucket)

      reducer = MapReduce::Reducer.new(double)
      allow(reducer).to receive(:reduce)
      allow(MapReduce::Reducer).to receive(:new).and_return(reducer)

      build_worker(
        args: {
          token: distributed_job.token,
          part: "0",
          action: Actions::REDUCE,
          frame: { token: "previous_token", partitions: 4 },
          klass: "TestWorker",
          args: [],
          kwargs: {},
          job_index: 0,
          step_index: 2,
          partition: 0
        },
        chunk_limit: 8
      ).call

      expect(reducer).to have_received(:reduce).with(chunk_limit: 8)
    end

    it "respects the specified concurrency" do
      TestWorker.define_method(:call) do
        Job.new(worker: TestWorker)
           .parallelize(partitions: 4) {}
           .map {}
      end

      allow(Parallelizer).to receive(:each).and_call_original

      chunk = [
        JSON.generate(["item1", nil]),
        JSON.generate(["item2", nil]),
        JSON.generate(["item3", nil])
      ].join("\n")

      Kraps.driver.driver.store("prefix/previous_token/0/chunk.0.json", chunk, Kraps.driver.bucket)

      build_worker(
        args: {
          token: distributed_job.token,
          part: "0",
          action: Actions::MAP,
          frame: { token: "previous_token", partitions: 4 },
          klass: "TestWorker",
          args: [],
          kwargs: {},
          job_index: 0,
          step_index: 1,
          partition: 0
        },
        concurrency: 4
      ).call

      expect(Parallelizer).to have_received(:each).with(anything, 4)
    end
  end
end
