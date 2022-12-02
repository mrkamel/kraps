class TestWorker; end

module Kraps
  RSpec.describe Worker do
    def build_worker(args:, memory_limit: 128 * 1024 * 1024, chunk_limit: 32, concurrency: 8, **rest)
      described_class.new(JSON.generate(args), memory_limit: memory_limit, chunk_limit: chunk_limit, concurrency: concurrency, **rest)
    end

    let(:redis_queue) { RedisQueue.new(token: "token", redis: Kraps.redis, namespace: Kraps.namespace, ttl: 60) }

    it "executes the specified before block" do
      before_called = false
      before = -> { before_called = true }

      TestWorker.define_method(:call) do
        Job.new(worker: TestWorker).parallelize(partitions: 8, before: before) {}
      end

      redis_queue.enqueue(item: "item1")

      build_worker(
        args: {
          token: redis_queue.token,
          part: "0",
          action: Actions::PARALLELIZE,
          klass: "TestWorker",
          args: [],
          kwargs: {},
          job_index: 0,
          step_index: 0
        }
      ).call

      expect(before_called).to eq(true)
    end

    it "executes the specified parallelize action" do
      TestWorker.define_method(:call) do
        Job.new(worker: TestWorker).parallelize(partitions: 8) do |collector|
          ["item1", "item2", "item3"].each { |item| collector.call(item) }
        end
      end

      redis_queue.enqueue(item: "item1", part: 0)

      build_worker(
        args: {
          token: redis_queue.token,
          part: "0",
          action: Actions::PARALLELIZE,
          klass: "TestWorker",
          args: [],
          kwargs: {},
          job_index: 0,
          step_index: 0
        }
      ).call

      expect(Kraps.driver.list.to_a).to eq(["prefix/token/7/chunk.0.json"])
      expect(Kraps.driver.value("prefix/token/7/chunk.0.json").strip).to eq(JSON.generate(["item1", nil]))
    end

    it "executes the specified map action" do
      TestWorker.define_method(:call) do
        Job.new(worker: TestWorker)
           .parallelize(partitions: 4) {}
           .map { |key, _, collector| collector.call(key, 1) }
           .map(jobs: 2) do |key, value, collector|
             collector.call(key + "a", value + 1)
             collector.call(key + "b", value + 1)
           end
      end

      chunk1 = [
        JSON.generate(["item1", 1]),
        JSON.generate(["item2", 1])
      ].join("\n")

      chunk2 = [
        JSON.generate(["item2", 1]),
        JSON.generate(["item3", 1])
      ].join("\n")

      chunk3 = [
        JSON.generate(["item3", 1]),
        JSON.generate(["item4", 1])
      ].join("\n")

      Kraps.driver.store("prefix/previous_token/0/chunk.0.json", chunk1)
      Kraps.driver.store("prefix/previous_token/0/chunk.1.json", chunk2)
      Kraps.driver.store("prefix/previous_token/2/chunk.0.json", chunk3)

      redis_queue.enqueue(partition: 0)
      redis_queue.enqueue(partition: 1)
      redis_queue.enqueue(partition: 2)
      redis_queue.enqueue(partition: 3)

      build_worker(
        args: {
          token: redis_queue.token,
          action: Actions::MAP,
          frame: { token: "previous_token", partitions: 4 },
          klass: "TestWorker",
          args: [],
          kwargs: {},
          job_index: 0,
          step_index: 2
        }
      ).call

      expect(Kraps.driver.list.to_a).to eq(
        [
          "prefix/previous_token/0/chunk.0.json",
          "prefix/previous_token/0/chunk.1.json",
          "prefix/previous_token/2/chunk.0.json",
          "prefix/token/0/chunk.0.json",
          "prefix/token/1/chunk.0.json",
          "prefix/token/1/chunk.2.json",
          "prefix/token/2/chunk.2.json",
          "prefix/token/3/chunk.0.json",
          "prefix/token/3/chunk.2.json"
        ]
      )
      expect(Kraps.driver.value("prefix/token/0/chunk.0.json").strip).to eq(
        [JSON.generate(["item1a", 2]), JSON.generate(["item1b", 2])].join("\n")
      )
      expect(Kraps.driver.value("prefix/token/1/chunk.0.json").strip).to eq(
        [JSON.generate(["item2a", 2]), JSON.generate(["item2a", 2]), JSON.generate(["item3a", 2])].join("\n")
      )
      expect(Kraps.driver.value("prefix/token/1/chunk.2.json").strip).to eq(
        [JSON.generate(["item3a", 2])].join("\n")
      )
      expect(Kraps.driver.value("prefix/token/2/chunk.2.json").strip).to eq(
        [JSON.generate(["item4b", 2])].join("\n")
      )
      expect(Kraps.driver.value("prefix/token/3/chunk.0.json").strip).to eq(
        [JSON.generate(["item2b", 2]), JSON.generate(["item2b", 2]), JSON.generate(["item3b", 2])].join("\n")
      )
      expect(Kraps.driver.value("prefix/token/3/chunk.2.json").strip).to eq(
        [JSON.generate(["item3b", 2]), JSON.generate(["item4a", 2])].join("\n")
      )
    end

    it "executes the specified map action and reduces already when the next step is a reduce step" do
      TestWorker.define_method(:call) do
        job = Job.new(worker: TestWorker).parallelize(partitions: 4) {}

        job = job.map do |key, _, collector|
          collector.call(key + "a", 1)
          collector.call(key + "b", 1)
          collector.call(key + "a", 1)
          collector.call(key + "b", 1)
          collector.call(key + "c", 1)
        end

        job.reduce do |_, value1, value2|
          value1 + value2
        end
      end

      chunk1 = [
        JSON.generate(["item1", nil]),
        JSON.generate(["item2", nil])
      ].join("\n")

      chunk2 = [
        JSON.generate(["item3", nil])
      ].join("\n")

      Kraps.driver.store("prefix/previous_token/0/chunk.0.json", chunk1)
      Kraps.driver.store("prefix/previous_token/0/chunk.1.json", chunk2)

      redis_queue.enqueue(partition: 0)
      redis_queue.enqueue(partition: 1)
      redis_queue.enqueue(partition: 2)
      redis_queue.enqueue(partition: 3)

      build_worker(
        args: {
          token: redis_queue.token,
          action: Actions::MAP,
          frame: { token: "previous_token", partitions: 4 },
          klass: "TestWorker",
          args: [],
          kwargs: {},
          job_index: 0,
          step_index: 1
        }
      ).call

      expect(Kraps.driver.list.to_a).to eq(
        [
          "prefix/previous_token/0/chunk.0.json",
          "prefix/previous_token/0/chunk.1.json",
          "prefix/token/0/chunk.0.json",
          "prefix/token/1/chunk.0.json",
          "prefix/token/3/chunk.0.json"
        ]
      )
      expect(Kraps.driver.value("prefix/token/0/chunk.0.json").strip).to eq(
        [JSON.generate(["item1a", 2]), JSON.generate(["item1b", 2]), JSON.generate(["item3c", 1])].join("\n")
      )
      expect(Kraps.driver.value("prefix/token/1/chunk.0.json").strip).to eq(
        [JSON.generate(["item1c", 1]), JSON.generate(["item2a", 2]), JSON.generate(["item2c", 1]), JSON.generate(["item3a", 2])].join("\n")
      )
      expect(Kraps.driver.value("prefix/token/3/chunk.0.json").strip).to eq(
        [JSON.generate(["item2b", 2]), JSON.generate(["item3b", 2])].join("\n")
      )
    end

    it "executes the specified map partitions action" do
      partitions = []

      TestWorker.define_method(:call) do
        Job.new(worker: TestWorker)
           .parallelize(partitions: 4) {}
           .map { |key, _, collector| collector.call(key, 1) }
           .map_partitions(jobs: 2) do |partition, pairs, collector|
             partitions << [partition, pairs.to_a]

             pairs.each do |key, value|
               collector.call(key + "x", value + 1)
             end
           end
      end

      chunk1 = [
        JSON.generate(["item1", 1]),
        JSON.generate(["item1", 1]),
        JSON.generate(["item2", 1]),
        JSON.generate(["item3", 1])
      ].join("\n")

      chunk2 = [
        JSON.generate(["item2", 1]),
        JSON.generate(["item3", 1]),
        JSON.generate(["item4", 1])
      ].join("\n")

      chunk3 = [
        JSON.generate(["item3", 1]),
        JSON.generate(["item4", 1]),
        JSON.generate(["item5", 1])
      ].join("\n")

      Kraps.driver.store("prefix/previous_token/0/chunk.0.json", chunk1)
      Kraps.driver.store("prefix/previous_token/0/chunk.1.json", chunk2)
      Kraps.driver.store("prefix/previous_token/2/chunk.0.json", chunk3)

      redis_queue.enqueue(partition: 0)
      redis_queue.enqueue(partition: 1)
      redis_queue.enqueue(partition: 2)
      redis_queue.enqueue(partition: 3)

      build_worker(
        args: {
          token: redis_queue.token,
          action: Actions::MAP_PARTITIONS,
          frame: { token: "previous_token", partitions: 4 },
          klass: "TestWorker",
          args: [],
          kwargs: {},
          job_index: 0,
          step_index: 2
        }
      ).call

      expect(partitions).to eq(
        [
          [0, [["item1", 1], ["item1", 1], ["item2", 1], ["item2", 1], ["item3", 1], ["item3", 1], ["item4", 1]]],
          [1, []],
          [2, [["item3", 1], ["item4", 1], ["item5", 1]]],
          [3, []]
        ]
      )

      expect(Kraps.driver.list.to_a).to eq(
        [
          "prefix/previous_token/0/chunk.0.json",
          "prefix/previous_token/0/chunk.1.json",
          "prefix/previous_token/2/chunk.0.json",
          "prefix/token/0/chunk.0.json",
          "prefix/token/0/chunk.2.json",
          "prefix/token/2/chunk.0.json",
          "prefix/token/2/chunk.2.json",
          "prefix/token/3/chunk.0.json",
          "prefix/token/3/chunk.2.json"
        ]
      )
      expect(Kraps.driver.value("prefix/token/0/chunk.0.json").strip).to eq(
        [JSON.generate(["item4x", 2])].join("\n")
      )
      expect(Kraps.driver.value("prefix/token/0/chunk.2.json").strip).to eq(
        [JSON.generate(["item4x", 2])].join("\n")
      )
      expect(Kraps.driver.value("prefix/token/2/chunk.0.json").strip).to eq(
        [JSON.generate(["item2x", 2]), JSON.generate(["item2x", 2])].join("\n")
      )
      expect(Kraps.driver.value("prefix/token/2/chunk.2.json").strip).to eq(
        [JSON.generate(["item5x", 2])].join("\n")
      )
      expect(Kraps.driver.value("prefix/token/3/chunk.0.json").strip).to eq(
        [JSON.generate(["item1x", 2]), JSON.generate(["item1x", 2]), JSON.generate(["item3x", 2]), JSON.generate(["item3x", 2])].join("\n")
      )
      expect(Kraps.driver.value("prefix/token/3/chunk.2.json").strip).to eq(
        [JSON.generate(["item3x", 2])].join("\n")
      )
    end

    it "executes the specified map partitions action and reduces already when the next step is a reduce step" do
      TestWorker.define_method(:call) do
        job = Job.new(worker: TestWorker).parallelize(partitions: 4) {}

        job = job.map_partitions do |_, pairs, collector|
          pairs.each do |key, _|
            collector.call(key + "a", 1)
            collector.call(key + "b", 1)
            collector.call(key + "a", 1)
            collector.call(key + "b", 1)
            collector.call(key + "c", 1)
          end
        end

        job.reduce do |_, value1, value2|
          value1 + value2
        end
      end

      chunk1 = [
        JSON.generate(["item1", nil]),
        JSON.generate(["item2", nil])
      ].join("\n")

      chunk2 = [
        JSON.generate(["item3", nil])
      ].join("\n")

      Kraps.driver.store("prefix/previous_token/0/chunk.0.json", chunk1)
      Kraps.driver.store("prefix/previous_token/0/chunk.1.json", chunk2)

      redis_queue.enqueue(partition: 0)
      redis_queue.enqueue(partition: 1)
      redis_queue.enqueue(partition: 2)
      redis_queue.enqueue(partition: 3)

      build_worker(
        args: {
          token: redis_queue.token,
          action: Actions::MAP,
          frame: { token: "previous_token", partitions: 4 },
          klass: "TestWorker",
          args: [],
          kwargs: {},
          job_index: 0,
          step_index: 1
        }
      ).call

      expect(Kraps.driver.list.to_a).to eq(
        ["prefix/previous_token/0/chunk.0.json", "prefix/previous_token/0/chunk.1.json", "prefix/token/0/chunk.0.json", "prefix/token/1/chunk.0.json", "prefix/token/3/chunk.0.json"]
      )
      expect(Kraps.driver.value("prefix/token/0/chunk.0.json").strip).to eq(
        [JSON.generate(["item1a", 2]), JSON.generate(["item1b", 2]), JSON.generate(["item3c", 1])].join("\n")
      )
      expect(Kraps.driver.value("prefix/token/1/chunk.0.json").strip).to eq(
        [JSON.generate(["item1c", 1]), JSON.generate(["item2a", 2]), JSON.generate(["item2c", 1]), JSON.generate(["item3a", 2])].join("\n")
      )
      expect(Kraps.driver.value("prefix/token/3/chunk.0.json").strip).to eq(
        [JSON.generate(["item2b", 2]), JSON.generate(["item3b", 2])].join("\n")
      )
    end

    it "executes the specified reduce action" do
      TestWorker.define_method(:call) do
        Job.new(worker: TestWorker)
           .parallelize(partitions: 4) {}
           .map {}
           .reduce(jobs: 2) { |_, value1, value2| value1 + value2 }
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

      chunk3 = [
        JSON.generate(["item3", 1]),
        JSON.generate(["item4", 2]),
        JSON.generate(["item5", 3])
      ].join("\n")

      Kraps.driver.store("prefix/previous_token/0/chunk.0.json", chunk1)
      Kraps.driver.store("prefix/previous_token/0/chunk.1.json", chunk2)
      Kraps.driver.store("prefix/previous_token/2/chunk.0.json", chunk3)

      redis_queue.enqueue(partition: 0)
      redis_queue.enqueue(partition: 1)
      redis_queue.enqueue(partition: 2)
      redis_queue.enqueue(partition: 3)

      build_worker(
        args: {
          token: redis_queue.token,
          action: Actions::REDUCE,
          frame: { token: "previous_token", partitions: 4 },
          klass: "TestWorker",
          args: [],
          kwargs: {},
          job_index: 0,
          step_index: 2
        }
      ).call

      expect(Kraps.driver.list.to_a).to eq(
        [
          "prefix/previous_token/0/chunk.0.json",
          "prefix/previous_token/0/chunk.1.json",
          "prefix/previous_token/2/chunk.0.json",
          "prefix/token/0/chunk.0.json",
          "prefix/token/1/chunk.1.json",
          "prefix/token/2/chunk.2.json",
          "prefix/token/3/chunk.3.json"
        ]
      )
      expect(Kraps.driver.value("prefix/token/0/chunk.0.json").strip).to eq(
        [JSON.generate(["item1", 3]), JSON.generate(["item2", 4]), JSON.generate(["item3", 6]), JSON.generate(["item4", 2])].join("\n")
      )
      expect(Kraps.driver.value("prefix/token/1/chunk.1.json").strip).to eq(
        [].join("\n")
      )
      expect(Kraps.driver.value("prefix/token/2/chunk.2.json").strip).to eq(
        [JSON.generate(["item3", 1]), JSON.generate(["item4", 2]), JSON.generate(["item5", 3])].join("\n")
      )
      expect(Kraps.driver.value("prefix/token/3/chunk.3.json").strip).to eq(
        [].join("\n")
      )
    end

    it "executes the specified each partition action" do
      partitions = []

      TestWorker.define_method(:call) do
        Job.new(worker: TestWorker)
           .parallelize(partitions: 4) {}
           .each_partition(jobs: 2) do |partition, pairs|
             partitions << [partition, pairs.to_a]
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

      chunk3 = [
        JSON.generate(["item3", 1]),
        JSON.generate(["item4", 2]),
        JSON.generate(["item5", 3])
      ].join("\n")

      Kraps.driver.store("prefix/previous_token/0/chunk.0.json", chunk1)
      Kraps.driver.store("prefix/previous_token/0/chunk.1.json", chunk2)
      Kraps.driver.store("prefix/previous_token/2/chunk.0.json", chunk3)

      redis_queue.enqueue(partition: 0)
      redis_queue.enqueue(partition: 1)
      redis_queue.enqueue(partition: 2)
      redis_queue.enqueue(partition: 3)

      build_worker(
        args: {
          token: redis_queue.token,
          action: Actions::EACH_PARTITION,
          frame: { token: "previous_token", partitions: 4 },
          klass: "TestWorker",
          args: [],
          kwargs: {},
          job_index: 0,
          step_index: 1
        }
      ).call

      expect(partitions).to eq(
        [
          [0, [["item1", 1], ["item1", 2], ["item2", 1], ["item2", 3], ["item3", 2], ["item3", 4], ["item4", 2]]],
          [1, []],
          [2, [["item3", 1], ["item4", 2], ["item5", 3]]],
          [3, []]
        ]
      )
    end

    it "executes the specified combine action" do
      TestWorker.define_method(:call) do
        job1 = Job.new(worker: TestWorker).parallelize(partitions: 4) {}
        job1 = job1.map {}

        job2 = Job.new(worker: TestWorker).parallelize(partitions: 4) {}
        job2 = job2.map {}

        job2.combine(job1, jobs: 2) do |_, value1, value2|
          (value1 || 0) + (value2 || 0)
        end
      end

      chunk1 = [
        JSON.generate(["key1", 1]),
        JSON.generate(["key2", 2])
      ].join("\n")

      chunk2 = [
        JSON.generate(["key3", 3]),
        JSON.generate(["key4", 3]),
        JSON.generate(["key5", 5])
      ].join("\n")

      chunk3 = [
        JSON.generate(["key4", 4]),
        JSON.generate(["key5", 5]),
        JSON.generate(["key6", 6])
      ].join("\n")

      Kraps.driver.store("prefix/combine_token/0/chunk.0.json", chunk1)
      Kraps.driver.store("prefix/combine_token/0/chunk.1.json", chunk2)
      Kraps.driver.store("prefix/combine_token/2/chunk.0.json", chunk3)

      chunk4 = [
        JSON.generate(["key0", 1]),
        JSON.generate(["key1", 1]),
        JSON.generate(["key1", 1]),
        JSON.generate(["key3", 1])
      ].join("\n")

      chunk5 = [
        JSON.generate(["key3", 1]),
        JSON.generate(["key5", 2]),
        JSON.generate(["key6", 3]),
        JSON.generate(["key7", 4])
      ].join("\n")

      chunk6 = [
        JSON.generate(["key4", 3]),
        JSON.generate(["key6", 4]),
        JSON.generate(["key6", 5]),
        JSON.generate(["key7", 6]),
        JSON.generate(["key8", 7])
      ].join("\n")

      Kraps.driver.store("prefix/previous_token/0/chunk.0.json", chunk4)
      Kraps.driver.store("prefix/previous_token/0/chunk.1.json", chunk5)
      Kraps.driver.store("prefix/previous_token/2/chunk.0.json", chunk6)

      redis_queue.enqueue(partition: 0, combine_frame: { token: "combine_token", partitions: 4 })
      redis_queue.enqueue(partition: 1, combine_frame: { token: "combine_token", partitions: 4 })
      redis_queue.enqueue(partition: 2, combine_frame: { token: "combine_token", partitions: 4 })
      redis_queue.enqueue(partition: 3, combine_frame: { token: "combine_token", partitions: 4 })

      build_worker(
        args: {
          token: redis_queue.token,
          action: Actions::COMBINE,
          frame: { token: "previous_token", partitions: 4 },
          klass: "TestWorker",
          args: [],
          kwargs: {},
          job_index: 1,
          step_index: 2
        }
      ).call(retries: 0)

      expect(Kraps.driver.list.to_a).to eq(
        [
          "prefix/combine_token/0/chunk.0.json",
          "prefix/combine_token/0/chunk.1.json",
          "prefix/combine_token/2/chunk.0.json",
          "prefix/previous_token/0/chunk.0.json",
          "prefix/previous_token/0/chunk.1.json",
          "prefix/previous_token/2/chunk.0.json",
          "prefix/token/0/chunk.0.json",
          "prefix/token/1/chunk.0.json",
          "prefix/token/1/chunk.2.json",
          "prefix/token/2/chunk.0.json",
          "prefix/token/2/chunk.2.json",
          "prefix/token/3/chunk.0.json",
          "prefix/token/3/chunk.2.json"
        ]
      )
      expect(Kraps.driver.value("prefix/token/0/chunk.0.json").strip).to eq(
        [JSON.generate(["key5", 7])].join("\n")
      )
      expect(Kraps.driver.value("prefix/token/1/chunk.0.json").strip).to eq(
        [JSON.generate(["key0", 1]), JSON.generate(["key1", 2]), JSON.generate(["key1", 2])].join("\n")
      )
      expect(Kraps.driver.value("prefix/token/1/chunk.2.json").strip).to eq(
        [JSON.generate(["key4", 7])].join("\n")
      )
      expect(Kraps.driver.value("prefix/token/2/chunk.0.json").strip).to eq(
        [JSON.generate(["key3", 4]), JSON.generate(["key3", 4]), JSON.generate(["key7", 4])].join("\n")
      )
      expect(Kraps.driver.value("prefix/token/2/chunk.2.json").strip).to eq(
        [JSON.generate(["key7", 6])].join("\n")
      )
      expect(Kraps.driver.value("prefix/token/3/chunk.0.json").strip).to eq(
        [JSON.generate(["key6", 3])].join("\n")
      )
      expect(Kraps.driver.value("prefix/token/3/chunk.2.json").strip).to eq(
        [JSON.generate(["key6", 10]), JSON.generate(["key6", 11]), JSON.generate(["key8", 7])].join("\n")
      )
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
          token: redis_queue.token,
          part: "0",
          action: Actions::PARALLELIZE,
          klass: "TestWorker",
          args: ["arg1", "arg2"],
          kwargs: { "kwarg1" => "value1", "kwarg2" => "value2" },
          job_index: 0,
          step_index: 0
        }
      ).call

      expect(passed_args).to eq(["arg1", "arg2"])
      expect(passed_kwargs).to eq(kwarg1: "value1", kwarg2: "value2")
    end

    it "passes the right number of partitions to the partitioner" do
      TestWorker.define_method(:call) do
        Job.new(worker: TestWorker)
           .parallelize(partitions: 4, partitioner: proc { |key, num_partitions| [key, num_partitions] }) {}
           .map(partitions: 8) { raise("error") }
      end

      worker = build_worker(
        args: {
          token: redis_queue.token,
          part: "0",
          action: Actions::MAP,
          frame: { token: "previous_token", partitions: 4 },
          klass: "TestWorker",
          args: [],
          kwargs: {},
          job_index: 0,
          step_index: 1
        }
      )

      expect(worker.send(:partitioner).call("key")).to eq(["key", 8])
    end

    it "retries for the specified amount of times and logs errors" do
      logger = double
      allow(logger).to receive(:error)

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

      Kraps.driver.store("prefix/previous_token/0/chunk.0.json", chunk)

      redis_queue.enqueue(partition: 0)
      redis_queue.enqueue(partition: 1)
      redis_queue.enqueue(partition: 2)
      redis_queue.enqueue(partition: 3)

      worker = build_worker(
        args: {
          token: redis_queue.token,
          action: Actions::MAP,
          frame: { token: "previous_token", partitions: 4 },
          klass: "TestWorker",
          args: [],
          kwargs: {},
          job_index: 0,
          step_index: 1
        },
        logger: logger
      )

      allow(worker).to receive(:sleep)

      expect { worker.call(retries: 5) }.to raise_error("error")

      expect(worker).to have_received(:sleep).with(5).exactly(5).times
      expect(logger).to have_received(:error).with(RuntimeError).exactly(5).times
      expect(redis_queue.stopped?).to eq(true)
    end

    it "does not stop until the queue is empty or stopped" do
      TestWorker.define_method(:call) do
        Job.new(worker: TestWorker)
           .parallelize(partitions: 4) {}
           .map {}
      end

      redis_queue.enqueue(partition: 0)
      redis_queue.enqueue(partition: 1)

      worker = build_worker(
        args: {
          token: redis_queue.token,
          action: Actions::MAP,
          frame: { token: "previous_token", partitions: 4 },
          klass: "TestWorker",
          args: [],
          kwargs: {},
          job_index: 0,
          step_index: 1
        }
      )

      allow(worker).to receive(:sleep)

      thread = Thread.new do
        redis_queue.dequeue do
          sleep 1
        end
      end

      worker.call(retries: 5)

      expect(thread).not_to be_alive
    end

    it "respects the specified memory limit in parallelize" do
      TestWorker.define_method(:call) do
        Job.new(worker: TestWorker).parallelize(partitions: 4) {}
      end

      allow(MapReduce::Mapper).to receive(:new).and_call_original

      redis_queue.enqueue(partition: 0)

      build_worker(
        args: {
          token: redis_queue.token,
          part: "0",
          action: Actions::PARALLELIZE,
          klass: "TestWorker",
          args: [],
          kwargs: {},
          job_index: 0,
          step_index: 0
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

      Kraps.driver.store("prefix/previous_token/0/chunk.0.json", chunk)

      redis_queue.enqueue(partition: 0)

      build_worker(
        args: {
          token: redis_queue.token,
          action: Actions::MAP,
          frame: { token: "previous_token", partitions: 4 },
          klass: "TestWorker",
          args: [],
          kwargs: {},
          job_index: 0,
          step_index: 1
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

      Kraps.driver.store("prefix/previous_token/0/chunk.0.json", chunk)

      reducer = MapReduce::Reducer.new(double)
      allow(reducer).to receive(:reduce)
      allow(MapReduce::Reducer).to receive(:new).and_return(reducer)

      redis_queue.enqueue(partition: 0)

      build_worker(
        args: {
          token: redis_queue.token,
          action: Actions::REDUCE,
          frame: { token: "previous_token", partitions: 4 },
          klass: "TestWorker",
          args: [],
          kwargs: {},
          job_index: 0,
          step_index: 2
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

      Kraps.driver.store("prefix/previous_token/0/chunk.0.json", chunk)

      redis_queue.enqueue(partition: 0)

      build_worker(
        args: {
          token: redis_queue.token,
          action: Actions::MAP,
          frame: { token: "previous_token", partitions: 4 },
          klass: "TestWorker",
          args: [],
          kwargs: {},
          job_index: 0,
          step_index: 1
        },
        concurrency: 4
      ).call

      expect(Parallelizer).to have_received(:each).with(anything, 4).exactly(2).times
    end

    it "stops the redis queue on continous errors" do
      TestWorker.define_method(:call) do
        Job.new(worker: TestWorker)
           .parallelize(partitions: 4) {}
           .map { raise("error") }
      end

      allow(Parallelizer).to receive(:each).and_call_original

      chunk = [
        JSON.generate(["item1", nil]),
        JSON.generate(["item2", nil]),
        JSON.generate(["item3", nil])
      ].join("\n")

      Kraps.driver.store("prefix/previous_token/0/chunk.0.json", chunk)

      redis_queue.enqueue(partition: 0)
      redis_queue.enqueue(partition: 1)
      redis_queue.enqueue(partition: 2)
      redis_queue.enqueue(partition: 3)

      expect do
        build_worker(
          args: {
            token: redis_queue.token,
            action: Actions::MAP,
            frame: { token: "previous_token", partitions: 4 },
            klass: "TestWorker",
            args: [],
            kwargs: {},
            job_index: 0,
            step_index: 1
          },
          concurrency: 4
        ).call(retries: 0)
      end.to raise_error("error")
    end

    it "retries for the specified number of times" do
      retries = 0

      TestWorker.define_method(:call) do
        Job.new(worker: TestWorker)
           .parallelize(partitions: 4) {}
           .map do
             retries += 1

             raise("error") if retries < 3
           end
      end

      allow(Parallelizer).to receive(:each).and_call_original

      chunk = [
        JSON.generate(["item1", nil]),
        JSON.generate(["item2", nil]),
        JSON.generate(["item3", nil])
      ].join("\n")

      Kraps.driver.store("prefix/previous_token/0/chunk.0.json", chunk)

      expect do
        worker = build_worker(
          args: {
            token: redis_queue.token,
            part: "0",
            action: Actions::MAP,
            frame: { token: "previous_token", partitions: 4 },
            klass: "TestWorker",
            args: [],
            kwargs: {},
            job_index: 0,
            step_index: 1
          },
          concurrency: 4
        )

        allow(worker).to receive(:sleep)

        worker.call(retries: 3)
      end.not_to raise_error
    end
  end
end
