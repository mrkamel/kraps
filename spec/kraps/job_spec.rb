class TestJobWorker1; end
class TestJobWorker2; end

module Kraps
  RSpec.describe Job do
    describe "#parallelize" do
      it "adds a corresponding step" do
        block = proc do |collector|
          [1, 2, 3].each { |item| collector.call(item) }
        end

        job = described_class.new(worker: TestJobWorker1)
        job = job.parallelize(partitions: 8, &block)

        expect(job.steps).to match(
          [
            an_object_having_attributes(
              action: Actions::PARALLELIZE,
              partitions: 8,
              partitioner: kind_of(HashPartitioner),
              worker: TestJobWorker1,
              before: nil,
              block: block
            )
          ]
        )
      end

      it "respects the passed partitioner, worker and before" do
        block = proc do |collector|
          [1, 2, 3].each { |item| collector.call(item) }
        end

        partitioner = ->(key) { key }
        before = -> {}

        job = described_class.new(worker: TestJobWorker1)
        job = job.parallelize(partitions: 16, partitioner: partitioner, worker: TestJobWorker2, before: before, &block)

        expect(job.steps).to match(
          [
            an_object_having_attributes(
              action: Actions::PARALLELIZE,
              partitions: 16,
              partitioner: partitioner,
              worker: TestJobWorker2,
              before: before,
              block: block
            )
          ]
        )
      end
    end

    describe "#map" do
      it "adds a corresponding step" do
        block = ->(key, value, collector) { collector.call(key, value) }

        job = described_class.new(worker: TestJobWorker1)

        job = job.parallelize(partitions: 4) {}
        job = job.map(partitions: 8, &block)

        expect(job.steps).to match(
          [
            an_object_having_attributes(action: Actions::PARALLELIZE),
            an_object_having_attributes(
              action: Actions::MAP,
              partitions: 8,
              jobs: 4,
              partitioner: kind_of(HashPartitioner),
              worker: TestJobWorker1,
              before: nil,
              block: block
            )
          ]
        )
      end

      it "respects the passed jobs, partitions, partitioner, worker and before" do
        block = ->(key, value, collector) { collector.call(key, value) }
        partitioner = ->(key) { key }
        before = -> {}

        job = described_class.new(worker: TestJobWorker1)
        job = job.parallelize(partitions: 8) {}
        job = job.map(jobs: 4, partitions: 16, partitioner: partitioner, worker: TestJobWorker2, before: before, &block)

        expect(job.steps).to match(
          [
            an_object_having_attributes(action: Actions::PARALLELIZE),
            an_object_having_attributes(
              action: Actions::MAP,
              jobs: 4,
              partitions: 16,
              partitioner: partitioner,
              worker: TestJobWorker2,
              before: before,
              block: block
            )
          ]
        )
      end
    end

    describe "#map_partitions" do
      it "adds a corresponding step" do
        block = -> {}

        job = described_class.new(worker: TestJobWorker1)

        job = job.parallelize(partitions: 4) {}
        job = job.map_partitions(partitions: 8, &block)

        expect(job.steps).to match(
          [
            an_object_having_attributes(action: Actions::PARALLELIZE),
            an_object_having_attributes(
              action: Actions::MAP_PARTITIONS,
              partitions: 8,
              jobs: 4,
              partitioner: kind_of(HashPartitioner),
              worker: TestJobWorker1,
              before: nil,
              block: block
            )
          ]
        )
      end

      it "respects the passed jobs, partitions, partitioner, worker and before" do
        block = -> {}
        partitioner = ->(key) { key }
        before = -> {}

        job = described_class.new(worker: TestJobWorker1)
        job = job.parallelize(partitions: 8) {}
        job = job.map_partitions(jobs: 4, partitions: 16, partitioner: partitioner, worker: TestJobWorker2, before: before, &block)

        expect(job.steps).to match(
          [
            an_object_having_attributes(action: Actions::PARALLELIZE),
            an_object_having_attributes(
              action: Actions::MAP_PARTITIONS,
              jobs: 4,
              partitions: 16,
              partitioner: partitioner,
              worker: TestJobWorker2,
              before: before,
              block: block
            )
          ]
        )
      end
    end

    describe "#reduce" do
      it "adds a corresponding step" do
        block = -> {}

        job = described_class.new(worker: TestJobWorker1)
        job = job.parallelize(partitions: 8) {}
        job = job.map { |key, value, collector| collector.call(key, value) }
        job = job.reduce(&block)

        expect(job.steps).to match(
          [
            an_object_having_attributes(action: Actions::PARALLELIZE),
            an_object_having_attributes(action: Actions::MAP),
            an_object_having_attributes(
              action: Actions::REDUCE,
              partitions: 8,
              partitioner: kind_of(HashPartitioner),
              worker: TestJobWorker1,
              before: nil,
              block: block
            )
          ]
        )
      end

      it "respects the passed jobs, worker and before" do
        block = -> {}
        before = -> {}

        job = described_class.new(worker: TestJobWorker1)
        job = job.parallelize(partitions: 8) {}
        job = job.map { |key, value, collector| collector.call(key, value) }
        job = job.reduce(jobs: 4, before: before, worker: TestJobWorker2, &block)

        expect(job.steps).to match(
          [
            an_object_having_attributes(action: Actions::PARALLELIZE),
            an_object_having_attributes(action: Actions::MAP),
            an_object_having_attributes(
              action: Actions::REDUCE,
              jobs: 4,
              partitions: 8,
              partitioner: kind_of(HashPartitioner),
              worker: TestJobWorker2,
              before: before,
              block: block
            )
          ]
        )
      end
    end

    describe "#append" do
      it "appends the corresponding step" do
        job1 = described_class.new(worker: TestJobWorker1)
        job1 = described_class.new(worker: TestJobWorker1)
        job1 = job1.parallelize(partitions: 8) do |collector|
          collector.call("key1", 1)
          collector.call("key2", 2)
          collector.call("key3", 3)
        end
        job1 = job1.map do |key, value, collector|
          collector.call(key, value + 1)
        end

        job2 = described_class.new(worker: TestJobWorker1)
        job2 = job2.parallelize(partitions: 8) do |collector|
          collector.call("key1", 3)
          collector.call("key2", 2)
          collector.call("key3", 1)
        end
        job2 = job2.append(job1)

        expect(job2.steps).to match(
          [
            an_object_having_attributes(action: Actions::PARALLELIZE),
            an_object_having_attributes(
              action: Actions::APPEND,
              partitions: 8,
              partitioner: kind_of(HashPartitioner),
              worker: TestJobWorker1,
              before: nil,
              block: nil,
              dependency: job1,
              options: { append_step_index: 1 }
            )
          ]
        )
      end

      it "respects the passed jobs, worker and before" do
        block = -> {}
        before = -> {}

        job1 = described_class.new(worker: TestJobWorker1)
        job1 = job1.parallelize(partitions: 8) do |collector|
          collector.call("key1", 1)
          collector.call("key2", 2)
          collector.call("key3", 3)
        end
        job1 = job1.map do |key, value, collector|
          collector.call(key, value + 1)
        end

        job2 = described_class.new(worker: TestJobWorker1)
        job2 = job2.parallelize(partitions: 8) do |collector|
          collector.call("key1", 3)
          collector.call("key2", 2)
          collector.call("key3", 1)
        end
        job2 = job2.append(job1, jobs: 4, worker: TestJobWorker2, before: before)

        expect(job2.steps).to match(
          [
            an_object_having_attributes(action: Actions::PARALLELIZE),
            an_object_having_attributes(
              action: Actions::APPEND,
              jobs: 4,
              partitions: 8,
              partitioner: kind_of(HashPartitioner),
              worker: TestJobWorker2,
              before: before,
              block: nil
            )
          ]
        )
      end
    end

    describe "#combine" do
      it "adds a corresponding step" do
        block = -> {}

        job1 = described_class.new(worker: TestJobWorker1)
        job1 = job1.parallelize(partitions: 8) do |collector|
          collector.call("key1", 1)
          collector.call("key2", 2)
          collector.call("key3", 3)
        end
        job1 = job1.map do |key, value, collector|
          collector.call(key, value + 1)
        end

        job2 = described_class.new(worker: TestJobWorker1)
        job2 = job2.parallelize(partitions: 8) do |collector|
          collector.call("key1", 3)
          collector.call("key2", 2)
          collector.call("key3", 1)
        end

        job3 = job2.combine(job1, &block)

        expect(job3.steps).to match(
          [
            an_object_having_attributes(action: Actions::PARALLELIZE),
            an_object_having_attributes(
              action: Actions::COMBINE,
              partitions: 8,
              partitioner: kind_of(HashPartitioner),
              worker: TestJobWorker1,
              before: nil,
              block: block,
              dependency: job1,
              options: { combine_step_index: 1 }
            )
          ]
        )
      end

      it "respects the passed jobs, worker and before" do
        block = -> {}
        before = -> {}

        job1 = described_class.new(worker: TestJobWorker1)
        job1 = job1.parallelize(partitions: 8) do |collector|
          collector.call("key1", 1)
          collector.call("key2", 2)
          collector.call("key3", 3)
        end
        job1 = job1.map do |key, value, collector|
          collector.call(key, value + 1)
        end

        job2 = described_class.new(worker: TestJobWorker1)
        job2 = job2.parallelize(partitions: 8) do |collector|
          collector.call("key1", 3)
          collector.call("key2", 2)
          collector.call("key3", 1)
        end
        job2 = job2.combine(job1, jobs: 4, worker: TestJobWorker2, before: before, &block)

        expect(job2.steps).to match(
          [
            an_object_having_attributes(action: Actions::PARALLELIZE),
            an_object_having_attributes(
              action: Actions::COMBINE,
              jobs: 4,
              partitions: 8,
              partitioner: kind_of(HashPartitioner),
              worker: TestJobWorker2,
              before: before,
              block: block
            )
          ]
        )
      end
    end

    describe "#repartition" do
      it "adds a corresponding map step" do
        job = described_class.new(worker: TestJobWorker1)
        job = job.parallelize(partitions: 8) {}
        job = job.repartition(partitions: 16)

        expect(job.steps).to match(
          [
            an_object_having_attributes(action: Actions::PARALLELIZE),
            an_object_having_attributes(
              action: Actions::MAP,
              partitions: 16,
              partitioner: kind_of(HashPartitioner),
              worker: TestJobWorker1,
              before: nil,
              block: kind_of(Proc)
            )
          ]
        )

        collected = []
        collector = ->(key, value) { collected.push([key, value]) }

        job.steps.last.block.call("key", "value", collector)

        expect(collected).to eq([["key", "value"]])
      end

      it "respects the passed jobs, partitioner, worker and before" do
        partitioner = ->(key) { key }
        before = -> {}

        job = described_class.new(worker: TestJobWorker1)
        job = job.parallelize(partitions: 8) {}
        job = job.repartition(jobs: 4, partitions: 16, partitioner: partitioner, worker: TestJobWorker2, before: before)

        expect(job.steps).to match(
          [
            an_object_having_attributes(action: Actions::PARALLELIZE),
            an_object_having_attributes(
              action: Actions::MAP,
              jobs: 4,
              partitions: 16,
              partitioner: partitioner,
              worker: TestJobWorker2,
              before: before,
              block: kind_of(Proc)
            )
          ]
        )
      end
    end

    describe "#each_partition" do
      it "adds a corresponding map step" do
        job = described_class.new(worker: TestJobWorker1)
        job = job.parallelize(partitions: 8) {}
        job = job.each_partition {}

        expect(job.steps).to match(
          [
            an_object_having_attributes(action: Actions::PARALLELIZE),
            an_object_having_attributes(
              action: Actions::EACH_PARTITION,
              partitions: 8,
              partitioner: kind_of(HashPartitioner),
              worker: TestJobWorker1,
              before: nil,
              block: kind_of(Proc)
            )
          ]
        )
      end

      it "respects the passed jobs, worker and before" do
        before = -> {}

        job = described_class.new(worker: TestJobWorker1)
        job = job.parallelize(partitions: 8) {}
        job = job.each_partition(jobs: 4, worker: TestJobWorker2, before: before) {}

        expect(job.steps).to match(
          [
            an_object_having_attributes(action: Actions::PARALLELIZE),
            an_object_having_attributes(
              action: Actions::EACH_PARTITION,
              jobs: 4,
              partitions: 8,
              partitioner: kind_of(HashPartitioner),
              worker: TestJobWorker2,
              before: before,
              block: kind_of(Proc)
            )
          ]
        )
      end
    end

    describe "#dump" do
      it "adds a corresponding each partition step" do
        job = described_class.new(worker: TestJobWorker1)
        job = job.parallelize(partitions: 8) {}
        job = job.dump(prefix: "path/to/destination")

        expect(job.steps).to match(
          [
            an_object_having_attributes(action: Actions::PARALLELIZE),
            an_object_having_attributes(
              action: Actions::EACH_PARTITION,
              partitions: 8,
              partitioner: kind_of(HashPartitioner),
              worker: TestJobWorker1,
              before: nil,
              block: kind_of(Proc)
            )
          ]
        )
      end

      it "respects the passed worker" do
        job = described_class.new(worker: TestJobWorker1)
        job = job.parallelize(partitions: 8) {}
        job = job.dump(prefix: "path/to/destination", worker: TestJobWorker2)

        expect(job.steps).to match(
          [
            an_object_having_attributes(action: Actions::PARALLELIZE),
            an_object_having_attributes(
              action: Actions::EACH_PARTITION,
              partitions: 8,
              partitioner: kind_of(HashPartitioner),
              worker: TestJobWorker2,
              before: nil,
              block: kind_of(Proc)
            )
          ]
        )
      end
    end

    describe "#load" do
      it "adds a corresponding parallelize and map partitions step" do
        partitioner = ->(key) { key }

        job = described_class.new(worker: TestJobWorker1)
        job = job.load(prefix: "path/to/destination", partitions: 8, partitioner: partitioner, concurrency: 8)

        expect(job.steps).to match(
          [
            an_object_having_attributes(
              action: Actions::PARALLELIZE,
              partitions: 8,
              partitioner: kind_of(Proc),
              worker: TestJobWorker1,
              before: nil,
              block: kind_of(Proc)
            ),
            an_object_having_attributes(
              action: Actions::MAP_PARTITIONS,
              partitions: 8,
              partitioner: partitioner,
              worker: TestJobWorker1,
              before: nil,
              block: kind_of(Proc)
            )
          ]
        )
      end

      it "respects the passed worker" do
        partitioner = ->(key) { key }

        job = described_class.new(worker: TestJobWorker1)
        job = job.load(prefix: "path/to/destination", partitions: 8, partitioner: partitioner, worker: TestJobWorker2, concurrency: 8)

        expect(job.steps).to match(
          [
            an_object_having_attributes(
              action: Actions::PARALLELIZE,
              jobs: nil,
              partitions: 8,
              partitioner: kind_of(Proc),
              worker: TestJobWorker2,
              before: nil,
              block: kind_of(Proc)
            ),
            an_object_having_attributes(
              action: Actions::MAP_PARTITIONS,
              jobs: 8,
              partitions: 8,
              partitioner: partitioner,
              worker: TestJobWorker2,
              before: nil,
              block: kind_of(Proc)
            )
          ]
        )
      end
    end
  end
end
