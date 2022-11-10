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

        job = job.parallelize(partitions: 8) {}
        job = job.map(&block)

        expect(job.steps).to match(
          [
            an_object_having_attributes(action: Actions::PARALLELIZE),
            an_object_having_attributes(
              action: Actions::MAP,
              partitions: 8,
              partitioner: kind_of(HashPartitioner),
              worker: TestJobWorker1,
              before: nil,
              block: block
            )
          ]
        )
      end

      it "respects the passed partitions, partitioner, worker and before" do
        block = ->(key, value, collector) { collector.call(key, value) }
        partitioner = ->(key) { key }
        before = -> {}

        job = described_class.new(worker: TestJobWorker1)
        job = job.parallelize(partitions: 8) {}
        job = job.map(partitions: 16, partitioner: partitioner, worker: TestJobWorker2, before: before, &block)

        expect(job.steps).to match(
          [
            an_object_having_attributes(action: Actions::PARALLELIZE),
            an_object_having_attributes(
              action: Actions::MAP,
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
        block = ->(_key, value1, value2) { value1 + value2 }

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

      it "respects the passed before" do
        block = ->(_key, value1, value2) { value1 + value2 }
        before = -> {}

        job = described_class.new(worker: TestJobWorker1)
        job = job.parallelize(partitions: 8) {}
        job = job.map { |key, value, collector| collector.call(key, value) }
        job = job.reduce(before: before, &block)

        expect(job.steps).to match(
          [
            an_object_having_attributes(action: Actions::PARALLELIZE),
            an_object_having_attributes(action: Actions::MAP),
            an_object_having_attributes(
              action: Actions::REDUCE,
              partitions: 8,
              partitioner: kind_of(HashPartitioner),
              worker: TestJobWorker1,
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

      it "respects the passed partitioner, worker and before" do
        partitioner = ->(key) { key }
        before = -> {}

        job = described_class.new(worker: TestJobWorker1)
        job = job.parallelize(partitions: 8) {}
        job = job.repartition(partitions: 16, partitioner: partitioner, worker: TestJobWorker2, before: before)

        expect(job.steps).to match(
          [
            an_object_having_attributes(action: Actions::PARALLELIZE),
            an_object_having_attributes(
              action: Actions::MAP,
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

      it "respects the passed worker and before" do
        before = -> {}

        job = described_class.new(worker: TestJobWorker1)
        job = job.parallelize(partitions: 8) {}
        job = job.each_partition(worker: TestJobWorker2, before: before) {}

        expect(job.steps).to match(
          [
            an_object_having_attributes(action: Actions::PARALLELIZE),
            an_object_having_attributes(
              action: Actions::EACH_PARTITION,
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
  end
end
