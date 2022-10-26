class TestJobWorker1; end
class TestJobWorker2; end

module Kraps
  RSpec.describe Job do
    describe "#parallelize" do
      it "adds a corresponding step" do
        block = -> { [1, 2, 3] }

        job = described_class.new(worker: TestJobWorker1)
        job = job.parallelize(partitions: 8, &block)

        expect(job.steps).to match(
          [an_object_having_attributes(action: Actions::PARALLELIZE, args: { partitions: 8, partitioner: kind_of(MapReduce::HashPartitioner), worker: TestJobWorker1 }, block: block)]
        )
      end

      it "respects the passed partitioner and worker" do
        block = -> { [1, 2, 3] }
        partitioner = ->(key) { key }

        job = described_class.new(worker: TestJobWorker1)
        job = job.parallelize(partitions: 16, partitioner: partitioner, worker: TestJobWorker2, &block)

        expect(job.steps).to match(
          [an_object_having_attributes(action: Actions::PARALLELIZE, args: { partitions: 16, partitioner: partitioner, worker: TestJobWorker2 }, block: block)]
        )
      end
    end

    describe "#map" do
      it "adds a corresponding step" do
        block = ->(key, value, collector) { collector.call(key, value) }

        job = described_class.new(worker: TestJobWorker1)
        job = job.parallelize(partitions: 8) { [1, 2, 3] }
        job = job.map(&block)

        expect(job.steps).to match(
          [
            an_object_having_attributes(action: Actions::PARALLELIZE),
            an_object_having_attributes(action: Actions::MAP, args: { partitions: 8, partitioner: kind_of(MapReduce::HashPartitioner), worker: TestJobWorker1 }, block: block)
          ]
        )
      end

      it "respects the passed partitions, partitioner and worker" do
        block = ->(key, value, collector) { collector.call(key, value) }
        partitioner = ->(key) { key }

        job = described_class.new(worker: TestJobWorker1)
        job = job.parallelize(partitions: 8) { [1, 2, 3] }
        job = job.map(partitions: 16, partitioner: partitioner, worker: TestJobWorker2, &block)

        expect(job.steps).to match(
          [
            an_object_having_attributes(action: Actions::PARALLELIZE),
            an_object_having_attributes(action: Actions::MAP, args: { partitions: 16, partitioner: partitioner, worker: TestJobWorker2 }, block: block)
          ]
        )
      end
    end

    describe "#reduce" do
      it "adds a corresponding step" do
        block = ->(_key, value1, value2) { value1 + value2 }

        job = described_class.new(worker: TestJobWorker1)
        job = job.parallelize(partitions: 8)
        job = job.map { |key, value, collector| collector.call(key, value) }
        job = job.reduce(&block)

        expect(job.steps).to match(
          [
            an_object_having_attributes(action: Actions::PARALLELIZE),
            an_object_having_attributes(action: Actions::MAP),
            an_object_having_attributes(action: Actions::REDUCE, args: { partitions: 8, partitioner: kind_of(MapReduce::HashPartitioner), worker: TestJobWorker1 }, block: block)
          ]
        )
      end
    end

    describe "#repartition" do
      it "adds a corresponding map step" do
        job = described_class.new(worker: TestJobWorker1)
        job = job.parallelize(partitions: 8) { [1, 2, 3] }
        job = job.repartition(partitions: 16)

        expect(job.steps).to match(
          [
            an_object_having_attributes(action: Actions::PARALLELIZE),
            an_object_having_attributes(action: Actions::MAP, args: { partitions: 16, partitioner: kind_of(MapReduce::HashPartitioner), worker: TestJobWorker1 }, block: kind_of(Proc))
          ]
        )

        collected = []
        collector = ->(key, value) { collected.push([key, value]) }

        job.steps.last.block.call("key", "value", collector)

        expect(collected).to eq([["key", "value"]])
      end

      it "respects the passed partitioner and worker" do
        partitioner = ->(key) { key }

        job = described_class.new(worker: TestJobWorker1)
        job = job.parallelize(partitions: 8) { [1, 2, 3] }
        job = job.repartition(partitions: 16, partitioner: partitioner, worker: TestJobWorker2)

        expect(job.steps).to match(
          [
            an_object_having_attributes(action: Actions::PARALLELIZE),
            an_object_having_attributes(action: Actions::MAP, args: { partitions: 16, partitioner: partitioner, worker: TestJobWorker2 }, block: kind_of(Proc))
          ]
        )
      end
    end

    describe "#each_partition" do
      it "adds a corresponding map step" do
        job = described_class.new(worker: TestJobWorker1)
        job = job.parallelize(partitions: 8) { [1, 2, 3] }
        job = job.each_partition {}

        expect(job.steps).to match(
          [
            an_object_having_attributes(action: Actions::PARALLELIZE),
            an_object_having_attributes(action: Actions::EACH_PARTITION, args: { partitions: 8, partitioner: kind_of(MapReduce::HashPartitioner), worker: TestJobWorker1 }, block: kind_of(Proc))
          ]
        )
      end

      it "respects the passed worker" do
        job = described_class.new(worker: TestJobWorker1)
        job = job.parallelize(partitions: 8) { [1, 2, 3] }
        job = job.each_partition(worker: TestJobWorker2) {}

        expect(job.steps).to match(
          [
            an_object_having_attributes(action: Actions::PARALLELIZE),
            an_object_having_attributes(action: Actions::EACH_PARTITION, args: { partitions: 8, partitioner: kind_of(MapReduce::HashPartitioner), worker: TestJobWorker2 }, block: kind_of(Proc))
          ]
        )
      end
    end
  end
end
