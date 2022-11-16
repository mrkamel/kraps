module Kraps
  RSpec.describe JobResolver do
    describe "#call" do
      it "resolves the dependencies of steps correctly" do
        job1 = Kraps::Job.new(worker: TestRunnerWorker)
        job1 = job1.parallelize(partitions: 8) {}

        job2 = Kraps::Job.new(worker: TestRunnerWorker)
        job2 = job2.parallelize(partitions: 8) {}
        job2 = job2.combine(job1) {}

        job3 = Kraps::Job.new(worker: TestRunnerWorker)
        job3 = job3.parallelize(partitions: 8) {}
        job3 = job3.combine(job2) {}

        job4 = Kraps::Job.new(worker: TestRunnerWorker)
        job4 = job4.parallelize(partitions: 8) {}
        job4 = job4.combine(job1) {}

        resolved_jobs = described_class.new.call([job3, job4])

        expect(resolved_jobs).to eq([job1, job2, job3, job4])
      end
    end
  end
end
