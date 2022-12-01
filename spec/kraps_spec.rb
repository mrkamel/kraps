RSpec.describe Kraps do
  it "has a version number" do
    expect(Kraps::VERSION).not_to be_nil
  end

  describe ".configure" do
    around do |test|
      driver = described_class.driver
      redis = described_class.redis
      namespace = described_class.namespace
      job_ttl = described_class.job_ttl
      enqueuer = described_class.enqueuer

      test.run
    ensure
      described_class.configure(driver: driver, redis: redis, namespace: namespace, job_ttl: job_ttl, enqueuer: enqueuer)
    end

    it "sets driver, redis, namespace, job_ttl and enqueuer" do
      described_class.configure(driver: "driver", redis: "redis", namespace: "namespace", job_ttl: 1, enqueuer: "enqueuer")

      expect(described_class).to have_attributes(
        driver: "driver",
        redis: "redis",
        namespace: "namespace",
        job_ttl: 1,
        enqueuer: "enqueuer"
      )
    end

    it "applies #to_i on the job_ttl" do
      described_class.configure(driver: described_class.driver, job_ttl: "300")

      expect(described_class.job_ttl).to eq(300)
    end
  end
end
