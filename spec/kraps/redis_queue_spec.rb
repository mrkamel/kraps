module Kraps
  RSpec.describe RedisQueue do
    let(:redis) { Kraps.redis }

    def build_redis_queue(token: "token", namespace: nil, ttl: 60)
      RedisQueue.new(token: token, redis: redis, namespace: namespace, ttl: ttl)
    end

    describe "#enqueue" do
      it "enqueues the specified job" do
        redis_queue = build_redis_queue
        redis_queue.enqueue(key: "value1")
        redis_queue.enqueue(key: "value2")

        expect(redis.lrange("kraps:queue:token", 0, 10)).to eq([JSON.generate(key: "value1"), JSON.generate(key: "value2")])
      end

      it "respects the specified namespace" do
        redis_queue = build_redis_queue(namespace: "namespace")
        redis_queue.enqueue(key: "value")

        expect(redis.lrange("namespace:kraps:queue:token", 0, 10)).to eq([JSON.generate(key: "value")])
      end

      it "updates the expiry" do
        redis_queue = build_redis_queue(ttl: 30)
        redis_queue.enqueue(key: "value1")
        redis_queue.enqueue(key: "value2")
        redis_queue.stop

        redis_queue.dequeue do
          redis.persist("kraps:queue:token")
          redis.persist("kraps:pending:token")
          redis.persist("kraps:status:token")

          redis_queue.enqueue(key: "value3")

          expect(redis.ttl("kraps:queue:token")).to be_between(29, 30)
          expect(redis.ttl("kraps:pending:token")).to be_between(29, 30)
          expect(redis.ttl("kraps:status:token")).to be_between(29, 30)
        end
      end
    end

    describe "#size" do
      it "returns the size of the queue" do
        redis_queue = build_redis_queue

        expect(redis_queue.size).to eq(0)

        redis_queue.enqueue(key: "value1")
        expect(redis_queue.size).to eq(1)

        redis_queue.enqueue(key: "value2")
        expect(redis_queue.size).to eq(2)

        redis_queue.dequeue do
          expect(redis_queue.size).to eq(2)
        end

        expect(redis_queue.size).to eq(1)
      end

      it "respects the namespace" do
        redis_queue = build_redis_queue(namespace: "namespace")
        redis_queue.enqueue(key: "value1")
        redis_queue.enqueue(key: "value2")

        redis_queue.dequeue do
          expect(redis_queue.size).to eq(2)
        end

        expect(redis_queue.size).to eq(1)
      end

      it "updates the expiry" do
        redis_queue = build_redis_queue(ttl: 30)
        redis_queue.enqueue(key: "value1")
        redis_queue.enqueue(key: "value2")
        redis_queue.stop

        redis_queue.dequeue do
          redis.persist("kraps:queue:token")
          redis.persist("kraps:pending:token")
          redis.persist("kraps:status:token")

          redis_queue.size

          expect(redis.ttl("kraps:queue:token")).to be_between(29, 30)
          expect(redis.ttl("kraps:pending:token")).to be_between(29, 30)
          expect(redis.ttl("kraps:status:token")).to be_between(29, 30)
        end
      end
    end

    describe "#dequeue" do
      it "dequeues a job" do
        redis_queue = build_redis_queue
        redis_queue.enqueue(key: "value")

        job = nil

        expect(redis.llen("kraps:queue:token")).to eq(1)
        expect(redis.zcard("kraps:pending:token")).to eq(0)

        redis_queue.dequeue do |payload|
          job = payload

          expect(redis.zcard("kraps:pending:token")).to eq(1)
        end

        expect(redis.llen("kraps:queue:token")).to eq(0)
        expect(redis.zcard("kraps:pending:token")).to eq(0)

        expect(job).to eq("key" => "value")
      end

      it "yields nil when no job is available" do
        redis_queue = build_redis_queue

        job = "none"

        redis_queue.dequeue do |payload|
          job = payload
        end

        expect(job).to be_nil
      end

      it "respects the namespace" do
        redis_queue = build_redis_queue(namespace: "namespace")
        redis_queue.enqueue(key: "value")

        job = nil

        expect(redis.llen("namespace:kraps:queue:token")).to eq(1)
        expect(redis.zcard("namespace:kraps:pending:token")).to eq(0)

        redis_queue.dequeue do |payload|
          job = payload

          expect(redis.zcard("namespace:kraps:pending:token")).to eq(1)
        end

        expect(redis.llen("namespace:kraps:queue:token")).to eq(0)
        expect(redis.zcard("namespace:kraps:pending:token")).to eq(0)

        expect(job).to eq("key" => "value")
      end

      it "keeps the job in the pending list when an exception is raised" do
        redis_queue = build_redis_queue
        redis_queue.enqueue(key: "value")

        expect do
          redis_queue.dequeue do
            raise "error"
          end
        end.to raise_error("error")

        expect(redis.llen("kraps:queue:token")).to eq(0)
        expect(redis.zcard("kraps:pending:token")).to eq(1)
      end

      it "returns the job from the pending list when it gets visible again" do
        redis_queue = build_redis_queue
        redis_queue.enqueue(key: "value1")
        redis_queue.enqueue(key: "value2")

        expect do
          redis_queue.dequeue do
            raise "error"
          end
        end.to raise_error("error")

        redis.zadd("kraps:pending:token", redis.time[0] - 1, JSON.generate(key: "value1"))

        job = nil

        redis_queue.dequeue do |payload|
          job = payload
        end

        expect(job).to eq("key" => "value1")
      end

      it "does not return the job from the pending list when it is not visible again yet" do
        redis_queue = build_redis_queue
        redis_queue.enqueue(key: "value1")
        redis_queue.enqueue(key: "value2")

        expect do
          redis_queue.dequeue do
            raise "error"
          end
        end.to raise_error("error")

        job = nil

        redis_queue.dequeue do |payload|
          job = payload
        end

        expect(job).to eq("key" => "value2")
      end

      it "updates the expiry" do
        redis_queue = build_redis_queue(ttl: 30)
        redis_queue.enqueue(key: "value1")
        redis_queue.enqueue(key: "value2")
        redis_queue.enqueue(key: "value3")
        redis_queue.stop

        redis_queue.dequeue do
          redis.persist("kraps:queue:token")
          redis.persist("kraps:pending:token")
          redis.persist("kraps:status:token")

          redis_queue.dequeue do
            # nothing
          end

          expect(redis.ttl("kraps:queue:token")).to be_between(29, 30)
          expect(redis.ttl("kraps:pending:token")).to be_between(29, 30)
          expect(redis.ttl("kraps:status:token")).to be_between(29, 30)
        end
      end
    end

    describe "#stop" do
      it "sets the status of the queue to stopped" do
        redis_queue = build_redis_queue

        expect(redis_queue.stopped?).to eq(false)

        redis_queue.stop

        expect(redis_queue.stopped?).to eq(true)
      end

      it "respects the namespace" do
        redis_queue = build_redis_queue(namespace: "namespace")

        expect(redis_queue.stopped?).to eq(false)

        redis_queue.stop

        expect(redis_queue.stopped?).to eq(true)
      end

      it "updates the expiry" do
        redis_queue = build_redis_queue(ttl: 30)
        redis_queue.enqueue(key: "value1")
        redis_queue.enqueue(key: "value2")

        redis_queue.dequeue do
          redis.persist("kraps:queue:token")
          redis.persist("kraps:pending:token")
          redis.persist("kraps:status:token")

          redis_queue.stop

          expect(redis.ttl("kraps:queue:token")).to be_between(29, 30)
          expect(redis.ttl("kraps:pending:token")).to be_between(29, 30)
          expect(redis.ttl("kraps:status:token")).to be_between(29, 30)
        end
      end
    end
  end
end
