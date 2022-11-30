module Kraps
  class RedisQueue
    VISIBILITY_TIMEOUT = 60

    attr_reader :token

    def initialize(redis:, token:, namespace:, ttl:)
      @redis = redis
      @token = token
      @namespace = namespace
      @ttl = ttl
    end

    def size
      @size_script ||= <<~SCRIPT
        local queue_key, pending_key, status_key, ttl, job = ARGV[1], ARGV[2], ARGV[3], tonumber(ARGV[4]), ARGV[5]

        redis.call('expire', queue_key, ttl)
        redis.call('expire', pending_key, ttl)
        redis.call('expire', status_key, ttl)

        return redis.call('llen', queue_key) + redis.call('zcard', pending_key)
      SCRIPT

      @redis.eval(@size_script, argv: [redis_queue_key, redis_pending_key, redis_status_key, @ttl])
    end

    def enqueue(payload)
      @enqueue_script ||= <<~SCRIPT
        local queue_key, pending_key, status_key, ttl, job = ARGV[1], ARGV[2], ARGV[3], tonumber(ARGV[4]), ARGV[5]

        redis.call('rpush', queue_key, job)

        redis.call('expire', queue_key, ttl)
        redis.call('expire', pending_key, ttl)
        redis.call('expire', status_key, ttl)
      SCRIPT

      @redis.eval(@enqueue_script, argv: [redis_queue_key, redis_pending_key, redis_status_key, @ttl, JSON.generate(payload)])
    end

    def dequeue
      @dequeue_script ||= <<~SCRIPT
        local queue_key, pending_key, status_key, ttl, visibility_timeout = ARGV[1], ARGV[2], ARGV[3], tonumber(ARGV[4]), tonumber(ARGV[5])

        local zitem = redis.call('zrange', pending_key, 0, 0, 'WITHSCORES')
        local job = zitem[1]

        if not zitem[2] or tonumber(zitem[2]) > tonumber(redis.call('time')[1]) then
          job = redis.call('lpop', queue_key)
        end

        redis.call('expire', queue_key, ttl)
        redis.call('expire', pending_key, ttl)
        redis.call('expire', status_key, ttl)

        if not job then return nil end

        redis.call('zadd', pending_key, tonumber(redis.call('time')[1]) + visibility_timeout, job)
        redis.call('expire', pending_key, ttl)

        return job
      SCRIPT

      job = @redis.eval(@dequeue_script, argv: [redis_queue_key, redis_pending_key, redis_status_key, @ttl, VISIBILITY_TIMEOUT])

      unless job
        yield(nil)
        return
      end

      keep_alive(job) do
        yield(JSON.parse(job)) if job
      end

      @remove_script ||= <<~SCRIPT
        local queue_key, pending_key, status_key, ttl, job = ARGV[1], ARGV[2], ARGV[3], tonumber(ARGV[4]), ARGV[5]

        redis.call('zrem', pending_key, job)

        redis.call('expire', queue_key, ttl)
        redis.call('expire', pending_key, ttl)
        redis.call('expire', status_key, ttl)
      SCRIPT

      @redis.eval(@remove_script, argv: [redis_queue_key, redis_pending_key, redis_status_key, @ttl, job])
    end

    def stop
      @stop_script ||= <<~SCRIPT
        local queue_key, pending_key, status_key, ttl = ARGV[1], ARGV[2], ARGV[3], tonumber(ARGV[4])

        redis.call('hset', status_key, 'stopped', 1)

        redis.call('expire', queue_key, ttl)
        redis.call('expire', pending_key, ttl)
        redis.call('expire', status_key, ttl)
      SCRIPT

      @redis.eval(@stop_script, argv: [redis_queue_key, redis_pending_key, redis_status_key, @ttl])
    end

    def stopped?
      @stopped_script ||= <<~SCRIPT
        local queue_key, pending_key, status_key, ttl = ARGV[1], ARGV[2], ARGV[3], tonumber(ARGV[4])

        redis.call('expire', queue_key, ttl)
        redis.call('expire', pending_key, ttl)
        redis.call('expire', status_key, ttl)

        return redis.call('hget', status_key, 'stopped')
      SCRIPT

      @redis.eval(@stopped_script, argv: [redis_queue_key, redis_pending_key, redis_status_key, @ttl]).to_i == 1
    end

    private

    def keep_alive(job)
      @keep_alive_script ||= <<~SCRIPT
        local queue_key, pending_key, status_key, ttl, job, visibility_timeout = ARGV[1], ARGV[2], ARGV[3], tonumber(ARGV[4]), ARGV[5], tonumber(ARGV[6])

        redis.call('zadd', pending_key, tonumber(redis.call('time')[1]) + visibility_timeout, job)

        redis.call('expire', queue_key, ttl)
        redis.call('expire', pending_key, ttl)
        redis.call('expire', status_key, ttl)
      SCRIPT

      interval = Interval.new(5) do
        @redis.eval(@keep_alive_script, argv: [redis_queue_key, redis_pending_key, redis_status_key, @ttl, job, VISIBILITY_TIMEOUT])
      end

      yield
    ensure
      interval&.stop
    end

    def redis_queue_key
      [@namespace, "kraps", "queue", @token].compact.join(":")
    end

    def redis_pending_key
      [@namespace, "kraps", "pending", @token].compact.join(":")
    end

    def redis_status_key
      [@namespace, "kraps", "status", @token].compact.join(":")
    end
  end
end
