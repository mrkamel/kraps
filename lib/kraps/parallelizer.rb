module Kraps
  class Parallelizer
    def self.each(enum, num_threads)
      queue = Queue.new

      enum.each { |element| queue.push element }

      stopped = false

      threads = Array.new(num_threads) do
        Thread.new do
          yield queue.pop(true) until stopped || queue.empty?
        rescue ThreadError
          # Queue empty
        rescue StandardError => e
          stopped = true

          e
        end
      end

      threads.each(&:join).each do |thread|
        raise thread.value if thread.value.is_a?(Exception)
      end

      enum
    end
  end
end
