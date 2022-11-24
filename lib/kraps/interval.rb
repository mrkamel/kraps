module Kraps
  class Interval
    def initialize(timeout, &block)
      @queue = TimeoutQueue.new
      @stopped = false

      @thread = Thread.new do
        until @stopped
          @queue.deq(timeout: timeout)

          block.call unless @stopped
        end
      end
    end

    def stop
      @stopped = true
      @queue.enq(nil)
      @thread.join
    end
  end
end
