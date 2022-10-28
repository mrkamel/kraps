module Kraps
  class Interval
    include MonitorMixin

    def initialize(timeout, &block)
      super()

      @thread_queue = TimeoutQueue.new
      @main_queue = TimeoutQueue.new
      @stopped = false

      @thread = Thread.new do
        until @stopped
          item = @thread_queue.deq(timeout: timeout)

          block.call unless @stopped

          @main_queue.enq(1) if item
        end
      end
    end

    def fire(timeout:)
      @thread_queue.enq(1)
      @main_queue.deq(timeout: timeout)
    end

    def stop
      @stopped = true
      @thread_queue.enq(nil)
      @thread.join
    end
  end
end
