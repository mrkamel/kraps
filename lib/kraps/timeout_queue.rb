module Kraps
  class TimeoutQueue
    include MonitorMixin

    def initialize
      super

      @cond = new_cond
      @queue = []
    end

    def enq(item)
      synchronize do
        @queue << item
        @cond.signal
      end
    end

    def deq(timeout:)
      synchronize do
        @cond.wait(timeout) if @queue.empty?

        return @queue.empty? ? nil : @queue.shift
      end
    end
  end
end
