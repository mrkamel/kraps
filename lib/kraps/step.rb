module Kraps
  Step = Struct.new(:action, :partitioner, :partitions, :block, :worker, :before, :frame, keyword_init: true)
end
