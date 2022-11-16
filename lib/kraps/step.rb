module Kraps
  Step = Struct.new(:action, :partitioner, :partitions, :block, :worker, :before, :frame, :dependency, :options, keyword_init: true)
end
