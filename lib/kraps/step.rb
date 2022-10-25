module Kraps
  Step = Struct.new(:action, :args, :block, :frame, keyword_init: true)
end
