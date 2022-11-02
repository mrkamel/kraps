module Kraps
  class HashPartitioner
    def call(key, num_partitions)
      Digest::SHA1.hexdigest(JSON.generate(key))[0..4].to_i(16) % num_partitions
    end
  end
end
