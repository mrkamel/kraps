module Kraps
  class TempPaths
    include MonitorMixin
    include Enumerable

    def initialize
      super

      @temp_paths = []
    end

    def add
      synchronize do
        temp_path = TempPath.new
        @temp_paths << temp_path
        temp_path
      end
    end

    def unlink
      synchronize do
        @temp_paths.each(&:unlink)
      end
    end

    def each(&block)
      return enum_for(__method__) unless block_given?

      synchronize do
        @temp_paths.each(&block)
      end
    end
  end
end
