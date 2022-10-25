module Kraps
  class TempPath
    attr_reader :path

    def initialize(prefix: nil, suffix: nil)
      @path = File.join(Dir.tmpdir, [prefix, SecureRandom.hex[0, 16], Process.pid, suffix].compact.join("."))

      File.open(@path, File::CREAT | File::EXCL) {}

      ObjectSpace.define_finalizer(self, self.class.finalize(@path))

      return unless block_given?

      begin
        yield
      ensure
        unlink
      end
    end

    def unlink
      FileUtils.rm_f(@path)
    end

    def self.finalize(path)
      proc { FileUtils.rm_f(path) }
    end
  end
end
