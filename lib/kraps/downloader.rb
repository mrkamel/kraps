module Kraps
  class Downloader
    def self.download_all(prefix:, concurrency:)
      temp_paths = TempPaths.new

      files = Kraps.driver.list(prefix: prefix).sort

      temp_paths_index = files.each_with_object({}) do |file, hash|
        hash[file] = temp_paths.add
      end

      Parallelizer.each(files, concurrency) do |file|
        Kraps.driver.download(file, temp_paths_index[file].path)
      end

      temp_paths
    end
  end
end
