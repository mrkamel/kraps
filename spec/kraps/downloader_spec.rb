module Kraps
  RSpec.describe Downloader do
    describe ".download_all" do
      it "downloads the availble files and returns the temp paths" do
        Kraps.driver.store("path/to/chunk1.json", "chunk1")
        Kraps.driver.store("path/to/chunk2.json", "chunk2")
        Kraps.driver.store("path/to/chunk3.json", "chunk3")

        temp_paths = described_class.download_all(prefix: "path/to/", concurrency: 2)
        data = temp_paths.map { |temp_path| File.read(temp_path.path) }

        expect(data).to eq(["chunk1", "chunk2", "chunk3"])
      ensure
        temp_paths&.delete
      end

      it "returns no temp paths when there are no files to download" do
        expect(described_class.download_all(prefix: "some/unknown/path/", concurrency: 4).to_a).to eq([])
      end

      it "uses the parallelizer and passes the concurrency" do
        allow(Parallelizer).to receive(:each).and_call_original

        described_class.download_all(prefix: "some/path/", concurrency: 4)

        expect(Parallelizer).to have_received(:each).with(anything, 4)
      end
    end
  end
end
