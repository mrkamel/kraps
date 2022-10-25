module Kraps
  RSpec.describe TempPath do
    describe "#initialize" do
      it "generates a path to a tempfile" do
        temp_path = described_class.new

        expect(File.exist?(temp_path.path)).to eq(true)
        expect(temp_path.path).to match(%r{^/tmp/[a-f0-9]{16}.[0-9]+$})
      end

      it "respects prefix and suffix" do
        temp_path = described_class.new(prefix: "prefix", suffix: "suffix")

        expect(temp_path.path).to match(%r{^/tmp/prefix.[a-f0-9]{16}.[0-9]+.suffix$})
      end
    end

    describe "#unlink" do
      it "deletes the tempfile" do
        temp_path = described_class.new
        temp_path.unlink

        expect(File.exist?(temp_path.path)).to eq(false)
      end
    end
  end
end
