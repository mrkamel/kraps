module Kraps
  RSpec.describe TempPaths do
    describe "#add" do
      it "adds a temp path" do
        temp_paths = described_class.new

        expect do
          temp_paths.add
          temp_paths.add
        end.to change { temp_paths.count }.by(2)
      end

      it "returns the temp path" do
        temp_paths = described_class.new
        temp_path = temp_paths.add

        expect(temp_paths.to_a.last).to eq(temp_path)
      end
    end

    describe "#delete" do
      it "deletes all temp paths" do
        temp_paths = described_class.new
        temp_paths.add
        temp_paths.add
        temp_paths.delete
        expect(temp_paths.all? { |temp_path| File.exist?(temp_path.path) }).to eq(false)
      end
    end

    describe "#each" do
      it "yields each temp path" do
        temp_paths = described_class.new
        temp_path1 = temp_paths.add
        temp_path2 = temp_paths.add

        expect(temp_paths.each.to_a).to eq([temp_path1, temp_path2])
      end
    end
  end
end
