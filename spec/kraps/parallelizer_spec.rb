module Kraps
  RSpec.describe Parallelizer do
    describe ".each" do
      it "uses the specified amount of threads" do
        allow(Thread).to receive(:new).and_call_original

        described_class.each([1, 2, 3], 10) do
          # nothing
        end

        expect(Thread).to have_received(:new).exactly(10).times
      end

      it "yields each value from the collection" do
        expect(described_class.to_enum(:each, [1, 2, 3, 4, 5], 2).to_set).to eq([1, 2, 3, 4, 5].to_set)
      end

      it "raises when a thread fails" do
        expect { described_class.each([1, 2, 3], 2) { raise("error") } }.to raise_error("error")
      end
    end
  end
end
