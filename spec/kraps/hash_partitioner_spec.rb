module Kraps
  RSpec.describe HashPartitioner do
    describe "#call" do
      it "returns the correct partition number" do
        expect(described_class.new.call(["some", "key"], 128)).to eq(22)
        expect(described_class.new.call(["another", "key"], 128)).to eq(57)
        expect(described_class.new.call(["another", "key"], 4)).to eq(1)
      end

      it "converts the key to json" do
        allow(JSON).to receive(:generate).and_call_original

        described_class.new.call(["some key"], 32)

        expect(JSON).to have_received(:generate).with(["some key"])
      end

      it "calculates a SHA1" do
        allow(Digest::SHA1).to receive(:hexdigest).and_call_original

        described_class.new.call(["some key"], 32)

        expect(Digest::SHA1).to have_received(:hexdigest).with('["some key"]')
      end
    end
  end
end
