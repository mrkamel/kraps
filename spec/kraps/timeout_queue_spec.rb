module Kraps
  RSpec.describe TimeoutQueue do
    describe "#enq" do
      it "enqueues the item" do
        queue = described_class.new
        queue.enq("item1")
        queue.enq("item2")

        expect(queue.deq(timeout: 1)).to eq("item1")
        expect(queue.deq(timeout: 1)).to eq("item2")
      end
    end

    describe "#deq" do
      it "dequeues an item" do
        # Already tested in enq
      end

      it "returns nil after the timeout has passed and the queue is still empty" do
        queue = described_class.new
        time = Time.now.to_f

        expect(queue.deq(timeout: 0.21)).to be_nil
        expect(Time.now.to_f - time).to be > 0.2
      end
    end
  end
end
