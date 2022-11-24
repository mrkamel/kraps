module Kraps
  RSpec.describe Interval do
    describe "#initialize" do
      it "fires after every timeout" do
        fired = 0

        interval = Interval.new(0.1) do
          fired += 1
        end

        sleep(0.35)
        interval.stop

        expect(fired).to eq(3)
      ensure
        interval&.stop
      end

      it "can be stopped" do
        interval = Interval.new(3) do
          # nothing
        end

        sleep(0.1)
        interval.stop

        expect(interval.instance_variable_get(:@thread)).not_to be_alive
      ensure
        interval&.stop
      end
    end
  end
end
