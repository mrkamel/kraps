module Kraps
  RSpec.describe Drivers do
    let(:driver) { Kraps::Drivers::FakeDriver.new(bucket: "bucket", prefix: "some/prefix") }

    after { driver.flush }

    describe "#with_prefix" do
      it "prepends the prefix" do
        expect(driver.with_prefix("and/path")).to eq("some/prefix/and/path")
      end
    end

    describe "#flush" do
      it "clears all objects" do
        driver.store("path/to/object1", "value1")
        driver.store("path/to/object2", "value2")

        driver.flush

        expect(driver.exists?("path/to/object1")).to eq(false)
        expect(driver.exists?("path/to/object2")).to eq(false)
      end
    end

    describe "#list" do
      it "returns the list of objects in the bucket" do
        driver.store("some/path/to/object1", "value1")
        driver.store("other/path/to/object2", "value2")

        expect(driver.list.to_a).to eq(["other/path/to/object2", "some/path/to/object1"])
      end

      it "respects the specified prefix" do
        driver.store("some/path/to/object1", "value1")
        driver.store("other/path/to/object2", "value2")
        driver.store("other/path/to/object3", "value3")

        expect(driver.list(prefix: "other/").to_a).to eq(["other/path/to/object2", "other/path/to/object3"])
      end
    end

    describe "#value" do
      it "returns the content of the object" do
        driver.store("path/to/object1", "value1")
        driver.store("path/to/object2", "value2")

        expect(driver.value("path/to/object1")).to eq("value1")
        expect(driver.value("path/to/object2")).to eq("value2")
      end
    end

    describe "#download" do
      it "downloads the object to the specified path" do
        tempfile = Tempfile.new

        driver.store("path/to/object1", "value1")
        driver.store("path/to/object2", "value2")

        driver.download("path/to/object1", tempfile.path)

        expect(tempfile.read).to eq("value1")
      ensure
        tempfile&.close(true)
      end
    end

    describe "#exists?" do
      it "returns true when the object exists and false when not" do
        driver.store("path/to/object", "value")

        expect(driver.exists?("path/to/object")).to eq(true)
        expect(driver.exists?("path/to/missing")).to eq(false)
      end
    end

    describe "#store" do
      it "stores the specified data" do
        driver.store("path/to/object", "value")

        expect(driver.value("path/to/object")).to eq("value")
      end

      it "stores the specified io" do
        tempfile = Tempfile.new
        tempfile.write("value")

        driver.store("path/to/object", tempfile.tap(&:rewind))

        expect(driver.value("path/to/object")).to eq("value")
      ensure
        tempfile&.close(true)
      end
    end
  end
end
