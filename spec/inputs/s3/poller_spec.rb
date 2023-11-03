# encoding: utf-8
#
require "logstash/inputs/s3"
require "aws-sdk-s3"
require "ostruct"
require "rspec/wait"

describe LogStash::Inputs::S3::Poller do
  let(:sincedb) { double("sincedb") }
  let(:bucket_name) { "my-stuff" }
  let(:bucket) { Aws::S3::Bucket.new(:stub_responses => true, :name => bucket_name) }
  let(:objects) { [OpenStruct.new({:key => "myobject", :last_modified => Time.now-60, :body => "Nooo" })] }

  before :each do
    allow(bucket).to receive(:objects).with(anything).and_return(objects)
  end

  subject { described_class.new(bucket) }

  it "lists the files from the remote host" do
    retrieved_objects = []

    subject.run do |object|
      retrieved_objects << object
      subject.stop if objects.size == retrieved_objects.size
    end

    expect(retrieved_objects.collect(&:key)).to eq(objects.collect(&:key))
  end

  it "can be stopped" do
    t = Thread.new {  subject.run {} }
    expect(["run", "sleep"]).to include(t.status)
    subject.stop
    wait_for { t.status }.to eq(false)
  end
end

