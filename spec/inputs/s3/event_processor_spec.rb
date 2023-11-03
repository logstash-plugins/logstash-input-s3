# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/inputs/s3/event_processor"
require "logstash/codecs/json"
require "logstash/json"
require "thread"

describe LogStash::Inputs::S3::EventProcessor do
  let(:metadata) { { "s3" => { "bucket_name" => "bucket-land" } } }
  let(:encoded_line) { LogStash::Json.dump({ "message" => "Hello World" }) }
  let(:codec) { LogStash::Codecs::JSON.new }
  let(:queue) { Queue.new }
  
  before do
    described_class.new(codec, queue).process(encoded_line, metadata)
  end

  subject { queue.pop }

  it "uses the codec and insert the event to the queue" do
    expect(subject["message"]).to eq("Hello World")
  end

  it "add metadata to the event" do
    expect(subject["[@metadata][s3][bucket_name]"]).to eq("bucket-land")
  end
end
