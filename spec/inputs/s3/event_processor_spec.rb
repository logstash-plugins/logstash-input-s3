# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/inputs/s3/event_processor"
require "logstash/codecs/json"
require "logstash/json"
require "thread"

describe LogStash::Inputs::S3::EventProcessor do
  let(:logstash_inputs_s3) { double("logstash-inputs-s3") }
  let(:include_object_properties) { true }
  let(:logger) { double("Logger").as_null_object }
  let(:metadata) { { "s3" => { "bucket_name" => "bucket-land" } } }
  let(:encoded_line) { LogStash::Json.dump({ "message" => "Hello World" }) }
  let(:codec) { LogStash::Codecs::JSON.new }
  let(:queue) { Queue.new }
  let(:remote_file_data) { { "bucket_name" => "bucket-land" } }

  before do
    described_class.new(logstash_inputs_s3, codec, queue, include_object_properties, logger).process(encoded_line, metadata, remote_file_data)
  end

  subject { queue.pop }

  it "queue should have things in it" do
    expect(queue).not_to be_empty
  end

  it "Event object should not be nil" do
    expect(subject).not_to be_nil
  end

  it "uses the codec and insert the event to the queue" do
    expect(subject["message"]).to eq("Hello World")
  end

  it "add metadata to the event" do
    expect(subject["[@metadata][s3][bucket_name]"]).to eq("bucket-land")
  end
end
