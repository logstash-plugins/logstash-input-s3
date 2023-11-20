# encoding: utf-8
#
require "logstash/inputs/s3"
require "aws-sdk-s3"
require "ostruct"
require "rspec/wait"

describe LogStash::Inputs::S3::Poller do
  let(:sincedb) { double("sincedb").as_null_object }
  let(:logger) { double("logger").as_null_object }
  let(:bucket_name) { "my-stuff" }
  let(:bucket) { Aws::S3::Bucket.new(:stub_responses => true, :name => bucket_name) }
  let(:remote_objects) { double("remote_objects") }
  let(:objects) { [OpenStruct.new({:key => "myobject", :last_modified => Time.now-60, :body => "Nooo" })] }

  before :each do
    allow(bucket).to receive(:objects).with(anything).and_return(remote_objects)
    allow(remote_objects).to receive(:limit).with(anything) do |num|
      expect(num).to be_a(Integer)
      expect(num).to be > 0
      objects
    end
  end

  subject { described_class.new(bucket, sincedb, logger) }

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

  context 'use_start_after is true' do
    let(:options) { {:use_start_after => true} }
    subject { described_class.new(bucket, sincedb, logger, options) }

    context 'there are files to fetch' do
      it "sets the last_key_fetched" do
        retrieved_objects = []

        subject.run do |object|
          retrieved_objects << object
          subject.stop if objects.size == retrieved_objects.size
        end

        expect(subject.instance_variable_get(:@last_key_fetched)).to eq(objects.first.key)
      end
    end

    context 'there are no files to fetch' do
      let(:objects) { [] }

      it "does not set the last_key_fetched" do
        subject.run {}
        expect(subject.instance_variable_get(:@last_key_fetched)).to be_nil
      end

      context 'and there is an oldest_key in the sincedb' do
        let(:oldest_key) { "oldest-object-key" }

        it 'calls @bucekt.objects with the appropriate listing options' do
          allow(sincedb).to receive(:oldest_key).and_return(oldest_key)
          subject.run {}
          expect(bucket).to have_received(:objects).with({:start_after => oldest_key})
        end
      end

      context 'and there is no oldest_key in the sincedb' do
        it 'calls @bucket.objects with no listing options' do
          allow(sincedb).to receive(:oldest_key).and_return(nil)
          subject.run {}
          expect(bucket).to have_received(:objects).with({})
        end
      end
    end
  end
end
