# encoding: utf-8
require "logstash/inputs/s3/processor"
require "logstash/inputs/s3/remote_file"
require "logstash/inputs/s3/processing_policy_validator"
require "logstash/inputs/s3/event_processor"
require "cabin"

describe LogStash::Inputs::S3::Processor do
  let(:event_processor) { spy("LogStash::Inputs::S3::EventProcessor") }
  let(:post_processor_1) { spy("LogStash::Inputs::S3::PostProcessor") }
  let(:post_processor_2) { spy("LogStash::Inputs::S3::PostProcessor") }
  let(:post_processors) { [post_processor_1, post_processor_2] }
  let(:logger) { Cabin::Channel.get }

  let(:validator) { LogStash::Inputs::S3::ProcessingPolicyValidator.new(logger, LogStash::Inputs::S3::ProcessingPolicyValidator::SkipEmptyFile) }
  let(:remote_file) { LogStash::Inputs::S3::RemoteFile.new(logger, s3_object) }
  let(:s3_object) { double("s3_object",
                           :key => "hola",
                           :bucket_name => "mon-bucket",
                           :content_length => 20,
                           :last_modified => Time.now-60) }

  subject { described_class.new(logger, validator, event_processor, post_processors) }

  context "When handling remote file" do
    context "when the file is not valid to process" do
      let(:s3_object) { double("s3_object",
                               :key => "hola",
                               :content_length => 0,
                               :last_modified => Time.now-60) }

      it "doesnt download the file" do
        expect(remote_file).not_to receive(:download!)
        subject.handle(remote_file)
      end
    end

    context "when the file is valid to process" do
      let(:content) { "bonjour la famille" }
      let(:metadata) { { "s3" => { "key" => "hola", "bucket_name" => "mon-bucket" }} }

      before do
        expect(remote_file).to receive(:download!).and_return(true)
        expect(remote_file).to receive(:each_line).and_yield(content, metadata)
      end

      it "send the file content to the event processor" do
        subject.handle(remote_file)
        expect(event_processor).to have_received(:process).with(content, { "s3" => hash_including(metadata["s3"])})
      end

      it "sends the file to all post processors" do
        subject.handle(remote_file)
        expect(post_processor_1).to have_received(:process).with(remote_file)
        expect(post_processor_2).to have_received(:process).with(remote_file)
      end
    end
  end
end

