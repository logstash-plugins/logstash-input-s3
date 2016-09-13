# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/inputs/s3/post_processor"
require "logstash/inputs/s3/remote_file"
require "logstash/inputs/s3/sincedb"
require "stud/temporary"

describe LogStash::Inputs::S3::PostProcessor do
  let(:logger) { Cabin::Channel.get }
  let(:s3_object) { double("s3_object",
                           :key => "hola",
                           :bucket_name => "mon-bucket",
                           :content_length => 20,
                           :etag => "123",
                           :last_modified => Time.now-60) }
  let(:remote_file) { LogStash::Inputs::S3::RemoteFile.new(logger, s3_object) }

  describe LogStash::Inputs::S3::PostProcessor::UpdateSinceDB do
    let(:sincedb_path) { Stud::Temporary.file.path }
    let(:sincedb) { LogStash::Inputs::S3::SinceDB.new(logger, sincedb_path) }

    subject { described_class.new(sincedb) }

    after :each do
      File.delete(sincedb_path)
    end

    it "make the remote file as completed" do
      subject.process(remote_file)
      expect(sincedb.processed?(remote_file)).to be_truthy
    end
  end
end

