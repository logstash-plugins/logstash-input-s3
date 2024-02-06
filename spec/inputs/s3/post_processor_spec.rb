# encoding: utf-8
require "logstash/inputs/s3/post_processor"
require "logstash/inputs/s3/remote_file"
require "logstash/inputs/s3/sincedb"
require "stud/temporary"

describe LogStash::Inputs::S3::PostProcessor do
  let(:logger) { double("logger").as_null_object }
  let(:gzip_pattern) { "*.gz" }
  let(:remote_file) { LogStash::Inputs::S3::RemoteFile.new(s3_object, logger, gzip_pattern) }
  let(:s3_object) { double("s3_object",
                           :key => "hola",
                           :bucket_name => "mon-bucket",
                           :content_length => 20,
                           :etag => "123",
                           :last_modified => Time.now-60) }

  describe LogStash::Inputs::S3::PostProcessor::UpdateSinceDB do
    let(:ignore_older) { 3600 }
    let(:sincedb_path) { Stud::Temporary.file.path }
    let(:logger) { double("logger").as_null_object }

    before do
      # Avoid starting the bookkeeping thread since it will keep running after the test
      allow_any_instance_of(LogStash::Inputs::S3::SinceDB).to receive(:start_bookkeeping)
    end

    let(:sincedb) { LogStash::Inputs::S3::SinceDB.new(sincedb_path, ignore_older, logger) }

    subject { described_class.new(sincedb) }

    after :each do
      File.delete(sincedb_path)
    end

    it "mark the remote file as completed" do
      subject.process(remote_file)
      expect(sincedb.processed?(remote_file)).to be_truthy
    end
  end
end

