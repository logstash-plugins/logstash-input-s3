# encoding: utf-8
require "logstash/inputs/s3/remote_file"

describe LogStash::Inputs::S3::RemoteFile do
  let(:logger) { double("logger").as_null_object }
  let(:gzip_pattern) { "\.gz(ip)?$" }

  subject { described_class.new(s3_object, logger, gzip_pattern) }

  context "#compressed_gzip?" do
    context "when remote object key ends in .gz" do
      let(:s3_object) { double("s3_object",
                               :content_type => "application/gzip",
                               :key => "hola.gz",
                               :content_length => 20,
                               :last_modified => Time.now-60) }

      it "return true" do
        expect(subject.compressed_gzip?).to be_truthy
      end
    end

    context "when remote object key ends in something else" do
      let(:s3_object) { double("s3_object",
                               :content_type => "text/plain",
                               :key => "hola",
                               :content_length => 20,
                               :last_modified => Time.now-60) }
      it "return false" do
        expect(subject.compressed_gzip?).to be_falsey
      end
    end
  end
end
