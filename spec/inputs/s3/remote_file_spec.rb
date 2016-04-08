# encoding: utf-8
require "logstash/inputs/s3/remote_file"

module LogStash module Inputs class S3
  describe RemoteFile do
    subject { described_class.new(s3_object) }

    context "#compressed_gzip?" do
      context "when `content_type` is `application/gzip`" do
        let(:s3_object) { double("s3_object",
                                 :content_type => "application/gzip",
                                 :key => "hola",
                                 :content_length => 20,
                                 :last_modified => Time.now-60) }

        it "return true" do
          expect(subject.compressed_gzip?).to be_truthy
        end
      end

      context "when `content_type` is not `application/gzip`" do
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
end; end; end
