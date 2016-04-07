# encoding: utf-8
require "logstash/s3/downloader"

describe LogStash::Inputs::S3::Downloader do
  let(:s3_client) { Aws::S3::Client.new }
  let(:remote_file_content) { "I am a remote file" }

  before do
  end

  it "allow to download a file from S3" do
    expect(Downloader.download(s3object).rewind.read).to eq(remote_file_content) 
  end

  context "with `keep_alive`" do
    let(:keep_alive) { double("keep_alive") }

    it "periodically send keep_alive" do

    end
  end
end

## S3Object
## KeepAlive(SQSMessage)
