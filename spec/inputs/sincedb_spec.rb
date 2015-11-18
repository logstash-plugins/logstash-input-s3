# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/inputs/s3"
require "stud/temporary"
require "fileutils"

describe LogStash::Inputs::S3::SinceDB::File do
  let(:file) { Stud::Temporary.file.path }
  subject { LogStash::Inputs::S3::SinceDB::File.new(file) }
  before do
    FileUtils.touch(file)
  end

  it "doesnt raise an exception if the file is empty" do
    expect { subject.read }.not_to raise_error
  end
end
