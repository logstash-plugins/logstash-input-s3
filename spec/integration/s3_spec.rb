require "logstash/devutils/rspec/spec_helper"
require "logstash/inputs/s3"
require "aws-sdk-s3"
require "fileutils"
require_relative "../support/helpers"

describe LogStash::Inputs::S3, :integration => true, :s3 => true do
  before do
    Thread.abort_on_exception = true

    upload_file('../fixtures/uncompressed.log' , "#{prefix}uncompressed_1.log")
    upload_file('../fixtures/compressed.log.gz', "#{prefix}compressed_1.log.gz")
  end

  after do
    delete_remote_files(prefix)
    FileUtils.rm_rf(temporary_directory)
    delete_remote_files(backup_prefix)
  end
  
  let(:temporary_directory) { Stud::Temporary.directory }
  let(:prefix)  { 'logstash-s3-input-prefix/' }
  
  let(:minimal_settings)  {  { "access_key_id" => ENV['AWS_ACCESS_KEY_ID'],
                               "secret_access_key" => ENV['AWS_SECRET_ACCESS_KEY'],
                               "bucket" => ENV['AWS_LOGSTASH_TEST_BUCKET'],
                               "region" => ENV["AWS_REGION"] || "us-east-1",
                               "prefix" => prefix,
                               "temporary_directory" => temporary_directory } }
  let(:backup_prefix) { "backup/" }

  it "support prefix to scope the remote files" do
    events = fetch_events(minimal_settings)
    expect(events.size).to eq(4)
  end


  it "add a prefix to the file" do
    fetch_events(minimal_settings.merge({ "backup_to_bucket" => ENV["AWS_LOGSTASH_TEST_BUCKET"],
                                                   "backup_add_prefix" => backup_prefix }))
    expect(list_remote_files(backup_prefix).size).to eq(2)
  end

  it "allow you to backup to a local directory" do
    Stud::Temporary.directory do |backup_dir|
      fetch_events(minimal_settings.merge({ "backup_to_dir" => backup_dir }))
      expect(Dir.glob(File.join(backup_dir, "*")).size).to eq(2)
    end
  end

  context "remote backup" do
    it "another bucket" do
      fetch_events(minimal_settings.merge({ "backup_to_bucket" => "logstash-s3-input-backup"}))
      expect(list_remote_files("", "logstash-s3-input-backup").size).to eq(2)
    end

    after do
      delete_bucket("logstash-s3-input-backup")
    end
  end
end
