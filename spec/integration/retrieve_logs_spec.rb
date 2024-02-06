# encoding: utf-8
require "logstash/inputs/s3"
require "logstash/inputs/s3/sincedb"
require_relative "../support/matcher_helpers"
require_relative "../support/s3_input_test_helper"
require "stud/temporary"
require "thread"

# Retrieve the credentials from the environment
# and clear them to make sure the credentials are taken where they should
# be.
ACCESS_KEY_ID = ENV.delete("AWS_ACCESS_KEY_ID")
SECRET_ACCESS_KEY = ENV.delete("AWS_SECRET_ACCESS_KEY")
BUCKET_SOURCE = ENV.fetch("AWS_LOGSTASH_TEST_BUCKET", "logstash-input-s3-test")
BACKUP_BUCKET = "ls-ph-test"
REGION = ENV.fetch("AWS_LOGSTASH_REGION", "us-east-1")

describe "Retrieve logs from S3", :tags => :integration do
  let(:queue) { Queue.new }
  let(:logger) { instance_double('LogStash::Logging::Logger') }
  let(:sincedb_args) { [
    plugin_config["sincedb_path"],
    86400,
    logger,
    { :sincedb_expire_secs => 120 }
  ] }
  let(:stub_since_db) { instance_double('LogStash::Inputs::S3::SinceDB') }

  before do
    skip "AWS credentials not found" unless ACCESS_KEY_ID && SECRET_ACCESS_KEY

    # Stub this out so that we can avoid starting the bookkeeper thread which doesn't die
    allow(LogStash::Inputs::S3::SinceDB).to receive(:new).with(*sincedb_args).and_return(stub_since_db)
    allow(stub_since_db).to receive(:close).and_return(true)
    @plugin = LogStash::Inputs::S3.new(plugin_config)
  end

  # let(:plugin) { LogStash::Inputs::S3.new(plugin_config) }

  let(:plugin_config) do
    { 
      "bucket" => bucket_source,
      "interval" => 1,
      "sincedb_path" => Stud::Temporary.file.path
    }
  end

  context "when credentials are defined in the config as `access_key_id` `secret_access_key`" do
    let(:access_key_id) { ACCESS_KEY_ID }
    let(:secret_access_key) { SECRET_ACCESS_KEY }
    let(:bucket_source) { BUCKET_SOURCE }
    let(:bucket_backup) { BACKUP_BUCKET }
    let(:region) { REGION }

    let(:plugin_config) do
      super().merge({
        "access_key_id" => access_key_id,
        "secret_access_key" => secret_access_key,
        "region" => region,
      })
    end

    let(:s3_client) do
      credentials = Aws::Credentials.new(access_key_id, secret_access_key)
      Aws::S3::Client.new(:region => region, :credentials => credentials)
    end

    let(:s3_bucket) { Aws::S3::Bucket.new(bucket_source, :client => s3_client) }
    let(:s3_input_test_helper) { S3InputTestHelper.new(s3_bucket) }

    before :each do
      @thread_abort_on_exception = Thread.abort_on_exception
      Thread.abort_on_exception = true

      @plugin.register

      s3_input_test_helper.setup

      @plugin_thread = Thread.new do
        @plugin.run(queue)
      end
    end

    after :each do
      @plugin.stop if @plugin
      Thread.abort_on_exception = @thread_abort_on_exception
    end

    it "correctly generate the content" do
      sleep(50)
      expect(queue.size).to eq(s3_input_test_helper.content.size)
      # expect(queue).to include_content_of(s3_input_test_helper.content)
    end

    xit "update the local database"
    xit "it rename files with a prefix"
    xit "it move files to a bucket on complete"
  end
end
