# encoding: utf-8
require "logstash/inputs/s3"
require_relative "../support/matcher_helpers"
require_relative "../support/s3_input_test_helper"
require "thread"

# Retrieve the credentials from the environment
# and clear them to make sure the credentials are taken where they shouldn't
# be.
ACCESS_KEY_ID = ENV.delete("AWS_ACCESS_KEY_ID")
SECRET_ACCESS_KEY = ENV.delete("AWS_SECRET_ACCESS_KEY")
BUCKET_SOURCE = ENV.delete("AWS_LOGSTASH_TEST_BUCKET")
REGION = ENV.fetch("AWS_LOGSTASH_REGION", "us-east-1")

describe "Retrieve logs from S3", :tags => [:integration] do
  let(:queue) { [] }

  let(:plugin) { LogStash::Inputs::S3.new(plugin_config) }

  let(:plugin_config) do
    { "bucket" => bucket_source }
  end

  context "when credentials are defined in the config as `access_key_id` `secret_access_key`" do
    let(:access_key_id) { ACCESS_KEY_ID }
    let(:secret_access_key) { SECRET_ACCESS_KEY }
    let(:bucket_source) { BUCKET_SOURCE }
    let(:region) { REGION }

    let(:plugin_config) do
      super.merge({
        "access_key_id" => access_key_id,
        "secret_access_key" => secret_access_key,
        "bucket" => bucket_source,
        "region" => region
      })
    end

    let(:s3_client) do
      credentials = Aws::Credentials.new(access_key_id, secret_access_key)
      Aws::S3::Client.new(:region => region, :credentials => credentials)
    end

    let(:s3_bucket) { Aws::S3::Bucket.new(bucket_source, :client => s3_client) }
    let(:s3_input_test_helper) { S3InputTestHelper.new(s3_bucket) }

    before :each do
      plugin.register

      s3_input_test_helper.setup

      @plugin_thread = Thread.new do
        plugin.run(queue)
      end
    end

    after :each do
      plugin.stop
      # s3_input_test_helper.teardown
    end

    it "correctly generate the content" do
      expect(queue).to include_content_of(s3_input_test_helper.content)
    end

    xit "update the local database"
    xit "it rename files with a prefix"
    xit "it move files to a bucket on complete"
  end
end
