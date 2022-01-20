# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/devutils/rspec/shared_examples"
require "logstash/inputs/s3"
require "logstash/codecs/multiline"
require "logstash/errors"
require "aws-sdk-resources"
require_relative "../support/helpers"
require "stud/temporary"
require "aws-sdk"
require "fileutils"
require 'logstash/plugin_mixins/ecs_compatibility_support/spec_helper'

describe LogStash::Inputs::S3 do
  let(:temporary_directory) { Stud::Temporary.pathname }
  let(:sincedb_path) { Stud::Temporary.pathname }
  let(:day) { 3600 * 24 }
  let(:creds) { Aws::Credentials.new('1234', 'secret') }
  let(:config) {
    {
      "access_key_id" => "1234",
      "secret_access_key" => "secret",
      "bucket" => "logstash-test",
      "temporary_directory" => temporary_directory,
      "sincedb_path" => File.join(sincedb_path, ".sincedb")
    }
  }
  let(:cutoff) { LogStash::Inputs::S3::CUTOFF_SECOND }


  before do
    FileUtils.mkdir_p(sincedb_path)
    Aws.config[:stub_responses] = true
    Thread.abort_on_exception = true
  end

  context "when interrupting the plugin" do
    let(:config) { super().merge({ "interval" => 5 }) }
    let(:s3_obj) { double(:key => "awesome-key", :last_modified => Time.now.round, :content_length => 10, :storage_class => 'STANDARD', :object => double(:data => double(:restore => nil)) ) }

    before do
      expect_any_instance_of(LogStash::Inputs::S3).to receive(:list_new_files).and_return(TestInfiniteS3Object.new(s3_obj))
    end

    it_behaves_like "an interruptible input plugin" do
      let(:allowed_lag) { 16 } if LOGSTASH_VERSION.split('.').first.to_i <= 6
    end
  end

  describe "#register" do
    subject { LogStash::Inputs::S3.new(config) }

    context "with temporary directory" do
      let(:temporary_directory) { Stud::Temporary.pathname }

      it "creates the direct when it doesn't exist" do
        expect { subject.register }.to change { Dir.exist?(temporary_directory) }.from(false).to(true)
      end
    end
  end

  describe '#get_s3object' do
    subject { LogStash::Inputs::S3.new(settings) }

    context 'with modern access key options' do
      let(:settings) {
        {
          "access_key_id" => "1234",
          "secret_access_key" => "secret",
          "proxy_uri" => "http://example.com",
          "bucket" => "logstash-test",
        }
      }

      it 'should instantiate AWS::S3 clients with a proxy set' do
        expect(Aws::S3::Resource).to receive(:new).with({
          :credentials => kind_of(Aws::Credentials),
          :http_proxy => 'http://example.com',
          :region => subject.region
        })

        subject.send(:get_s3object)
      end
    end

    describe "additional_settings" do
      context "supported settings" do
        let(:settings) {
          {
            "additional_settings" => { "force_path_style" => 'true', "ssl_verify_peer" => 'false', "profile" => 'logstash' },
            "bucket" => "logstash-test",
          }
        }

        it 'should instantiate AWS::S3 clients with force_path_style set' do
          expect(Aws::S3::Resource).to receive(:new).with({
            :region => subject.region,
            :force_path_style => true, :ssl_verify_peer => false, :profile => 'logstash'
          }).and_call_original

          subject.send(:get_s3object)
        end
      end

      context 'when an unknown setting is given' do
        let(:settings) {
          {
            "additional_settings" => { "this_setting_doesnt_exist" => true },
            "bucket" => "logstash-test",
          }
        }

        it 'should raise an error' do
          expect { subject.send(:get_s3object) }.to raise_error(ArgumentError)
        end
      end
    end
  end

  describe "#list_new_files" do
    before { allow_any_instance_of(Aws::S3::Bucket).to receive(:objects) { objects_list } }

    let!(:present_object_after_cutoff) {double(:key => 'this-should-not-be-present', :last_modified => Time.now, :content_length => 10, :storage_class => 'STANDARD', :object => double(:data => double(:restore => nil)) ) }
    let!(:present_object) {double(:key => 'this-should-be-present', :last_modified => Time.now - cutoff, :content_length => 10, :storage_class => 'STANDARD', :object => double(:data => double(:restore => nil)) ) }
    let!(:archived_object) {double(:key => 'this-should-be-archived', :last_modified => Time.now  - cutoff, :content_length => 10, :storage_class => 'GLACIER', :object => double(:data => double(:restore => nil)) ) }
    let!(:deep_archived_object) {double(:key => 'this-should-be-archived', :last_modified => Time.now - cutoff, :content_length => 10, :storage_class => 'GLACIER', :object => double(:data => double(:restore => nil)) ) }
    let!(:restored_object) {double(:key => 'this-should-be-restored-from-archive', :last_modified => Time.now  - cutoff, :content_length => 10, :storage_class => 'GLACIER', :object => double(:data => double(:restore => 'ongoing-request="false", expiry-date="Thu, 01 Jan 2099 00:00:00 GMT"')) ) }
    let!(:deep_restored_object) {double(:key => 'this-should-be-restored-from-deep-archive', :last_modified => Time.now  - cutoff, :content_length => 10, :storage_class => 'DEEP_ARCHIVE', :object => double(:data => double(:restore => 'ongoing-request="false", expiry-date="Thu, 01 Jan 2099 00:00:00 GMT"')) ) }
    let(:objects_list) {
      [
        double(:key => 'exclude-this-file-1', :last_modified => Time.now - 2 * day, :content_length => 100, :storage_class => 'STANDARD'),
        double(:key => 'exclude/logstash', :last_modified => Time.now - 2 * day, :content_length => 50, :storage_class => 'STANDARD'),
        archived_object,
        restored_object,
        deep_restored_object,
        present_object,
        present_object_after_cutoff
      ]
    }

    it 'should allow user to exclude files from the s3 bucket' do
      plugin = LogStash::Inputs::S3.new(config.merge({ "exclude_pattern" => "^exclude" }))
      plugin.register

      files = plugin.list_new_files.map { |item| item.key }
      expect(files).to include(present_object.key)
      expect(files).to include(restored_object.key)
      expect(files).to include(deep_restored_object.key)
      expect(files).to_not include('exclude-this-file-1') # matches exclude pattern
      expect(files).to_not include('exclude/logstash')    # matches exclude pattern
      expect(files).to_not include(archived_object.key)   # archived
      expect(files).to_not include(deep_archived_object.key)   # archived
      expect(files).to_not include(present_object_after_cutoff.key)   # after cutoff
      expect(files.size).to eq(3)
    end

    it 'should support not providing a exclude pattern' do
      plugin = LogStash::Inputs::S3.new(config)
      plugin.register

      files = plugin.list_new_files.map { |item| item.key }
      expect(files).to include(present_object.key)
      expect(files).to include(restored_object.key)
      expect(files).to include(deep_restored_object.key)
      expect(files).to include('exclude-this-file-1')   # no exclude pattern given
      expect(files).to include('exclude/logstash')      # no exclude pattern given
      expect(files).to_not include(archived_object.key) # archived
      expect(files).to_not include(deep_archived_object.key)   # archived
      expect(files).to_not include(present_object_after_cutoff.key)   # after cutoff
      expect(files.size).to eq(5)
    end

    context 'when all files are excluded from a bucket' do
      let(:objects_list) {
        [
            double(:key => 'exclude-this-file-1', :last_modified => Time.now - 2 * day, :content_length => 100, :storage_class => 'STANDARD'),
            double(:key => 'exclude/logstash', :last_modified => Time.now - 2 * day, :content_length => 50, :storage_class => 'STANDARD'),
        ]
      }

      it 'should not log that no files were found in the bucket' do
        plugin = LogStash::Inputs::S3.new(config.merge({ "exclude_pattern" => "^exclude" }))
        plugin.register
        allow(plugin.logger).to receive(:debug).with(anything, anything)

        expect(plugin.logger).not_to receive(:info).with(/No files found/, anything)
        expect(plugin.logger).to receive(:debug).with(/Ignoring/, anything)
        expect(plugin.list_new_files).to be_empty
      end
    end

    context 'with an empty bucket' do
      let(:objects_list) { [] }

      it 'should log that no files were found in the bucket' do
        plugin = LogStash::Inputs::S3.new(config)
        plugin.register
        allow(plugin.logger).to receive(:info).with(/Using the provided sincedb_path/, anything)
        expect(plugin.logger).to receive(:info).with(/No files found/, anything)
        expect(plugin.list_new_files).to be_empty
      end
    end

    context "If the bucket is the same as the backup bucket" do
      it 'should ignore files from the bucket if they match the backup prefix' do
        objects_list = [
          double(:key => 'mybackup-log-1', :last_modified => Time.now, :content_length => 5, :storage_class => 'STANDARD'),
          present_object
        ]

        allow_any_instance_of(Aws::S3::Bucket).to receive(:objects) { objects_list }

        plugin = LogStash::Inputs::S3.new(config.merge({ 'backup_add_prefix' => 'mybackup',
                                                         'backup_to_bucket' => config['bucket']}))
        plugin.register

        files = plugin.list_new_files.map { |item| item.key }
        expect(files).to include(present_object.key)
        expect(files).to_not include('mybackup-log-1') # matches backup prefix
        expect(files.size).to eq(1)
      end
    end

    it 'should ignore files older than X' do
      plugin = LogStash::Inputs::S3.new(config.merge({ 'backup_add_prefix' => 'exclude-this-file'}))


      allow_any_instance_of(LogStash::Inputs::S3::SinceDB::File).to receive(:read).and_return(Time.now - day)
      plugin.register

      files = plugin.list_new_files.map { |item| item.key }
      expect(files).to include(present_object.key)
      expect(files).to include(restored_object.key)
      expect(files).to include(deep_restored_object.key)
      expect(files).to_not include('exclude-this-file-1') # too old
      expect(files).to_not include('exclude/logstash')    # too old
      expect(files).to_not include(archived_object.key)   # archived
      expect(files).to_not include(deep_archived_object.key) # archived
      expect(files).to_not include(present_object_after_cutoff.key)   # after cutoff
      expect(files.size).to eq(3)
    end

    it 'should ignore file if the file match the prefix' do
        prefix = 'mysource/'

        objects_list = [
          double(:key => prefix, :last_modified => Time.now, :content_length => 5, :storage_class => 'STANDARD'),
          present_object
        ]

        allow_any_instance_of(Aws::S3::Bucket).to receive(:objects).with(:prefix => prefix) { objects_list }

        plugin = LogStash::Inputs::S3.new(config.merge({ 'prefix' => prefix }))
        plugin.register
        expect(plugin.list_new_files.map { |item| item.key }).to eq([present_object.key])
    end

    it 'should sort return object sorted by last_modification date with older first' do
      objects = [
        double(:key => 'YESTERDAY', :last_modified => Time.now - day, :content_length => 5, :storage_class => 'STANDARD'),
        double(:key => 'TODAY', :last_modified => Time.now, :content_length => 5, :storage_class => 'STANDARD'),
        double(:key => 'TODAY_BEFORE_CUTOFF', :last_modified => Time.now - cutoff, :content_length => 5, :storage_class => 'STANDARD'),
        double(:key => 'TWO_DAYS_AGO', :last_modified => Time.now - 2 * day, :content_length => 5, :storage_class => 'STANDARD')
      ]

      allow_any_instance_of(Aws::S3::Bucket).to receive(:objects) { objects }


      plugin = LogStash::Inputs::S3.new(config)
      plugin.register
      expect(plugin.list_new_files.map { |item| item.key }).to eq(['TWO_DAYS_AGO', 'YESTERDAY', 'TODAY_BEFORE_CUTOFF'])
    end

    describe "when doing backup on the s3" do
      it 'should copy to another s3 bucket when keeping the original file' do
        plugin = LogStash::Inputs::S3.new(config.merge({ "backup_to_bucket" => "mybackup"}))
        plugin.register

        s3object = Aws::S3::Object.new('mybucket', 'testkey')
        expect_any_instance_of(Aws::S3::Object).to receive(:copy_from).with(:copy_source => "mybucket/testkey")
        expect(s3object).to_not receive(:delete)

        plugin.backup_to_bucket(s3object)
      end

      it 'should copy to another s3 bucket when deleting the original file' do
        plugin = LogStash::Inputs::S3.new(config.merge({ "backup_to_bucket" => "mybackup", "delete" => true }))
        plugin.register

        s3object = Aws::S3::Object.new('mybucket', 'testkey')
        expect_any_instance_of(Aws::S3::Object).to receive(:copy_from).with(:copy_source => "mybucket/testkey")
        expect(s3object).to receive(:delete)

        plugin.backup_to_bucket(s3object)
      end

      it 'should add the specified prefix to the backup file' do
        plugin = LogStash::Inputs::S3.new(config.merge({ "backup_to_bucket" => "mybackup",
                                                           "backup_add_prefix" => 'backup-' }))
        plugin.register

        s3object = Aws::S3::Object.new('mybucket', 'testkey')
        expect_any_instance_of(Aws::S3::Object).to receive(:copy_from).with(:copy_source => "mybucket/testkey")
        expect(s3object).to_not receive(:delete)

        plugin.backup_to_bucket(s3object)
      end
    end

    it 'should support doing local backup of files' do
      Stud::Temporary.directory do |backup_dir|
        Stud::Temporary.file do |source_file|
          backup_file = File.join(backup_dir.to_s, Pathname.new(source_file.path).basename.to_s)

          plugin = LogStash::Inputs::S3.new(config.merge({ "backup_to_dir" => backup_dir }))

          plugin.backup_to_dir(source_file)

          expect(File.exists?(backup_file)).to eq(true)
        end
      end
    end
  end

  shared_examples "generated events"  do
    let(:events_to_process) { 2 }

    it 'should process events' do
      events = fetch_events(config)
      expect(events.size).to eq(events_to_process)
      expect(events[0].get("[@metadata][s3][key]")).to eql log.key
      expect(events[1].get("[@metadata][s3][key]")).to eql log.key
    end

    it "deletes the temporary file" do
      events = fetch_events(config)
      expect(Dir.glob(File.join(temporary_directory, "*")).size).to eq(0)
    end
  end

  context 'while communicating with s3' do
    let(:config) {
      {
          "access_key_id" => "1234",
          "secret_access_key" => "secret",
          "bucket" => "logstash-test",
          "codec" => "json",
      }
    }
    %w(AccessDenied NotFound).each do |error|
      context "while listing bucket contents, #{error} is returned" do
        before do
          Aws.config[:s3] = {
              stub_responses: {
                list_objects: error
              }
          }
        end

        it 'should not crash the plugin' do
          events = fetch_events(config)
          expect(events.size).to eq(0)
        end
      end
    end

    %w(AccessDenied NoSuchKey).each do |error|
      context "when retrieving an object, #{error} is returned" do
        let(:objects) { [log] }
        let(:log) { double(:key => 'uncompressed.log', :last_modified => Time.now - 2 * day, :content_length => 5, :storage_class => 'STANDARD') }

        let(:config) {
          {
              "access_key_id" => "1234",
              "secret_access_key" => "secret",
              "bucket" => "logstash-test",
              "codec" => "json",
          }
        }
        before do
          Aws.config[:s3] = {
              stub_responses: {
              get_object: error
              }
          }
          allow_any_instance_of(Aws::S3::Bucket).to receive(:objects) { objects }
        end

        it 'should not crash the plugin' do
          events = fetch_events(config)
          expect(events.size).to eq(0)
        end
      end
    end
  end

  context 'when working with logs' do
    let(:objects) { [log] }
    let(:log) { double(:key => 'uncompressed.log', :last_modified => Time.now - 2 * day, :content_length => 5, :data => { "etag" => 'c2c966251da0bc3229d12c2642ba50a4' }, :storage_class => 'STANDARD') }
    let(:data) { File.read(log_file) }

    before do
      Aws.config[:s3] = {
          stub_responses: {
              get_object: { body: data }
          }
      }
      allow_any_instance_of(Aws::S3::Bucket).to receive(:objects) { objects }
      allow_any_instance_of(Aws::S3::Bucket).to receive(:object).with(log.key) { log }
      expect(log).to receive(:get).with(instance_of(Hash)) do |arg|
        File.open(arg[:response_target], 'wb') { |s3file| s3file.write(data) }
      end
    end

    context "when event doesn't have a `message` field" do
      let(:log_file) { File.join(File.dirname(__FILE__), '..', 'fixtures', 'json.log') }
      let(:config) {
        {
          "access_key_id" => "1234",
          "secret_access_key" => "secret",
          "bucket" => "logstash-test",
          "codec" => "json",
        }
      }

      include_examples "generated events"
    end

    context "when event does have a `message` field" do
      let(:log_file) { File.join(File.dirname(__FILE__), '..', 'fixtures', 'json_with_message.log') }
      let(:config) {
        {
          "access_key_id" => "1234",
          "secret_access_key" => "secret",
          "bucket" => "logstash-test",
          "codec" => "json",
        }
      }

      include_examples "generated events"
    end

    context "multiple compressed streams" do
      let(:log) { double(:key => 'log.gz', :last_modified => Time.now - 2 * day, :content_length => 5, :storage_class => 'STANDARD') }
      let(:log_file) { File.join(File.dirname(__FILE__), '..', 'fixtures', 'multiple_compressed_streams.gz') }

      include_examples "generated events" do
        let(:events_to_process) { 16 }
      end
    end

    context 'compressed' do
      let(:log) { double(:key => 'log.gz', :last_modified => Time.now - 2 * day, :content_length => 5, :storage_class => 'STANDARD') }
      let(:log_file) { File.join(File.dirname(__FILE__), '..', 'fixtures', 'compressed.log.gz') }

      include_examples "generated events"
    end

    context 'compressed with gzip extension and using default gzip_pattern option' do
      let(:log) { double(:key => 'log.gz', :last_modified => Time.now - 2 * day, :content_length => 5, :storage_class => 'STANDARD') }
      let(:log_file) { File.join(File.dirname(__FILE__), '..', 'fixtures', 'compressed.log.gzip') }

      include_examples "generated events"
    end

    context 'compressed with gzip extension and using custom gzip_pattern option' do
      let(:config) { super().merge({ "gzip_pattern" => "gee.zip$" }) }
      let(:log) { double(:key => 'log.gee.zip', :last_modified => Time.now - 2 * day, :content_length => 5, :storage_class => 'STANDARD') }
      let(:log_file) { File.join(File.dirname(__FILE__), '..', 'fixtures', 'compressed.log.gee.zip') }
       include_examples "generated events"
    end

    context 'plain text' do
      let(:log_file) { File.join(File.dirname(__FILE__), '..', 'fixtures', 'uncompressed.log') }

      include_examples "generated events"
    end

    context 'multi-line' do
      let(:log_file) { File.join(File.dirname(__FILE__), '..', 'fixtures', 'multiline.log') }
       let(:config) {
           {
              "access_key_id" => "1234",
              "secret_access_key" => "secret",
              "bucket" => "logstash-test",
              "codec" => LogStash::Codecs::Multiline.new( {"pattern" => "__SEPARATOR__", "negate" => "true",  "what" => "previous"})
           }
        }

      include_examples "generated events"
    end

    context 'encoded' do
      let(:log_file) { File.join(File.dirname(__FILE__), '..', 'fixtures', 'invalid_utf8.gbk.log') }

      include_examples "generated events"
    end

    context 'cloudfront' do
      let(:log_file) { File.join(File.dirname(__FILE__), '..', 'fixtures', 'cloudfront.log') }

      describe "metadata", :ecs_compatibility_support, :aggregate_failures do
        ecs_compatibility_matrix(:disabled, :v1) do |ecs_select|
          before(:each) do
            allow_any_instance_of(described_class).to receive(:ecs_compatibility).and_return(ecs_compatibility)
          end

          it 'should extract metadata from cloudfront log' do
            events = fetch_events(config)

            events.each do |event|
              expect(event.get ecs_select[disabled: "cloudfront_fields", v1: "[@metadata][s3][cloudfront][fields]"] ).to eq('date time x-edge-location c-ip x-event sc-bytes x-cf-status x-cf-client-id cs-uri-stem cs-uri-query c-referrer x-page-url​  c-user-agent x-sname x-sname-query x-file-ext x-sid')
              expect(event.get ecs_select[disabled: "cloudfront_version", v1: "[@metadata][s3][cloudfront][version]"] ).to eq('1.0')
            end
          end
        end
      end

      include_examples "generated events"
    end

    context 'when include_object_properties is set to true' do
      let(:config) { super().merge({ "include_object_properties" => true }) }
      let(:log_file) { File.join(File.dirname(__FILE__), '..', 'fixtures', 'uncompressed.log') }

      it 'should extract object properties onto [@metadata][s3]' do
        events = fetch_events(config)
        events.each do |event|
          expect(event.get('[@metadata][s3]')).to include(log.data)
        end
      end

      include_examples "generated events"
    end

    context 'when include_object_properties is set to false' do
      let(:config) { super().merge({ "include_object_properties" => false }) }
      let(:log_file) { File.join(File.dirname(__FILE__), '..', 'fixtures', 'uncompressed.log') }

      it 'should NOT extract object properties onto [@metadata][s3]' do
        events = fetch_events(config)
        events.each do |event|
          expect(event.get('[@metadata][s3]')).to_not include(log.data)
        end
      end

      include_examples "generated events"
    end
  end

  describe "data loss" do
    let(:s3_plugin) { LogStash::Inputs::S3.new(config) }
    let(:queue) { [] }

    before do
      s3_plugin.register
    end

    context 'events come after cutoff time' do
      it 'should be processed in next cycle' do
        s3_objects = [
          double(:key => 'TWO_DAYS_AGO', :last_modified => Time.now.round - 2 * day, :content_length => 5, :storage_class => 'STANDARD'),
          double(:key => 'YESTERDAY', :last_modified => Time.now.round - day, :content_length => 5, :storage_class => 'STANDARD'),
          double(:key => 'TODAY_BEFORE_CUTOFF', :last_modified => Time.now.round - cutoff, :content_length => 5, :storage_class => 'STANDARD'),
          double(:key => 'TODAY', :last_modified => Time.now.round, :content_length => 5, :storage_class => 'STANDARD'),
          double(:key => 'TODAY', :last_modified => Time.now.round, :content_length => 5, :storage_class => 'STANDARD')
        ]
        size = s3_objects.length

        allow_any_instance_of(Aws::S3::Bucket).to receive(:objects) { s3_objects }
        allow_any_instance_of(Aws::S3::Bucket).to receive(:object).and_return(*s3_objects)
        expect(s3_plugin).to receive(:process_log).at_least(size).and_call_original
        expect(s3_plugin).to receive(:stop?).and_return(false).at_least(size)
        expect(s3_plugin).to receive(:download_remote_file).and_return(true).at_least(size)
        expect(s3_plugin).to receive(:process_local_log).and_return(true).at_least(size)

        # first iteration
        s3_plugin.process_files(queue)

        # second iteration
        sleep(cutoff + 1)
        s3_plugin.process_files(queue)
      end
    end

    context 's3 object updated after getting summary' do
      it 'should not update sincedb' do
        s3_summary = [
          double(:key => 'YESTERDAY', :last_modified => Time.now.round - day, :content_length => 5, :storage_class => 'STANDARD'),
          double(:key => 'TODAY', :last_modified => Time.now.round - (cutoff * 10), :content_length => 5, :storage_class => 'STANDARD')
        ]

        s3_objects = [
          double(:key => 'YESTERDAY', :last_modified => Time.now.round - day, :content_length => 5, :storage_class => 'STANDARD'),
          double(:key => 'TODAY_UPDATED', :last_modified => Time.now.round, :content_length => 5, :storage_class => 'STANDARD')
        ]

        size = s3_objects.length

        allow_any_instance_of(Aws::S3::Bucket).to receive(:objects) { s3_summary }
        allow_any_instance_of(Aws::S3::Bucket).to receive(:object).and_return(*s3_objects)
        expect(s3_plugin).to receive(:process_log).at_least(size).and_call_original
        expect(s3_plugin).to receive(:stop?).and_return(false).at_least(size)
        expect(s3_plugin).to receive(:download_remote_file).and_return(true).at_least(size)
        expect(s3_plugin).to receive(:process_local_log).and_return(true).at_least(size)

        s3_plugin.process_files(queue)
        expect(s3_plugin.send(:sincedb).read).to eq(s3_summary[0].last_modified)
      end
    end
  end
end
