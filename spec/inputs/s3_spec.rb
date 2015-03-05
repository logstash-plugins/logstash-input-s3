# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/inputs/s3"
require "logstash/errors"
require "aws-sdk"
require "stud/temporary"
require_relative "../support/helpers"

describe LogStash::Inputs::S3 do
  before do
    AWS.stub!
    Thread.abort_on_exception = true
  end
  let(:day) { 3600 * 24 }
  let(:settings) {
    {
      "access_key_id" => "1234",
      "secret_access_key" => "secret",
      "bucket" => "logstash-test"
    }
  }

  describe "#list_new_files" do
    before { allow_any_instance_of(AWS::S3::ObjectCollection).to receive(:with_prefix).with(nil) { objects_list } }

    let!(:present_object) { double(:key => 'this-should-be-present', :last_modified => Time.now) }
    let(:objects_list) {
      [
        double(:key => 'exclude-this-file-1', :last_modified => Time.now - 2 * day),
        double(:key => 'exclude/logstash', :last_modified => Time.now - 2 * day),
        present_object
      ]
    }

    it 'should allow user to exclude files from the s3 bucket' do
      config = LogStash::Inputs::S3.new(settings.merge({ "exclude_pattern" => "^exclude" }))
      config.register
      expect(config.list_new_files).to eq([present_object.key])
    end

    it 'should support not providing a exclude pattern' do
      config = LogStash::Inputs::S3.new(settings)
      config.register
      expect(config.list_new_files).to eq(objects_list.map(&:key))
    end

    context "If the bucket is the same as the backup bucket" do
      it 'should ignore files from the bucket if they match the backup prefix' do
        objects_list = [
          double(:key => 'mybackup-log-1', :last_modified => Time.now),
          present_object
        ]

        allow_any_instance_of(AWS::S3::ObjectCollection).to receive(:with_prefix).with(nil) { objects_list }

        config = LogStash::Inputs::S3.new(settings.merge({ 'backup_add_prefix' => 'mybackup',
                                                           'backup_to_bucket' => settings['bucket']}))
        config.register
        expect(config.list_new_files).to eq([present_object.key])
      end
    end

    it 'should ignore files older than X' do
      config = LogStash::Inputs::S3.new(settings.merge({ 'backup_add_prefix' => 'exclude-this-file'}))

      expect_any_instance_of(LogStash::Inputs::S3::SinceDB::File).to receive(:read).exactly(objects_list.size) { Time.now - day }
      config.register

      expect(config.list_new_files).to eq([present_object.key])
    end

    it 'should ignore file if the file match the prefix' do
        prefix = 'mysource/'

        objects_list = [
          double(:key => prefix, :last_modified => Time.now),
          present_object
        ]

        allow_any_instance_of(AWS::S3::ObjectCollection).to receive(:with_prefix).with(prefix) { objects_list }

        config = LogStash::Inputs::S3.new(settings.merge({ 'prefix' => prefix }))
        config.register
        expect(config.list_new_files).to eq([present_object.key])
    end

    it 'should sort return object sorted by last_modification date with older first' do
      objects = [
        double(:key => 'YESTERDAY', :last_modified => Time.now - day),
        double(:key => 'TODAY', :last_modified => Time.now),
        double(:key => 'TWO_DAYS_AGO', :last_modified => Time.now - 2 * day)
      ]

      allow_any_instance_of(AWS::S3::ObjectCollection).to receive(:with_prefix).with(nil) { objects }


      config = LogStash::Inputs::S3.new(settings)
      config.register
      expect(config.list_new_files).to eq(['TWO_DAYS_AGO', 'YESTERDAY', 'TODAY'])
    end

    describe "when doing backup on the s3" do
      it 'should copy to another s3 bucket when keeping the original file' do
        config = LogStash::Inputs::S3.new(settings.merge({ "backup_to_bucket" => "mybackup"}))
        config.register

        s3object = double()
        expect(s3object).to receive(:copy_to).with('test-file', :bucket => an_instance_of(AWS::S3::Bucket))

        config.backup_to_bucket(s3object, 'test-file')
      end

      it 'should move to another s3 bucket when deleting the original file' do
        config = LogStash::Inputs::S3.new(settings.merge({ "backup_to_bucket" => "mybackup", "delete" => true }))
        config.register

        s3object = double()
        expect(s3object).to receive(:move_to).with('test-file', :bucket => an_instance_of(AWS::S3::Bucket))

        config.backup_to_bucket(s3object, 'test-file')
      end

      it 'should add the specified prefix to the backup file' do
        config = LogStash::Inputs::S3.new(settings.merge({ "backup_to_bucket" => "mybackup",
                                                           "backup_add_prefix" => 'backup-' }))
        config.register

        s3object = double()
        expect(s3object).to receive(:copy_to).with('backup-test-file', :bucket => an_instance_of(AWS::S3::Bucket))

        config.backup_to_bucket(s3object, 'test-file')
      end
    end

    it 'should support doing local backup of files' do
      Stud::Temporary.directory do |backup_dir|
        Stud::Temporary.file do |source_file|
          backup_file = File.join(backup_dir.to_s, Pathname.new(source_file.path).basename.to_s)

          config = LogStash::Inputs::S3.new(settings.merge({ "backup_to_dir" => backup_dir }))

          config.backup_to_dir(source_file)

          expect(File.exists?(backup_file)).to be_true
        end
      end
    end

    it 'should accepts a list of credentials for the aws-sdk, this is deprecated' do
      Stud::Temporary.directory do |tmp_directory|
        old_credentials_settings = {
          "credentials" => ['1234', 'secret'],
          "backup_to_dir" => tmp_directory,
          "bucket" => "logstash-test"
        }

        config = LogStash::Inputs::S3.new(old_credentials_settings)
        expect{ config.register }.not_to raise_error
      end
    end
  end

  context 'when working with logs' do
    let(:objects) { [log] }
    let(:log) { double(:key => 'uncompressed.log', :last_modified => Time.now - 2 * day) }

    before do
      allow_any_instance_of(AWS::S3::ObjectCollection).to receive(:with_prefix).with(nil) { objects }
      allow_any_instance_of(AWS::S3::ObjectCollection).to receive(:[]).with(log.key) { log }
      expect(log).to receive(:read)  { |&block| block.call(File.read(log_file)) }
    end

    context 'compressed' do
      let(:log) { double(:key => 'log.gz', :last_modified => Time.now - 2 * day) }
      let(:log_file) { File.join(File.dirname(__FILE__), '..', 'fixtures', 'compressed.log.gz') }

      it 'should process events' do
        events = fetch_events(settings)
        expect(events.size).to eq(2)
      end
    end

    context 'plain text' do
      let(:log_file) { File.join(File.dirname(__FILE__), '..', 'fixtures', 'uncompressed.log') }

      it 'should process events' do
        events = fetch_events(settings)
        expect(events.size).to eq(2)
      end
    end

    context 'encoded' do
      let(:log_file) { File.join(File.dirname(__FILE__), '..', 'fixtures', 'invalid_utf8.log') }

      it 'should work with invalid utf-8 log event' do
        events = fetch_events(settings)
        expect(events.size).to eq(2)
      end
    end

    context 'cloudfront' do
      let(:log_file) { File.join(File.dirname(__FILE__), '..', 'fixtures', 'cloudfront.log') }

      it 'should extract metadata from cloudfront log' do
        events = fetch_events(settings)

        expect(events.size).to eq(2)

        events.each do |event|
          expect(event['cloudfront_fields']).to eq('date time x-edge-location c-ip x-event sc-bytes x-cf-status x-cf-client-id cs-uri-stem cs-uri-query c-referrer x-page-url​  c-user-agent x-sname x-sname-query x-file-ext x-sid')
          expect(event['cloudfront_version']).to eq('1.0')
        end
      end
    end
  end
end
