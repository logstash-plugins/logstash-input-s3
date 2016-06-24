# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/inputs/s3"
require "logstash/errors"
require "aws-sdk-resources"
require_relative "../support/helpers"
require "stud/temporary"
require "aws-sdk"
require "fileutils"

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


  before do
    FileUtils.mkdir_p(sincedb_path)
    Aws.config[:stub_responses] = true
    Thread.abort_on_exception = true
  end

  context "when interrupting the plugin" do
    let(:config) { super.merge({ "interval" => 5 }) }

    before do
      expect_any_instance_of(LogStash::Inputs::S3).to receive(:list_new_files).and_return(TestInfiniteS3Object.new)
    end

    it_behaves_like "an interruptible input plugin"
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
  end

  describe "#list_new_files" do
    before { allow_any_instance_of(Aws::S3::Bucket).to receive(:objects) { objects_list } }

    let!(:present_object) { double(:key => 'this-should-be-present', :last_modified => Time.now) }
    let(:objects_list) {
      [
        double(:key => 'exclude-this-file-1', :last_modified => Time.now - 2 * day),
        double(:key => 'exclude/logstash', :last_modified => Time.now - 2 * day),
        present_object
      ]
    }

    it 'should allow user to exclude files from the s3 bucket' do
      plugin = LogStash::Inputs::S3.new(config.merge({ "exclude_pattern" => "^exclude" }))
      plugin.register
      expect(plugin.list_new_files).to eq([present_object.key])
    end

    it 'should support not providing a exclude pattern' do
      plugin = LogStash::Inputs::S3.new(config)
      plugin.register
      expect(plugin.list_new_files).to eq(objects_list.map(&:key))
    end

    context "If the bucket is the same as the backup bucket" do
      it 'should ignore files from the bucket if they match the backup prefix' do
        objects_list = [
          double(:key => 'mybackup-log-1', :last_modified => Time.now),
          present_object
        ]

        allow_any_instance_of(Aws::S3::Bucket).to receive(:objects) { objects_list }

        plugin = LogStash::Inputs::S3.new(config.merge({ 'backup_add_prefix' => 'mybackup',
                                                         'backup_to_bucket' => config['bucket']}))
        plugin.register
        expect(plugin.list_new_files).to eq([present_object.key])
      end
    end

    it 'should ignore files older than X' do
      plugin = LogStash::Inputs::S3.new(config.merge({ 'backup_add_prefix' => 'exclude-this-file'}))

      expect_any_instance_of(LogStash::Inputs::S3::SinceDB::File).to receive(:read).exactly(objects_list.size) { Time.now - day }
      plugin.register

      expect(plugin.list_new_files).to eq([present_object.key])
    end

    it 'should ignore file if the file match the prefix' do
        prefix = 'mysource/'

        objects_list = [
          double(:key => prefix, :last_modified => Time.now),
          present_object
        ]

        allow_any_instance_of(Aws::S3::Bucket).to receive(:objects).with(:prefix => prefix) { objects_list }

        plugin = LogStash::Inputs::S3.new(config.merge({ 'prefix' => prefix }))
        plugin.register
        expect(plugin.list_new_files).to eq([present_object.key])
    end

    it 'should sort return object sorted by last_modification date with older first' do
      objects = [
        double(:key => 'YESTERDAY', :last_modified => Time.now - day),
        double(:key => 'TODAY', :last_modified => Time.now),
        double(:key => 'TWO_DAYS_AGO', :last_modified => Time.now - 2 * day)
      ]

      allow_any_instance_of(Aws::S3::Bucket).to receive(:objects) { objects }


      plugin = LogStash::Inputs::S3.new(config)
      plugin.register
      expect(plugin.list_new_files).to eq(['TWO_DAYS_AGO', 'YESTERDAY', 'TODAY'])
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
    it 'should process events' do
      events = fetch_events(config)
      expect(events.size).to eq(2)
    end

    it "deletes the temporary file" do
      events = fetch_events(config)
      expect(Dir.glob(File.join(temporary_directory, "*")).size).to eq(0)
    end
  end

  context 'when working with logs' do
    let(:objects) { [log] }
    let(:log) { double(:key => 'uncompressed.log', :last_modified => Time.now - 2 * day) }
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

    context 'compressed' do
      let(:log) { double(:key => 'log.gz', :last_modified => Time.now - 2 * day) }
      let(:log_file) { File.join(File.dirname(__FILE__), '..', 'fixtures', 'compressed.log.gz') }

      include_examples "generated events"
    end

    context 'plain text' do
      let(:log_file) { File.join(File.dirname(__FILE__), '..', 'fixtures', 'uncompressed.log') }

      include_examples "generated events"
    end

    context 'encoded' do
      let(:log_file) { File.join(File.dirname(__FILE__), '..', 'fixtures', 'invalid_utf8.log') }

      include_examples "generated events"
    end

    context 'cloudfront' do
      let(:log_file) { File.join(File.dirname(__FILE__), '..', 'fixtures', 'cloudfront.log') }

      it 'should extract metadata from cloudfront log' do
        events = fetch_events(config)

        events.each do |event|
          expect(event.get('cloudfront_fields')).to eq('date time x-edge-location c-ip x-event sc-bytes x-cf-status x-cf-client-id cs-uri-stem cs-uri-query c-referrer x-page-url​  c-user-agent x-sname x-sname-query x-file-ext x-sid')
          expect(event.get('cloudfront_version')).to eq('1.0')
        end
      end

      include_examples "generated events"
    end
  end
end
