# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "logstash/plugin_mixins/aws_config"
require "time"
require "date"
require "tmpdir"
require "stud/interval"
require "stud/temporary"
require "aws-sdk"
require "logstash/inputs/s3/patch"
require "logstash/plugin_mixins/ecs_compatibility_support"

require 'java'

Aws.eager_autoload!
# Stream events from files from a S3 bucket.
#
# Each line from each file generates an event.
# Files ending in `.gz` are handled as gzip'ed files.
class LogStash::Inputs::S3 < LogStash::Inputs::Base

  java_import java.io.InputStream
  java_import java.io.InputStreamReader
  java_import java.io.FileInputStream
  java_import java.io.BufferedReader
  java_import java.util.zip.GZIPInputStream
  java_import java.util.zip.ZipException

  include LogStash::PluginMixins::AwsConfig::V2
  include LogStash::PluginMixins::ECSCompatibilitySupport(:disabled, :v1, :v8 => :v1)

  config_name "s3"

  default :codec, "plain"

  # The name of the S3 bucket.
  config :bucket, :validate => :string, :required => true

  # If specified, the prefix of filenames in the bucket must match (not a regexp)
  config :prefix, :validate => :string, :default => nil

  config :additional_settings, :validate => :hash, :default => {}

  # The path to use for writing state. The state stored by this plugin is
  # a memory of files already processed by this plugin.
  #
  # If not specified, the default is in `{path.data}/plugins/inputs/s3/...`
  #
  # Should be a path with filename not just a directory.
  config :sincedb_path, :validate => :string, :default => nil

  # Name of a S3 bucket to backup processed files to.
  config :backup_to_bucket, :validate => :string, :default => nil

  # Append a prefix to the key (full path including file name in s3) after processing.
  # If backing up to another (or the same) bucket, this effectively lets you
  # choose a new 'folder' to place the files in
  config :backup_add_prefix, :validate => :string, :default => nil

  # Path of a local directory to backup processed files to.
  config :backup_to_dir, :validate => :string, :default => nil

  # Whether to delete processed files from the original bucket.
  config :delete, :validate => :boolean, :default => false

  # Interval to wait between to check the file list again after a run is finished.
  # Value is in seconds.
  config :interval, :validate => :number, :default => 60

  # Whether to watch for new files with the interval.
  # If false, overrides any interval and only lists the s3 bucket once.
  config :watch_for_new_files, :validate => :boolean, :default => true

  # Ruby style regexp of keys to exclude from the bucket
  config :exclude_pattern, :validate => :string, :default => nil

  # Set the directory where logstash will store the tmp files before processing them.
  # default to the current OS temporary directory in linux /tmp/logstash
  config :temporary_directory, :validate => :string, :default => File.join(Dir.tmpdir, "logstash")

  # Whether or not to include the S3 object's properties (last_modified, content_type, metadata)
  # into each Event at [@metadata][s3]. Regardless of this setting, [@metdata][s3][key] will always
  # be present.
  config :include_object_properties, :validate => :boolean, :default => false

  # Regular expression used to determine whether an input file is in gzip format.
  # default to an expression that matches *.gz and *.gzip file extensions
  config :gzip_pattern, :validate => :string, :default => "\.gz(ip)?$"

  CUTOFF_SECOND = 3

  def initialize(*params)
    super
    @cloudfront_fields_key = ecs_select[disabled: 'cloudfront_fields', v1: '[@metadata][s3][cloudfront][fields]']
    @cloudfront_version_key = ecs_select[disabled: 'cloudfront_version', v1: '[@metadata][s3][cloudfront][version]']
  end

  def register
    require "fileutils"
    require "digest/md5"
    require "aws-sdk-resources"

    @logger.info("Registering", :bucket => @bucket, :region => @region)

    s3 = get_s3object

    @s3bucket = s3.bucket(@bucket)

    unless @backup_to_bucket.nil?
      @backup_bucket = s3.bucket(@backup_to_bucket)
      begin
        s3.client.head_bucket({ :bucket => @backup_to_bucket})
      rescue Aws::S3::Errors::NoSuchBucket
        s3.create_bucket({ :bucket => @backup_to_bucket})
      end
    end

    unless @backup_to_dir.nil?
      Dir.mkdir(@backup_to_dir, 0700) unless File.exists?(@backup_to_dir)
    end

    FileUtils.mkdir_p(@temporary_directory) unless Dir.exist?(@temporary_directory)

    if !@watch_for_new_files && original_params.include?('interval')
      logger.warn("`watch_for_new_files` has been disabled; `interval` directive will be ignored.")
    end
  end

  def run(queue)
    @current_thread = Thread.current
    Stud.interval(@interval) do
      process_files(queue)
      stop unless @watch_for_new_files
    end
  end # def run

  def list_new_files
    objects = []
    found = false
    current_time = Time.now
    sincedb_time = sincedb.read
    begin
      @s3bucket.objects(:prefix => @prefix).each do |log|
        found = true
        @logger.debug('Found key', :key => log.key)
        if ignore_filename?(log.key)
          @logger.debug('Ignoring', :key => log.key)
        elsif log.content_length <= 0
          @logger.debug('Object Zero Length', :key => log.key)
        elsif log.last_modified <= sincedb_time
          @logger.debug('Object Not Modified', :key => log.key)
        elsif log.last_modified > (current_time - CUTOFF_SECOND).utc # file modified within last two seconds will be processed in next cycle
          @logger.debug('Object Modified After Cutoff Time', :key => log.key)
        elsif (log.storage_class == 'GLACIER' || log.storage_class == 'DEEP_ARCHIVE') && !file_restored?(log.object)
          @logger.debug('Object Archived to Glacier', :key => log.key)
        else
          objects << log
          @logger.debug("Added to objects[]", :key => log.key, :length => objects.length)
        end
      end
      @logger.info('No files found in bucket', :prefix => prefix) unless found
    rescue Aws::Errors::ServiceError => e
      @logger.error("Unable to list objects in bucket", :exception => e.class, :message => e.message, :backtrace => e.backtrace, :prefix => prefix)
    end
    objects.sort_by { |log| log.last_modified }
  end # def fetch_new_files

  def backup_to_bucket(object)
    unless @backup_to_bucket.nil?
      backup_key = "#{@backup_add_prefix}#{object.key}"
      @backup_bucket.object(backup_key).copy_from(:copy_source => "#{object.bucket_name}/#{object.key}")
      if @delete
        object.delete()
      end
    end
  end

  def backup_to_dir(filename)
    unless @backup_to_dir.nil?
      FileUtils.cp(filename, @backup_to_dir)
    end
  end

  def process_files(queue)
    objects = list_new_files

    objects.each do |log|
      if stop?
        break
      else
        process_log(queue, log)
      end
    end
  end # def process_files

  def stop
    # @current_thread is initialized in the `#run` method,
    # this variable is needed because the `#stop` is a called in another thread
    # than the `#run` method and requiring us to call stop! with a explicit thread.
    Stud.stop!(@current_thread)
  end

  private

  # Read the content of the local file
  #
  # @param [Queue] Where to push the event
  # @param [String] Which file to read from
  # @param [S3Object] Source s3 object
  # @return [Boolean] True if the file was completely read, false otherwise.
  def process_local_log(queue, filename, object)
    @logger.debug('Processing file', :filename => filename)
    metadata = {}
    # Currently codecs operates on bytes instead of stream.
    # So all IO stuff: decompression, reading need to be done in the actual
    # input and send as bytes to the codecs.
    read_file(filename) do |line|
      if stop?
        @logger.warn("Logstash S3 input, stop reading in the middle of the file, we will read it again when logstash is started")
        return false
      end

      @codec.decode(line) do |event|
        # We are making an assumption concerning cloudfront
        # log format, the user will use the plain or the line codec
        # and the message key will represent the actual line content.
        # If the event is only metadata the event will be drop.
        # This was the behavior of the pre 1.5 plugin.
        #
        # The line need to go through the codecs to replace
        # unknown bytes in the log stream before doing a regexp match or
        # you will get a `Error: invalid byte sequence in UTF-8'
        if event_is_metadata?(event)
          @logger.debug('Event is metadata, updating the current cloudfront metadata', :event => event)
          update_metadata(metadata, event)
        else
          push_decoded_event(queue, metadata, object, event)
        end
      end
    end
    # #ensure any stateful codecs (such as multi-line ) are flushed to the queue
    @codec.flush do |event|
      push_decoded_event(queue, metadata, object, event)
    end

    return true
  end # def process_local_log

  def push_decoded_event(queue, metadata, object, event)
    decorate(event)

    if @include_object_properties
      event.set("[@metadata][s3]", object.data.to_h)
    else
      event.set("[@metadata][s3]", {})
    end

    event.set("[@metadata][s3][key]", object.key)
    event.set(@cloudfront_version_key, metadata[:cloudfront_version]) unless metadata[:cloudfront_version].nil?
    event.set(@cloudfront_fields_key, metadata[:cloudfront_fields]) unless metadata[:cloudfront_fields].nil?

    queue << event
  end

  def event_is_metadata?(event)
    return false unless event.get("message").class == String
    line = event.get("message")
    version_metadata?(line) || fields_metadata?(line)
  end

  def version_metadata?(line)
    line.start_with?('#Version: ')
  end

  def fields_metadata?(line)
    line.start_with?('#Fields: ')
  end

  def update_metadata(metadata, event)
    line = event.get('message').strip

    if version_metadata?(line)
      metadata[:cloudfront_version] = line.split(/#Version: (.+)/).last
    end

    if fields_metadata?(line)
      metadata[:cloudfront_fields] = line.split(/#Fields: (.+)/).last
    end
  end

  def read_file(filename, &block)
    if gzip?(filename)
      read_gzip_file(filename, block)
    else
      read_plain_file(filename, block)
    end
  rescue => e
    # skip any broken file
    @logger.error("Failed to read file, processing skipped", :exception => e.class, :message => e.message, :filename => filename)
  end

  def read_plain_file(filename, block)
    File.open(filename, 'rb') do |file|
      file.each(&block)
    end
  end

  def read_gzip_file(filename, block)
    file_stream = FileInputStream.new(filename)
    gzip_stream = GZIPInputStream.new(file_stream)
    decoder = InputStreamReader.new(gzip_stream, "UTF-8")
    buffered = BufferedReader.new(decoder)

    while (line = buffered.readLine())
      block.call(line)
    end
  ensure
    buffered.close unless buffered.nil?
    decoder.close unless decoder.nil?
    gzip_stream.close unless gzip_stream.nil?
    file_stream.close unless file_stream.nil?
  end

  def gzip?(filename)
    Regexp.new(@gzip_pattern).match(filename)
  end

  def sincedb
    @sincedb ||= if @sincedb_path.nil?
                    @logger.info("Using default generated file for the sincedb", :filename => sincedb_file)
                    SinceDB::File.new(sincedb_file)
                  else
                    @logger.info("Using the provided sincedb_path", :sincedb_path => @sincedb_path)
                    SinceDB::File.new(@sincedb_path)
                  end
  end

  def sincedb_file
    digest = Digest::MD5.hexdigest("#{@bucket}+#{@prefix}")
    dir = File.join(LogStash::SETTINGS.get_value("path.data"), "plugins", "inputs", "s3")
    FileUtils::mkdir_p(dir)
    path = File.join(dir, "sincedb_#{digest}")

    # Migrate old default sincedb path to new one.
    if ENV["HOME"]
      # This is the old file path including the old digest mechanism.
      # It remains as a way to automatically upgrade users with the old default ($HOME)
      # to the new default (path.data)
      old = File.join(ENV["HOME"], ".sincedb_" + Digest::MD5.hexdigest("#{@bucket}+#{@prefix}"))
      if File.exist?(old)
        logger.info("Migrating old sincedb in $HOME to {path.data}")
        FileUtils.mv(old, path)
      end
    end

    path
  end

  def symbolized_settings
    @symbolized_settings ||= symbolize_keys_and_cast_true_false(@additional_settings)
  end

  def symbolize_keys_and_cast_true_false(hash)
    case hash
    when Hash
      symbolized = {}
      hash.each { |key, value| symbolized[key.to_sym] = symbolize_keys_and_cast_true_false(value) }
      symbolized
    when 'true'
      true
    when 'false'
      false
    else
      hash
    end
  end

  def ignore_filename?(filename)
    if @prefix == filename
      return true
    elsif filename.end_with?("/")
      return true
    elsif (@backup_add_prefix && @backup_to_bucket == @bucket && filename =~ /^#{backup_add_prefix}/)
      return true
    elsif @exclude_pattern.nil?
      return false
    elsif filename =~ Regexp.new(@exclude_pattern)
      return true
    else
      return false
    end
  end

  def process_log(queue, log)
    @logger.debug("Processing", :bucket => @bucket, :key => log.key)
    object = @s3bucket.object(log.key)

    filename = File.join(temporary_directory, File.basename(log.key))
    if download_remote_file(object, filename)
      if process_local_log(queue, filename, object)
        if object.last_modified == log.last_modified
          backup_to_bucket(object)
          backup_to_dir(filename)
          delete_file_from_bucket(object)
          FileUtils.remove_entry_secure(filename, true)
          sincedb.write(log.last_modified)
        else
          @logger.info("#{log.key} is updated at #{object.last_modified} and will process in the next cycle")
        end
      end
    else
      FileUtils.remove_entry_secure(filename, true)
    end
  end

  # Stream the remove file to the local disk
  #
  # @param [S3Object] Reference to the remove S3 objec to download
  # @param [String] The Temporary filename to stream to.
  # @return [Boolean] True if the file was completely downloaded
  def download_remote_file(remote_object, local_filename)
    completed = false
    @logger.debug("Downloading remote file", :remote_key => remote_object.key, :local_filename => local_filename)
    File.open(local_filename, 'wb') do |s3file|
      return completed if stop?
      begin
        remote_object.get(:response_target => s3file)
        completed = true
      rescue Aws::Errors::ServiceError => e
        @logger.warn("Unable to download remote file", :exception => e.class, :message => e.message, :remote_key => remote_object.key)
      end
    end
    completed
  end

  def delete_file_from_bucket(object)
    if @delete and @backup_to_bucket.nil?
      object.delete()
    end
  end

  def get_s3object
    options = symbolized_settings.merge(aws_options_hash || {})
    s3 = Aws::S3::Resource.new(options)
  end

  def file_restored?(object)
    begin
      restore = object.data.restore
      if restore && restore.match(/ongoing-request\s?=\s?["']false["']/)
        if restore = restore.match(/expiry-date\s?=\s?["'](.*?)["']/)
          expiry_date = DateTime.parse(restore[1])
          return true if DateTime.now < expiry_date # restored
        else
          @logger.debug("No expiry-date header for restore request: #{object.data.restore}")
          return nil # no expiry-date found for ongoing request
        end
      end
    rescue => e
      @logger.debug("Could not determine Glacier restore status", :exception => e.class, :message => e.message)
    end
    return false
  end

  module SinceDB
    class File
      def initialize(file)
        @sincedb_path = file
      end

      # @return [Time]
      def read
        if ::File.exists?(@sincedb_path)
          content = ::File.read(@sincedb_path).chomp.strip
          # If the file was created but we didn't have the time to write to it
          return content.empty? ? Time.new(0) : Time.parse(content)
        else
          return Time.new(0)
        end
      end

      def write(since = nil)
        since = Time.now if since.nil?
        ::File.open(@sincedb_path, 'w') { |file| file.write(since.to_s) }
      end
    end
  end
end # class LogStash::Inputs::S3
