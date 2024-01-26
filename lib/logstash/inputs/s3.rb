# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "time"
require "date"
require "tmpdir"
require "stud/interval"
require "stud/temporary"
require "aws-sdk-s3"
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

  include LogStash::PluginMixins::ECSCompatibilitySupport(:disabled, :v1, :v8 => :v1)

  require "logstash/inputs/s3/poller"
  require "logstash/inputs/s3/processor"
  require "logstash/inputs/s3/processor_manager"
  require "logstash/inputs/s3/processing_policy_validator"
  require "logstash/inputs/s3/event_processor"
  require "logstash/inputs/s3/sincedb"
  require "logstash/inputs/s3/post_processor"

  config_name "s3"

  default :codec, "plain"

  # The name of the S3 bucket.
  config :bucket, :validate => :string, :required => true

  # The AWS region name for the bucket. For most S3 buckets this is us-east-1
  # unless otherwise configured.
  config :region, :validate => :string, :default => 'us-east-1'

  config :access_key_id, :validate => :string, :default => nil

  config :secret_access_key, :validate => :string, :default => nil

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

  # When the S3 input discovers a file that was last modified less than ignore_newer
  # it will ignore it, causing it to be processed on the next attempt. This helps
  # prevent the input from getting stuck on files that are actively being written to.
  config :ignore_newer, :validate => :number, :default => 3

  # When the S3 input discovers a file that was last modified
  # before the specified timespan in seconds, the file is ignored.
  # After it's discovery, if an ignored file is modified it is no
  # longer ignored and any new data is read. The default is 24 hours.
  config :ignore_older, :validate => :number, :default => 24 * 60 * 60

  # Use the object key as the SinceDB key, rather than the last_modified date.
  # If this is set to true, objects will be fetched from S3 using start_after
  # so that filtering of old objects can happen on the server side, which
  # should dramatically speed up the initial listing of a bucket with many
  # objects. If set to true, you can use the sincedb_start_value parameter to
  # start at a manually specified key.
  config :use_start_after, :validate => :boolean, :default => false

  # Used in concert with object_key_sincedb, this allows you to specify the
  # object key to start at. This is useful if you want to start processing
  # if you want to start from a specific key rather than the last key that
  # was processed. Note that leaving this value the same across multiple restarts
  # will cause the input to reprocess all objects that have been processed before.
  # Can also be used without @object_key_sincedb to start at a specific last_modified
  # date. (Format: 2023-10-27 15:00:12 UTC). Once the value has been set, the
  # pipeline shuts down to prevent accidentally leaving this value set and surprising
  # people upon restart later on.
  config :sincedb_start_value, :validate => :string, :default => nil

  # How many threads to use for processing. You may want to tweak this for whatever
  # gives you the best performance for your particular environment.
  config :processors_count, :validate => :number, :default => 20

  # The number of events to fetch from S3 per request. The default is 1000.
  config :batch_size, :validate => :number, :default => 1000

  # Clear the sincedb database at startup and exit.
  config :purge_sincedb, :validate => :boolean, :default => false

  # Expire SinceDB entries that are sincedb_expire_secs older than the newest entry.
  # This keeps the database from getting too large and slowing down processing.
  config :sincedb_expire_secs, :validate => :number, :default => 120

  public
  def initialize(options = {})
    super

    if @purge_sincedb
      @logger.info("Purging the sincedb and exiting", :sincedb_path => @sincedb_path)
      ::File.unlink(@sincedb_path) rescue nil
      return
    end

    @sincedb = SinceDB.new(
      @sincedb_path,
      @ignore_older,
      @logger,
      { :sincedb_expire_secs => @sincedb_expire_secs }
    )
  end

  def register
    require "fileutils"
    require "digest/md5"

    @logger.info("Registering", :bucket => @bucket, :region => @region)

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
    return if @purge_sincedb

    if @sincedb_start_value && !@sincedb_start_value.empty?
      reseed_sincedb
      return
    end

    @poller = Poller.new(
      bucket_source,
      @sincedb,
      @logger,
      {
        :polling_interval => @interval,
        :use_start_after => @use_start_after,
        :batch_size => @batch_size,
        :gzip_pattern => @gzip_pattern
      }
    )

    validator = ProcessingPolicyValidator.new(@logger, *processing_policies)

    # Each processor is run into his own thread.
    processor = Processor.new(
      validator,
      EventProcessor.new(self, @codec, queue, @include_object_properties, @logger),
      @logger,
      post_processors
    )

    @manager = ProcessorManager.new(@logger, { :processor => processor,
                                               :processors_count => @processors_count})
    @manager.start

    # The poller get all the new files from the S3 buckets,
    # all the actual work is done in a processor which will handle the following
    # tasks:
    #  - Downloading
    #  - Uncompressing
    #  - Reading (with metadata extraction for cloudfront)
    #  - enqueue
    #  - Backup strategy
    #  - Book keeping
    @poller.run do |remote_file|
      remote_file.download_to_path = @temporary_directory
      @manager.enqueue_work(remote_file)
    end
  end

  def stop
    # Gracefully stop the polling of new S3 documents
    # the manager will stop consuming events from the queue, but will block until
    # all the processors thread are done with their work this may take some time if we are downloading large
    # files.
    @poller.stop unless @poller.nil?
    @manager.stop unless @manager.nil?
    @sincedb.close # Force a fsync of the database
  end

  private

  def reseed_sincedb
    start_object = bucket_source.objects(:prefix => @sincedb_start_value).first

    if start_object
      @logger.info("Reseeding sincedb and shutting down", :value => @sincedb_start_value)
      ::File.unlink(@sincedb_path) rescue nil
      @sincedb.reseed(start_object)
      return
    end

    raise "Could not find sincedb_start_value object [sincedb_start_value=#{@sincedb_start_value}]"
  end

  def processing_policies
    [
      ProcessingPolicyValidator::SkipEndingDirectory,
      ProcessingPolicyValidator::SkipEmptyFile,
      ProcessingPolicyValidator::IgnoreNewerThan.new(@ignore_newer),
      ProcessingPolicyValidator::IgnoreOlderThan.new(@ignore_older),
      @exclude_pattern ? ProcessingPolicyValidator::ExcludePattern.new(@exclude_pattern) : nil,
      @backup_prefix ? ProcessingPolicyValidator::ExcludeBackupedFiles.new(@backup_prefix) : nil,
      ProcessingPolicyValidator::AlreadyProcessed.new(@sincedb),
    ].compact
  end

  # PostProcessors are only run when everything went fine
  # in the processing of the file.
  def post_processors
    [
      @backup_bucket ? PostProcessor::BackupToBucket.new(backup_to_bucket, backup_add_prefix) : nil,
      @backup_dir ? PostProcessor::BackupLocally.new(backup_to_dir) : nil,
      @delete ? PostProcessor::DeleteFromSourceBucket.new : nil,
      PostProcessor::UpdateSinceDB.new(@sincedb) # The last step is to make sure we save our file progress
    ].compact
  end

  def bucket_source
    Aws::S3::Bucket.new(:name => @bucket, :client => client)
  end

  def client
    opts = { :region => @region }
    opts[:credentials] = credentials_options if @access_key_id && @secret_access_key
    Aws::S3::Client.new(opts)
  end

  # TODO: verify all the use cases from the mixin
  def credentials_options
    Aws::Credentials.new(@access_key_id,
                         @secret_access_key,
                         @session_token)
  end
end # class LogStash::Inputs::S3
