# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "logstash/plugin_mixins/aws_config"
require "time"
require "tmpdir"
require "stud/interval"
require "stud/temporary"
require "aws-sdk-resources"

# Stream events from files from a S3 bucket.
#
# Each line from each file generates an event.
# Files ending in `.gz` are handled as gzip'ed files.
class LogStash::Inputs::S3 < LogStash::Inputs::Base
  include LogStash::PluginMixins::AwsConfig::V2

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

  # If specified, the prefix of filenames in the bucket must match (not a regexp)
  config :prefix, :validate => :string, :default => nil

  # Where to write the since database (keeps track of the date
  # the last handled file was added to S3). The default will write
  # sincedb files to some path matching "$HOME/.sincedb*"
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

  # Ruby style regexp of keys to exclude from the bucket
  config :exclude_pattern, :validate => :string, :default => nil

  # Set the directory where logstash will store the tmp files before processing them.
  # default to the current OS temporary directory in linux /tmp/logstash
  config :temporary_directory, :validate => :string, :default => File.join(Dir.tmpdir, "logstash")

  # When the S3 input discovers a file that was last modified
  # before the specified timespan in seconds, the file is ignored.
  # After it's discovery, if an ignored file is modified it is no
  # longer ignored and any new data is read. The default is 24 hours.
  config :ignore_older, :validate => :number, :default => 24 * 60 * 60

  # Allow disabling line processing on input files. If disabled,
  # codecs will be fed the entire file at once, rather than
  # using the default line-based processing method.
  config :has_lines, :validate => :boolean, :default => true

  # Force GZIP decompression even if the S3 object's Content-Encoding
  # is not set to gzip.
  config :force_gzip, :validate => :boolean, :default => false

  # Set the number of parallel processing threads. This determines
  # the number of files that will be downloaded and processed in parallel
  config :processors, :validate => :number, :default => 4

  # Set the depth within the top-level prefix to parallelize object polling
  # Polling within individual prefixes is not yet parallelized, but this does
  # at least let you 'tail' multiple subdirectories
  config :depth, :validate => :number, :default => 0

  # Prefix delimiter for depth discovery
  config :delimiter, :validate => :string, :default => '/'

  public
  def initialize(options = {})
    super

    @sincedb = SinceDB.new(@logger, @sincedb_path, @bucket, @prefix)
  end

  def register
    FileUtils.mkdir_p(@temporary_directory)

    # TODO: Bucket, Access validation
    # TODO: Bucket, Write validation
  end

  def run(queue)
    @poller = Poller.new(@logger, bucket_source, @sincedb, prefixes, { :polling_interval => @interval, :each_line => @has_lines, :force_gzip => @force_gzip })

    validator = ProcessingPolicyValidator.new(@logger, *processing_policies)

    # Each processor is run into his own thread.
    processor = Processor.new(@logger, validator, EventProcessor.new(@logger, @codec, queue, self), post_processors)

    @manager = ProcessorManager.new(@logger, { :processor => processor,
                                               :processors_count => @processors})
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
  def processing_policies
    [
      ProcessingPolicyValidator::SkipEndingDirectory,
      ProcessingPolicyValidator::SkipEmptyFile,
      @ignore_older > 0 ? ProcessingPolicyValidator::IgnoreOlderThan.new(@ignore_older) : nil,
      @exclude_pattern ? ProcessingPolicyValidator::ExcludePattern.new(@exclude_pattern) : nil,
      @backup_prefix ? ProcessingPolicyValidator::ExcludeBackupedFiles.new(@backup_prefix) : nil,
      ProcessingPolicyValidator::AlreadyProcessed.new(@sincedb), # Mark file as in process last (if it passed other policies)
    ].compact
  end

  # PostProcessors are only run when everything went fine
  # in the processing of the file.
  def post_processors
    [
      PostProcessor::UpdateSinceDB.new(@sincedb), # Mark file as complete first to avoid duplicate documents on unclean shutdown
      @backup_bucket ? PostProcessor::BackupToBucket.new(backup_to_bucket, backup_add_prefix) : nil,
      @backup_dir ? PostProcessor::BackupLocally.new(backup_to_dir) : nil,
      @delete ? PostProcessor::DeleteFromSourceBucket.new : nil,
    ].compact
  end

  def bucket_source
    @s3 ||= Aws::S3::Resource.new(aws_options_hash).bucket(@bucket)
  end

  def prefixes(prefix = @prefix, depth = @depth)
    logger.debug('Searching for common prefixes', :bucket => @bucket, :delimiter => @delimiter, :prefix => prefix, :depth => depth)
    return [prefix] if depth == 0

    object = bucket_source.client.list_objects_v2({:bucket => @bucket, :delimiter => @delimiter, :prefix => prefix})
    return [prefix] if object.common_prefixes.count == 0

    object.common_prefixes.delete_if {|p| p.prefix =~ Regexp.new(@exclude_pattern)} if @exclude_pattern
    object.common_prefixes.map {|p| prefixes(p.prefix, depth - 1)} .flatten
  end

end
