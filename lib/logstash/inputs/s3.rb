# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "logstash/plugin_mixins/aws_config"
require "time"
require "tmpdir"
require "stud/interval"
require "stud/temporary"
require "aws-sdk"


# New 

# Stream events from files from a S3 bucket.
#
# Each line from each file generates an event.
# Files ending in `.gz` are handled as gzip'ed files.
class LogStash::Inputs::S3 < LogStash::Inputs::Base
  include LogStash::PluginMixins::AwsConfig

  require "logstash/inputs/s3/poller"
  require "logstash/inputs/s3/processor"
  require "logstash/inputs/s3/processor_manager"
  require "logstash/inputs/s3/processing_policy_validator"
  require "logstash/inputs/s3/event_processor"
  require "logstash/inputs/s3/sincedb"
  require "logstash/inputs/s3/post_processor"

  config_name "s3"

  default :codec, "plain"

  # DEPRECATED: The credentials of the AWS account used to access the bucket.
  # Credentials can be specified:
  # - As an ["id","secret"] array
  # - As a path to a file containing AWS_ACCESS_KEY_ID=... and AWS_SECRET_ACCESS_KEY=...
  # - In the environment, if not set (using variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY)
  config :credentials, :validate => :array, :default => [], :deprecated => "This only exists to be backwards compatible. This plugin now uses the AwsConfig from PluginMixins"

  # The name of the S3 bucket.
  config :bucket, :validate => :string, :required => true

  # The AWS region for your bucket.
  config :region_endpoint, :validate => ["us-east-1", "us-west-1", "us-west-2",
                                "eu-west-1", "ap-southeast-1", "ap-southeast-2",
                                "ap-northeast-1", "sa-east-1", "us-gov-west-1"], :deprecated => "This only exists to be backwards compatible. This plugin now uses the AwsConfig from PluginMixins"

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

  public
  def initialize(options = {})
    super
  end

  def register
    # TODO: Bucket, Access validation
    # TODO: Bucket, Write validation
  end

  def run(queue)
    @poller = Poller.new(bucket_source)

    processor = Processor.new(EventProcessor.new(self, queue), post_processors)
    @manager = ProcessorManager.new({ :processor => processor,
                                      :processors_count => 5 })
    @manager.start

    validator = ProcessingPolicyValidator.new(*processing_policies)

    # The poller get all the new files from the S3 buckets,
    # all the actual work is done in a processor which will handle the following
    # tasks:
    #  - Downloading
    #  - Uncompressing
    #  - Reading and queue
    #  - Bookeeping
    #  - Backup strategy
    @poller.run do |remote_file|
      @manager.enqueue_work(remote_file) if validator.process?(remote_file)
    end
  end

  def stop
    # Gracefully stop the polling of new S3 documents
    # the manager will stop consuming events from the queue, but will block untill
    # all the processors thread are still doing work unless with force quit logstash
    @poller.stop unless @poller.nil?
    @manager.stop unless @manager.nil?
  end
  
  private
  def processing_policies
    [ProcessingPolicyValidator::AlreadyProcessed.new(sincedb)]
  end

  def sincedb
    # Upgrade?
    @sincedb ||= SinceDB.new
  end

  # PostProcessors are only run when everything went fine
  # in the processing of the file.
  def post_processors
    # Backup Locally
    # Backup to an another bucket
    [PostProcessor::UpdateSinceDB.new(sincedb)]
  end

  def bucket_source
    Aws::S3::Bucket.new(:name => @bucket, :client  => client)
  end

  def client
    # TODO HARDCODED
    Aws::S3::Client.new(:region => "us-east-1",
                        :credentials => credentials_options)
  end

  # TODO: verify all the use cases from the mixin
  def credentials_options
    Aws::Credentials.new(@access_key_id,
                         @secret_access_key,
                         @session_token)
  end
end
