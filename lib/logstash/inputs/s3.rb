# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "logstash/plugin_mixins/aws_config"
require "time"
require "tmpdir"
require "stud/interval"
require "stud/temporary"
require "aws-sdk"
require "logstash/inputs/s3/patch"

Aws.eager_autoload!
# New 

# Stream events from files from a S3 bucket.
#
# Each line from each file generates an event.
# Files ending in `.gz` are handled as gzip'ed files.
class LogStash::Inputs::S3 < LogStash::Inputs::Base
  include LogStash::PluginMixins::AwsConfig::V2
  # include LogStash::PluginMixins::AwsConfig
  require "logstash/inputs/s3/poller"
  require "logstash/inputs/s3/processor"
  require "logstash/inputs/s3/processor_manager"
  require "logstash/inputs/s3/processing_policy_validator"

  config_name "s3"

  default :codec, "plain"

  # The name of the S3 bucket.
  config :bucket, :validate => :string, :required => true

  # If specified, the prefix of filenames in the bucket must match (not a regexp)
  config :prefix, :validate => :string, :default => nil

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

  # Ruby style regexp of keys to exclude from the bucket
  config :exclude_pattern, :validate => :string, :default => nil

  # Set the directory where logstash will store the tmp files before processing them.
  # default to the current OS temporary directory in linux /tmp/logstash
  config :temporary_directory, :validate => :string, :default => File.join(Dir.tmpdir, "logstash")

  public
  def register
    # TODO: Bucket, Access validation
    @poller = Poller.new(client.bucket[@bucket])
    
    # TODO: Bucket, Write validation
  end

  def run(queue)
    processor = processor
    @manager = ProcessorManager.new

    @poller.run do |remote_object|
      manager.enqueue_work(remote_object)
    end
  end

  def stop
    # Gracefully stop the polling of new S3 documents
    # the manager will stop consuming events from the queue, but will block untill
    # all the processors thread are still doing work.
    @poller.stop
    @manager.stop
  end
  
  private
  def processor
    Processor.new(OpenStruct.new)
  end

  def client
    Aws::S3.new(:credentials => credentials_options)
  end

  # TODO: verify all the use cases from the mixin
  def credentials_options
    Aws::Credentials.new(@access_key_id,
                         @secret_access_key,
                         @session_token)
  end
end
