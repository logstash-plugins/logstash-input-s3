# encoding: utf-8
require "logstash/inputs/s3/stream_downloader"
require "forwardable"

module LogStash module Inputs class S3
  class RemoteFile
    FILE_MODE = "w+b"

    extend Forwardable

    attr_reader :remote_object, :metadata, :file
    attr_accessor :download_to_path

    def_delegators :@remote_object, :key, :content_length, :last_modified, :etag, :bucket_name

    def initialize(logger, object, each_line = true)
      @logger = logger
      @remote_object = object
      @each_line = each_line
      @downloaded = false
      download_to_path = Dir.tmpdir
    end

    def download!
      @file = StreamDownloader.get(self)
      @downloaded = true
    end

    def download_to
      # Lazy create FD
      @download_to ||= begin
                         FileUtils.mkdir_p(download_to_path)
                         ::File.open(::File.join(download_to_path, ::File.basename(key)), FILE_MODE)
                       end
    end

    def each_line(&block)
      # extract_metadata_from_file
      # seek for cloudfront metadata
      if @each_line
        @file.each_line do |line|
          block.call(line, metadata)
        end
      else
        block.call(@file.read, metadata)
      end
    end

    def download_finished?
      @downloaded
    end

    def metadata
      { 
        "s3" =>  {
          "key" => key,
          "bucket_name" => bucket_name,
          "last_modified" => last_modified
        }
      }
    end

    def cleanup
      if @download_to
        @download_to.close unless @download_to.closed?
        ::File.delete(@download_to.path)
      end
    end

    def inspect
      "RemoteFile,##{object_id}: remote_object: #{remote_object.key}"
    end
    alias_method :to_s, :inspect
  end
end;end;end
