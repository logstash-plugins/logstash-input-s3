# encoding: utf-8
require "logstash/inputs/base"

require "logstash/inputs/s3/stream_downloader"
require "forwardable"

module LogStash module Inputs class S3 < LogStash::Inputs::Base
  class RemoteFile
    class NoKeepAlive
      def self.notify!
      end

      def self.complete!
      end
    end

    FILE_MODE = "w+b"

    extend Forwardable

    attr_reader :remote_object, :metadata, :file
    attr_accessor :download_to_path

    def_delegators :@remote_object, :key, :content_length, :last_modified, :etag, :bucket_name, :data

    def initialize(object, logger, gzip_pattern, keep_alive = NoKeepAlive)
      @remote_object = object
      @logger = logger
      @gzip_pattern = gzip_pattern
      @keep_alive = keep_alive

      @downloaded = false
      download_to_path = Dir.tmpdir
    end

    def download!
      @file = StreamDownloader.fetcher(self, @logger).fetch
      @downloaded = true
    end

    def download_to
      # Lazy create FD
      @download_to ||= begin
                         full_local_path = ::File.join(download_to_path, key)
                         local_dirname = ::File.dirname(full_local_path)
                         FileUtils.mkdir_p(local_dirname)
                         @logger.debug("Opening file for download", :remote_object => remote_object, :local_path => full_local_path)
                         ::File.open(full_local_path, FILE_MODE)
                       end
    end

    def each_line(&block)
      # extract_metadata_from_file
      # seek for cloudfront metadata
      @file.each_line do |line|
        block.call(line, metadata)
        @keep_alive.notify!
      end

      @keep_alive.complete!
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
        ::File.delete(@download_to.path) if ::File.exist?(@download_to.path)
      end
    end

    def compressed_gzip?
      # Usually I would use the content_type to retrieve this information.
      # but this require another call to S3 for each download which isn't really optimal.
      # So we will use the filename to do a best guess at the content type.
      ::File.extname(remote_object.key).downcase =~ Regexp.new(@gzip_pattern)
    end

    def inspect
      "RemoteFile,##{object_id}: remote_object: #{remote_object.key}"
    end
    alias_method :to_s, :inspect
  end
end;end;end
