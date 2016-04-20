# encoding: utf-8
require "logstash/inputs/s3/stream_downloader"
require "forwardable"

module LogStash module Inputs class S3
  class RemoteFile
    class NoKeepAlive
      def self.notify!
      end

      def self.complete!
      end
    end

    GZIP_EXTENSION = ".gz"

    extend Forwardable

    attr_reader :remote_object, :metadata

    def_delegators :@remote_object, :key, :content_length, :last_modified

    def initialize(object, keep_alive = NoKeepAlive)
      @remote_object = object
      @keep_alive = keep_alive
    end

    def each_line(&block)
      fetcher.each_line do |line|
        block.call(line, metadata)
        @keep_alive.notify!
      end
      @keep_alive.complete!
    end

    def metadata
      { 
        "s3" =>  {
          "key" => remote_object.key,
          "bucket_name" => remote_object.bucket_name,
          "last_modified" => remote_object.last_modified
        }
      }
    end

    def compressed_gzip?
      # Usually I would use the content_type to retrieve this information.
      # but this require another call to S3 for each download which isn't really optimal.
      # So we will use the filename to do a best guess at the content type.
      ::File.extname(remote_object.key).downcase == GZIP_EXTENSION
    end

    def fetcher
      StreamDownloader.fetcher(self)
    end

    def inspect
      "RemoteFile,##{object_id}: remote_object: #{remote_object.key}"
    end
    alias_method :to_s, :inspect
  end
end;end;end
