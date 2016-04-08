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

    CONTENT_TYPE_GZIP = "application/gzip"

    extend Forwardable

    attr_reader :remote_object, :metadata

    def_delegators :@remote_object, :key, :content_length

    def initialize(object, keep_alive = NoKeepAlive)
      @remote_object = object
      @keep_alive = keep_alive
      @metadata = retrieve_metadata
    end

    def each_line(&block)
      fetcher.each_line do |line|
        block.call(line)
        @keep_alive.notify!
      end
      @keep_alive.complete!
    end

    def retrieve_metadata
      { :key => remote_object.key }
    end

    def compressed_gzip?
      remote_object.content_type == CONTENT_TYPE_GZIP
    end

    def fetcher
      StreamDownloader.fetcher(self)
    end
  end
end;end;end
