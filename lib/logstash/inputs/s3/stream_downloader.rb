# encoding: utf-8
require "logstash/inputs/base"
require "stud/temporary"

module LogStash module Inputs class S3 < LogStash::Inputs::Base
  class StreamDownloader
    def initialize(logger, remote_object, writer = StringIO.new)
      @logger = logger
      @writer = writer
      @remote_object = remote_object
    end

    def fetch
      @logger.debug("Downloading remote file", :remote_object_key => @remote_object.key)
      @remote_object.get({ :response_target => @writer })
      # @writer.rewind
      @writer
    end

    def self.fetcher(remote_file, logger)
      if remote_file.compressed_gzip?
        return CompressedStreamDownloader.new(logger, remote_file.remote_object, remote_file.download_to)
      end

      StreamDownloader.new(logger, remote_file.remote_object, remote_file.download_to)
    end
  end

  class CompressedStreamDownloader < StreamDownloader
    def fetch
      original_file = super
      @logger.debug("Decompressing gzip file", :remote_object_key => @remote_object.key)
      Zlib::GzipReader.new(original_file)
    end
  end
end;end;end
