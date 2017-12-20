# encoding: utf-8
require "logstash/inputs/base"
require "stud/temporary"

module LogStash module Inputs class S3 < LogStash::Inputs::Base
  class StreamDownloader
    def initialize(remote_object, writer = StringIO.new)
      @writer = writer
      @remote_object = remote_object
    end

    def fetch
      @remote_object.get({ :response_target => @writer })
      # @writer.rewind
      @writer
    end

    def self.fetcher(remote_file)
      if remote_file.compressed_gzip?
        CompressedStreamDownloader.new(remote_file.remote_object, remote_file.download_to)
      else
        StreamDownloader.new(remote_file.remote_object, remote_file.download_to)
      end
    end
  end

  class CompressedStreamDownloader < StreamDownloader
    def fetch
      original_file = super
      Zlib::GzipReader.new(original_file)
    end
  end
end;end;end
