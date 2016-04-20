# encoding: utf-8
#
require "stud/temporary"
module LogStash module Inputs class S3
  class StreamDownloader
    attr_reader :remote_object

    def initialize(remote_object)
      @remote_object = remote_object
    end

    def fetch
      writer = StringIO.new
      remote_object.get({ :response_target => writer })
      writer
    end

    def each_line(&block)
      fetch.each_line { |line| block.call(line) }
    end

    def self.fetcher(remote_file)
      if remote_file.compressed_gzip?
        CompressedStreamDownloader.new(remote_file.remote_object)
      else
        StreamDownloader.new(remote_file.remote_object)
      end
    end
  end

  class CompressedStreamDownloader < StreamDownloader
    def each_line(&block)
      Zlib::GzipReader.new(fetch).each_line(&block)
    end
  end
end;end;end
