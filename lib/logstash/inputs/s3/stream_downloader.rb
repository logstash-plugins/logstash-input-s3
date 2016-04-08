# encoding: utf-8
#
module LogStash module Inputs module S3
  class StreamDownloader
    attr_reader :remote_object

    def initialize(remote_object)
      @remote_object
    end

    def stream_to_pipe
      reader, writer = IO.pipe

      Thread.new do
        remote_object.get({ :response_target => writer })
      end

      reader
    end

    def each_line(&block)
      stream_to_pipe.each_line(&block)
    end

    def self.fetcher
      if compressed_gzip?
        CompressedStreamDownloader.new(remote_object)
      else
        StreamDownloader.new(remote_object)
      end
    end
  end

  class CompressedStreamDownloader < StreamDownloader
    def each_line(&block)
      Zlib::GzipReader.new(stream_to_pipe).each_line(&block)
    end
  end
end;end;end
