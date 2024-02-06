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
      compressed_file_io_object = super
      @logger.debug("Decompressing gzip file", :remote_object_key => @remote_object.key)
      decompress_io_object(compressed_file_io_object)
    end

    private

    def decompress_io_object(io_object)
      # Shelling out is necessary here until logstash-oss is using JRuby 9.4 which includes
      # the Zlib::GzipReader.zcat method
      output = ''
      IO.popen('zcat', 'r+') do |zcat|
        writer_thread = Thread.new do
          while chunk = io_object.read(65536)
            zcat.write(chunk)
          end
          zcat.close_write
        end

        output = zcat.read
        writer_thread.join
      end

      output
    end
  end
end;end;end
