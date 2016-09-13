# encoding: utf-8
require "logstash/inputs/s3/remote_file"
require "stud/interval"

module LogStash module Inputs class S3
  class Poller
    DEFAULT_OPTIONS = {
      :polling_interval => 1,
      :each_line => true,
    }

    def initialize(logger, bucket, sincedb, prefix = nil, options = {})
      @logger = logger
      @bucket = bucket
      @sincedb = sincedb
      @prefix = prefix
      @stopped = false
      @options = DEFAULT_OPTIONS.merge(options)
    end

    def run(&block)
      Stud.interval(options[:polling_interval]) do
        Stud.stop! if stop?
        retrieve_objects(&block)
      end
    end

    def stop
      @stopped = true
    end

    private
    attr_reader :options

    def retrieve_objects(&block)
      remote_objects.each do |object|
        return if stop?
        block.call(RemoteFile.new(@logger, object, @options[:each_line]))
      end
    end

    def remote_objects
      @bucket.objects(bucket_listing_options)
    end

    def bucket_listing_options
      { :prefix => @prefix }
    end

    def stop?
      @stopped
    end
  end
end;end;end
