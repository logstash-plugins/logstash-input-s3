# encoding: utf-8
require "logstash/inputs/s3/remote_file"
require "stud/interval"

module LogStash module Inputs class S3
  class Poller
    DEFAULT_OPTIONS = {
      :polling_interval => 1,
      :each_line => true,
      :force_gzip => false,
    }

    def initialize(logger, bucket, sincedb, prefixes = [], options = {})
      @logger = logger
      @bucket = bucket
      @sincedb = sincedb
      @prefixes = prefixes
      @stopped = false
      @options = DEFAULT_OPTIONS.merge(options)
      @logger.info('Poller watching S3', :bucket => @bucket, :prefixes => @prefixes)
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
      @prefixes.each do |prefix|
        remote_objects(prefix).each do |object|
          return if stop?
          block.call(RemoteFile.new(@logger, object, prefix, @options[:each_line], @options[:force_gzip]))
        end
      end
    end

    def remote_objects(prefix)
      @bucket.objects({ :prefix => prefix, :marker => @sincedb.marker(prefix) })
    end

    def stop?
      @stopped
    end
  end
end;end;end
