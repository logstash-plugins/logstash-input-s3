# encoding: utf-8
require "logstash/inputs/base"

require "logstash/inputs/s3/remote_file"
require "stud/interval"

module LogStash module Inputs class S3 < LogStash::Inputs::Base
  class Poller
    DEFAULT_OPTIONS = {
      :polling_interval => 1,
      :use_start_after => false,
      :batch_size => 1000,
      :buckets_options => {},
      :gzip_pattern => "\.gz(ip)?$"
    }

    def initialize(bucket, sincedb, logger, options = {})
      @bucket = bucket
      @sincedb = sincedb
      @logger = logger
      @stopped = false

      @options = DEFAULT_OPTIONS.merge(options)
      @last_key_fetched = nil if @options[:use_start_after]
    end

    def run(&block)
      Stud.interval(options[:polling_interval]) do
        Stud.stop! if stop?

        retrieved_count = retrieve_objects(&block)

        # If we retrieved the amount of the batch size, it means there are still
        # more objects to retrieve so don't wait for the next interval
        redo if retrieved_count == options[:batch_size]
      end
    end

    def stop
      @stopped = true
    end

    private
    attr_reader :options

    def retrieve_objects(&block)
      @logger.debug("Retrieving objects from S3", :options => options)

      retrieved_count = 0
      remote_objects.limit(options[:batch_size]).each do |object|
        return if stop?

        block.call(RemoteFile.new(object, @logger, @options[:gzip_pattern]))

        if options[:use_start_after]
          if @last_key_fetched && (
            (@last_key_fetched <=> object.key) !=
            (last_mtime_fetched <=> object.last_modified)
          )
            @logger.warn("S3 object listing is not consistent. Results may be incomplete or out of order",
                          :last_object => last_object, :previous_object => previous_object)
          end

          @last_key_fetched = object.key
          last_mtime_fetched = object.last_modified
          @logger.debug("Setting last_key_fetched", :last_key_fetched => @last_key_fetched)
        end

        retrieved_count += 1
      end

      retrieved_count
    end

    def remote_objects
      @logger.info("Instantiating S3 object collection", :bucket_listing_options => bucket_listing_options)
      @bucket.objects(bucket_listing_options)
    end

    def bucket_listing_options
      output = {}

      if options[:use_start_after]
        if @last_key_fetched
          @logger.debug("Setting start_after to last_key_fetched",
                        :last_key_fetched => @last_key_fetched)
          output[:start_after] = @last_key_fetched
        elsif (oldest_key = @sincedb.oldest_key)
          @logger.debug("Setting start_after to SinceDB.oldest_key", :oldest_key => oldest_key)
          output[:start_after] = oldest_key
        else
          @logger.debug("use_start_after is enabled but no previous key was found in the " +
                        "sincedb and @last_key_fetched is nil. Starting from the beginning" +
                        " of the bucket.")
        end
      else
        @logger.debug("use_start_after is disabled, relying on last_modified to filter seen objects")
      end

      output.merge(options[:buckets_options])
    end

    def stop?
      @stopped
    end
  end
end;end;end
