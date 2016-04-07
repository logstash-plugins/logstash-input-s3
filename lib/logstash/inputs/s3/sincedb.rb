# encoding: utf-8
require "logstash/inputs/base"

require "logstash/util"
require "logstash/json"
require "thread_safe"
require "concurrent"
require "time"

module LogStash module Inputs class S3 < LogStash::Inputs::Base
  class SinceDB
    SinceDBKey = Struct.new(:key, :etag, :bucket_name) do
      def ==(other)
        other.key == key && other.etag == etag
      end

      def self.create_from_remote(remote_file)
        SinceDBKey.new(remote_file.key, remote_file.etag, remote_file.bucket_name)
      end

      def to_hash
        [
          key,
          etag,
          bucket_name
        ]
      end

      # TODO CHECK IF WE NEED #HASH
    end

    class SinceDBValue
      attr_reader :last_modified, :recorded_at

      def initialize(last_modified, recorded_at = Time.now)
        begin
          Time.now - last_modified
        rescue TypeError => e
          raise e, "last_modified must be a Time object, got #{last_modified.class} (#{last_modified.inspect})"
        end

        @last_modified = last_modified
        @recorded_at = recorded_at
      end

      def to_hash
        [recorded_at]
      end

      def older?(age)
        Time.now - last_modified >= age
      end
    end

    DEFAULT_OPTIONS = {
      :sincedb_expire_secs => 120
    }

    def initialize(file, ignore_older, logger, options = {})
      @file = file
      @ignore_older = ignore_older
      @logger = logger
      @options = DEFAULT_OPTIONS.merge(options)

      @db = ThreadSafe::Hash.new
      load_database

      @need_sync = Concurrent::AtomicBoolean.new(false)
      @stopped = Concurrent::AtomicBoolean.new(true)

      start_bookkeeping
    end

    def close
      @stopped.make_true
      clean_old_keys
      serialize
    end

    def completed(remote_file)
      @db[SinceDBKey.create_from_remote(remote_file)] = SinceDBValue.new(remote_file.last_modified)
      request_sync
    end

    def oldest_key
      return if @db.empty?
      @db.min_by { |_, value| value.last_modified }.first.key
    end

    def processed?(remote_file)
      @db.include?(SinceDBKey.create_from_remote(remote_file))
    end

    def reseed(remote_file)
      @db.clear
      completed(remote_file)
    end

    private
    attr_reader :options

    def start_bookkeeping 
      @stopped.make_false

      Thread.new do
        LogStash::Util.set_thread_name("S3 input, sincedb periodic fsync")
        Stud.interval(1) { periodic_sync }
      end
    end

    def stop?
      @stopped.true?
    end

    def load_database
      if !::File.exists?(@file)
        @logger.debug("Not loading sincedb since none exists at specified location", :location => @file)
        return
      end

      @logger.debug("Loading sincedb", :location => @file)

      ::File.open(@file).each_line do |line|
        data = JSON.parse(line)
        @db[SinceDBKey.new(*data["key"])] = SinceDBValue.new(Time.parse(*data["value"]))
      end
    end

    def newest_entry
      return if @db.empty?
      @db.max_by { |_, value| value.last_modified }.last
    end

    def serialize
      @logger.debug("Writing sincedb", :location => @file)
      @db.each do |sincedbkey, sincedbvalue| 
        ::File.open(@file, "a") do |f|
          f.puts(LogStash::Json.dump({ "key" => sincedbkey.to_hash,
                                        "value" => sincedbvalue.to_hash }))
        end
      end
    end

    def clean_old_keys
      @logger.debug("Cleaning sincedb keys older than #{@ignore_older} seconds")
      @db.each do |sincedbkey, sincedbvalue|
        @db.delete(sincedbkey) if sincedbvalue.older?(@ignore_older)
      end

      return unless @db.size > 1

      newest_last_modified = newest_entry.last_modified
      @logger.debug(
        "Cleaning sincedb keys older than newest_last_modified - SINCEDB_EXPIRE_SECS" +
        " seconds (#{newest_last_modified} - #{options[:sincedb_expire_secs]} = " +
        "#{newest_last_modified - options[:sincedb_expire_secs]})")
      @db.delete_if do |_sincedbkey, sincedbvalue|
        newest_last_modified - sincedbvalue.last_modified > options[:sincedb_expire_secs]
      end
    end

    def periodic_sync
      clean_old_keys

      if need_sync?
        serialize
        @need_sync.make_false
      end
    end

    def need_sync?
      @need_sync.value
    end

    def request_sync
      @need_sync.make_true
    end
  end
end end end
