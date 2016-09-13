# encoding: utf-8
require "logstash/util"
require "logstash/json"
require "thread_safe"
require "concurrent"
require "digest/md5"

module LogStash module Inputs class S3
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
        @last_modified = last_modified.class.name == "Time" ? last_modified : Time.at(last_modified)
        @recorded_at = recorded_at
      end

      def to_hash
        [last_modified.to_i]
      end

      def older?(age)
        Time.now - last_modified >= age
      end
    end

    DEFAULT_OPTIONS = {
      :flush_interval => 1
    }

    def initialize(logger, file, ignore_older, bucket = nil, prefix = nil, options = {})
      @logger = logger
      @file ||= ::File.join(ENV["HOME"], ".sincedb_" + Digest::MD5.hexdigest("#{@bucket}+#{@prefix}"))
      @ignore_older = ignore_older
      @db = ThreadSafe::Hash.new
      @options = DEFAULT_OPTIONS.merge(options) 
      load_database

      @need_sync = Concurrent::AtomicBoolean.new(false)
      @stopped = Concurrent::AtomicBoolean.new(true)

      start_bookkeeping
    end

    def start_bookkeeping 
      @stopped.make_false

      Thread.new do
        LogStash::Util.set_thread_name("<s3|sincedb")
        Stud.interval(@options[:flush_interval]) { periodic_sync }
      end
    end

    def stop?
      @stopped.true?
    end

    def load_database
      return if not ::File.exists?(@file)

      ::File.open(@file).each_line do |line|
        data = LogStash::Json.load(line)
        @db[SinceDBKey.new(*data["key"])] = SinceDBValue.new(*data["value"])
      end
      @logger.info('SinceDB database loaded', :count => @db.count)
      @logger.debug('SinceDB database contents', :db => @db)
    end
   
    def processed?(remote_file)
      @db.include?(SinceDBKey.create_from_remote(remote_file))
    end

    def completed(remote_file)
      @db[SinceDBKey.create_from_remote(remote_file)] = SinceDBValue.new(remote_file.last_modified)
      request_sync
    end

    def serialize
      ::File.open(@file, "w") do |f|
        @db.each do |sincedbkey, sincedbvalue| 
          f.puts(LogStash::Json.dump({ "key" => sincedbkey.to_hash,
                                       "value" => sincedbvalue.to_hash }))
        end
      end
    end

    def clean_old_keys
      @db.each do |sincedbkey, sincedbvalue|
        @db.delete(sincedbkey) if sincedbvalue.older?(@ignore_older)
      end
    end

    def periodic_sync
      clean_old_keys

      if need_sync?
        serialize
        @need_sync.make_false
      end
    end

    def close
      @stopped.make_true
      clean_old_keys
      serialize
    end

    def need_sync?
      @need_sync.value
    end

    def request_sync
      @need_sync.make_true
    end
  end
end end end
