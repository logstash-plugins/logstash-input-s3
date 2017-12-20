# encoding: utf-8
require "logstash/inputs/base"

require "logstash/util"
require "logstash/json"
require "thread_safe"
require "concurrent"

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
      :flush_interval => 1
    }

    def initialize(file, ignore_older, options = {})
      @file = file
      @ignore_older = ignore_older
      @db = ThreadSafe::Hash.new
      load_database

      @need_sync = Concurrent::AtomicBoolean.new(false)
      @stopped = Concurrent::AtomicBoolean.new(true)

      start_bookkeeping
    end

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
      return !::File.exists?(@file)

      ::File.open(@file).each_line do |line|
        data = LogStash::Json.load(line)
        @db[SinceDBValue.new(*data["key"])] = SinceDBKey.new(*data["value"])
      end
    end
    
    def processed?(remote_file)
      @db.include?(SinceDBKey.create_from_remote(remote_file))
    end

    def completed(remote_file)
      @db[SinceDBKey.create_from_remote(remote_file)] = SinceDBValue.new(remote_file.last_modified)
      request_sync
    end

    def serialize
      @db.each do |sincedbkey, sincedbvalue| 
        ::File.open(@file, "a") do |f|
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
