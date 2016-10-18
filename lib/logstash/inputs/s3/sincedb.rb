# encoding: utf-8
require "logstash/util"
require "logstash/json"
require "thread_safe"
require "concurrent"
require "digest/md5"

module LogStash module Inputs class S3
  class SinceDB
    SinceDBEntry = Struct.new(:key, :completed) do
      def ==(other)
        other.key == key
      end

      def self.create_from_remote(remote_file)
        SinceDBEntry.new(remote_file.key, false)
      end
    end

    DEFAULT_OPTIONS = {
      :flush_interval => 1
    }

    def initialize(logger, file = nil, bucket = nil, prefix = nil, options = {})
      @logger = logger
      @bucket = bucket
      @prefix = prefix
      @file = file || ::File.join(ENV["HOME"], ".sincedb-marker_" + Digest::MD5.hexdigest("#{@bucket}+#{@prefix}"))
      @db = ThreadSafe::Hash.new {|hash, key| hash[key] = ThreadSafe::Array.new }
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
      @file_io = ::File.open(@file, 'ab+')
      @file_io.each_line do |line|
        data = LogStash::Json.load(line)
        @db[data[0]].push(SinceDBEntry.new(data[1], data[2])) if data.count == 3
      end
      @logger.info('SinceDB database loaded', :file => @file, :count => @db.values.flatten.count)
    end
   
    def marker(prefix)
      first_completed = @db[prefix].take_while {|entry| entry.completed} .first
      @logger.info('SinceDB Object Marker', :prefix => prefix, :entry => first_completed)
      first_completed.key if first_completed
    end
 
    def processed?(remote_file)
      # Need to do the find/return within a block to hold the array lock
      # looking up an index and then operating on it releases the lock
      # between steps, and can result in the contents changing unexpectedly
      entry = SinceDBEntry.create_from_remote(remote_file)
      prefix = remote_file.prefix
      @db[prefix].each do |e|
        return e.completed if e == entry
      end

      @db[prefix].push(entry)
      request_sync
      false
    end

    def completed(remote_file)
      # Same concern as above - need to find and operate on entry in one operation
      entry = SinceDBEntry.create_from_remote(remote_file)
      prefix = remote_file.prefix
      @db[prefix].map! do |e|
        e.completed = true if e == entry
        e
      end
      request_sync
    end

    def serialize
      @file_io.rewind
      @file_io.truncate(0)
      @db.each_key do |prefix|
        @db[prefix].each do |entry|
          @file_io.puts(LogStash::Json.dump([prefix, entry.key, entry.completed]))
        end
      end
      @file_io.fsync
    end

    def clean_old_keys
      # This should be safe to do two-step, since other threads can only append items
      # to the array, and we are operating on the begining.
      @db.each_key do |prefix|
        next if @db[prefix].count <= 1
        completed_count = @db[prefix].take_while {|e| e.completed} .count
        @db[prefix].slice!(0, completed_count - 1)
      end
    end

    def periodic_sync
      return if stop?
      if need_sync?
        clean_old_keys
        serialize
        @need_sync.make_false
      end
    end

    def close
      @stopped.make_true
      clean_old_keys
      serialize
      if not @file_io.nil?
        @file_io.close
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
