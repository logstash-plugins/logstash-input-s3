# encoding: utf-8
require "logstash/inputs/s3/processor"
require "logstash/util"
require "thread"
require "concurrent"

module LogStash module Inputs class S3
  # This class Manage the processing threads and share the same processor instance
  # The event processor and the post processors need to be threadsafe
  class ProcessorManager
    java_import java.util.concurrent.SynchronousQueue
    java_import java.util.concurrent.TimeUnit

    DEFAULT_OPTIONS = { :processors_count => 5 }

    TIMEOUT_MS = 150 # milliseconds, use for the SynchronousQueue

    attr_reader :processors_count

    def initialize(options = {})
      options = DEFAULT_OPTIONS.merge(options)
      @processor = options[:processor]
      @processors_count = options[:processors_count]

      @available_processors = []

      @work_queue = java.util.concurrent.SynchronousQueue.new

      @stopped = Concurrent::AtomicBoolean.new(false)
    end

    def enqueue_work(remote_file)
      # block the main thread until we are able to enqueue the workers
      # but allow a gracefull shutdown.
      success = false

      while !success && !stop?
        success = @work_queue.offer(remote_file, TIMEOUT_MS, TimeUnit::MILLISECONDS)
      end
    end

    def start
      processors_count.times do |worker_id|
        @available_processors << Thread.new do
          start_processor(worker_id)
        end
      end
    end

    def stop
      @stopped.make_true
      @available_processors.join
    end

    def start_processor(worker_id)
      loop do
        break if stop?

        if remote_file = @work_queue.poll(TIMEOUT_MS, TimeUnit::MILLISECONDS)
          LogStash::Util.set_thread_name("[S3 Input Processor - #{worker_id}/#{processors_count}] Working on: #{remote_file.bucket_name}/#{remote_file.key} size: #{remote_file.content_length}")
          @processor.handle(remote_file)
        end
        LogStash::Util.set_thread_name("[S3 Input Processor - #{worker_id}/#{processors_count}] Waiting for work")
      end
    end

    private
    def stop?
      @stopped.value
    end
  end
end; end; end; 
