# encoding: utf-8
require "logstash/inputs/s3/processor"
require "thread"
require "concurrent"

module LogStash module Inputs class S3
  # This class Manage the processing threads and share the same processor instance
  # The event processor and the post processors need to be threadsafe
  class ProcessorManager
    java_import java.util.concurrent.SynchronousQueue
    java_import java.util.concurrent.TimeUnit

    DEFAULT_OPTIONS = {
      :processors_count => 5
    }

    TIMEOUT_MS = 150 # milliseconds, use for the SynchronousQueue

    attr_reader :processors_count

    def initialize(options = {})
      @options = DEFAULT_OPTIONS.merge(options)
      @processor = options[:processor]
      @processors_count = options[:processors_count]

      @available_processors = []

      @work_queue = java.util.concurrent.SynchronousQueue.new

      @stopped = Atomic::Boolean.new(false)
    end

    def enqueue_work(remote_file)
      @queue.offer(obj, TIMEOUT_MS, TimeUnit::MILLISECONDS)
    end

    def start
      @stopped.make_true
      processors_count.times { |w| @available_processors << Thread.new { start_processor } }
    end

    def stop
      @stopped.make_false
      @available_processors.join
    end

    def start_processor
      loop do
        break if stop?

        if remote_file = @queue.poll(TIMEOUT_MS, TimeUnit::MILLISECONDS)
          @processor.handle(remote_file)
        end
      end
    end

    private
    def stop?
      @stopped.value
    end
  end
end; end; end; 
