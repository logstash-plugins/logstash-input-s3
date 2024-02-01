# encoding: utf-8
require "logstash/inputs/base"

require "logstash/inputs/s3/processor"
require "logstash/util"
require "thread"
require "concurrent"

module LogStash module Inputs class S3 < LogStash::Inputs::Base
  # This class Manage the processing threads and share the same processor instance
  # The event processor and the post processors need to be threadsafe
  class ProcessorManager
    java_import java.util.concurrent.SynchronousQueue
    java_import java.util.concurrent.TimeUnit

    DEFAULT_OPTIONS = {
      :processors_count => 5,
      :broken_pipe_retries => 10
    }

    TIMEOUT_MS = 150 # milliseconds, use for the SynchronousQueue

    attr_reader :processors_count

    def initialize(logger, options = {})
      @logger = logger
      options = DEFAULT_OPTIONS.merge(options)
      @processor = options[:processor]
      @broken_pipe_retries = options[:broken_pipe_retries]
      @processors_count = options[:processors_count]

      @available_processors = []

      @work_queue = java.util.concurrent.SynchronousQueue.new

      @stopped = Concurrent::AtomicBoolean.new(false)
    end

    def enqueue_work(remote_file)
      @logger.debug("Enqueuing work", :remote_file => remote_file)

      # block the main thread until we are able to enqueue the workers
      # but allow a gracefull shutdown.
      success = false

      while !success && !stop?
        success = @work_queue.offer(remote_file, TIMEOUT_MS, TimeUnit::MILLISECONDS)
      end
    end

    def start
      @logger.debug("Starting processors", :processors_count => processors_count)
      processors_count.times do |worker_id|
        @available_processors << Thread.new do
          start_processor(worker_id)
        end
      end
    end

    def stop
      @logger.debug("Stopping processors")
      @stopped.make_true
      @available_processors.join
    end

    def start_processor(worker_id)
      @logger.debug("Starting processor", :worker_id => worker_id)
      loop do
        break if stop?

        # This can be useful for debugging but it is extremely verbose
        # @logger.debug("Waiting for new work", :worker_id => worker_id)
        if remote_file = @work_queue.poll(TIMEOUT_MS, TimeUnit::MILLISECONDS)
          @logger.debug("New work received", :worker_id => worker_id, :remote_file => remote_file)
          LogStash::Util.set_thread_name("[S3 Input Processor - #{worker_id}/#{processors_count}] Working on: #{remote_file.bucket_name}/#{remote_file.key} size: #{remote_file.content_length}")

          tries = 0
          begin
            @processor.handle(remote_file)
          rescue IOError => e
            @logger.error(
              "IOError when processing remote file. Skipping for now (But not adding to SinceDB).",
              :remote_file => remote_file,
              :exception => e
            )
          rescue Errno::EPIPE => e
            @logger.error(
              "Broken pipe when processing remote file",
              :remote_file => remote_file,
              :exception => e
            )

            raise e if (tries += 1) == @broken_pipe_retries

            sleep 1
            retry
          rescue Aws::S3::Errors::NoSuchKey
            @logger.debug(
              "File not found on S3 (probably already handled by another worker)",
              :remote_file => remote_file,
              :worker_id => worker_id
            )
            # This mean the file on S3 were removed under our current operation,
            # we cannot do anything about it, the file should not be available on the next pooling
          end
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
