# encoding: utf-8
require "aws-sdk"

module LogStash module Inputs class S3
  # The processor represent a workers thread
  class Processor
    def initialize(validator, event_processor, post_processors = [])
      @validator = validator
      @event_processor = event_processor
      @post_processors = post_processors
    end

    def handle(remote_file)
      return if !validator.process?(remote_file)

      begin
        remote_file.download!
        remote_file.each_line do |line|
          emit_event(line, remote_file.metadata)
        end
        post_process(remote_file)
      rescue Aws::S3::Errors::NoSuchKey
        # The object was deleted below our feet, nothing we can do.
        # Should be raised when we try to download the file.
        #
        # We just gracefully cleanup this object, Also take note that 
        # some post procesors will also handle this error if its non fatal for them.
      ensure
        remote_file.cleanup
      end
    end

    private
    attr_reader :event_processor, :post_processors, :validator

    def emit_event(line, metadata)
      @event_processor.process(line, metadata)
    end

    def post_process(remote_file)
      @post_processors.each { |processor| processor.process(remote_file) }
    end
  end
end; end; end
