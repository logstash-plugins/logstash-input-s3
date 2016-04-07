# encoding: utf-8
require "logstash/inputs/base"

require "aws-sdk"

module LogStash module Inputs class S3 < LogStash::Inputs::Base
  # The processor represent a workers thread
  class Processor
    def initialize(validator, event_processor, logger, post_processors = [])
      @validator = validator
      @event_processor = event_processor
      @logger = logger
      @post_processors = post_processors
    end

    def handle(remote_file)
      @logger.debug("Validating remote file to see if we should download it", :remote_file => remote_file)
      return if !validator.process?(remote_file)
      @logger.debug("Remote file passed validation. Downloading data.", :remote_file => remote_file)

      remote_file.download!

      @logger.debug("File downloaded. Emitting events.", :remote_file => remote_file)
      remote_file.each_line do |line|
        emit_event(line, remote_file.metadata, remote_file.data)
      end
      post_process(remote_file)
      remote_file.cleanup
    end

    private
    attr_reader :event_processor, :post_processors, :validator

    def emit_event(line, metadata, object)
      @event_processor.process(line, metadata, object)
    end

    def post_process(remote_file)
      @logger.debug("Post processing remote file", :remote_file => remote_file)

      @post_processors.each do |processor|
        @logger.debug("Running post processor", :processor => processor.class)
        processor.process(remote_file)
      end
    end
  end
end; end; end
