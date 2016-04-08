# encoding: utf-8
module LogStash module Inputs module S3
  # The processor represent a workers thread
  class Processor
    def initialize(event_processor, post_processors = [])
      @event_processor = event_processor
      @post_processor = post_processors
    end

    def handle(remote_file)
      # Extract of metadata need to be handled in the `RemoteFile`
      remote_file.each_line do |line, metadata|
        emit_event(line, metadata)
      end

      post_process(remote_file)
    end

    private
    attr_reader :event_processor, :post_processors

    def emit_event(line, metadata)
      event_processor.process(line, metadata)
    end

    def post_process(remote_file)
      post_processors.each { |processor| processor.post_process(remote_file) }
    end
  end
end; end; end
