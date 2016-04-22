# encoding: utf-8
module LogStash module Inputs class S3
  # The processor represent a workers thread
  class Processor
    def initialize(event_processor, post_processors = [])
      @event_processor = event_processor
      @post_processors = post_processors
    end

    def handle(remote_file)
      begin
        remote_file.download!
        remote_file.each_line do |line|
          emit_event(line, remote_file.metadata)
        end
        post_process(remote_file)
      rescue Aws::S3::Errors::NoSuchKey
        # The object was deleted below our feet, nothing we can do.
        # This can happen in any stage of the processing or the post processing
      ensure
        remote_file.cleanup
      end
    end

    private
    attr_reader :event_processor, :post_processors

    def emit_event(line, metadata)
      @event_processor.process(line, metadata)
    end

    def post_process(remote_file)
      @post_processors.each { |processor| processor.process(remote_file) }
    end
  end
end; end; end
