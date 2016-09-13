# encoding: utf-8
module LogStash module Inputs class S3
  # Take the raw event from the files and apply the codec
  # and the metadata.
  class EventProcessor
    def initialize(logger, codec, queue)
      @logger = logger
      @queue = queue
      @codec = codec
    end

    def process(line, metadata)
      @codec.decode(line) do |event| 
        event.set("@metadata", metadata)
        @queue << event 
      end
    end
  end
end end end
