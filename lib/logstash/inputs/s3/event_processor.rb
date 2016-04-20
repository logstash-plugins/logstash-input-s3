# encoding: utf-8
module LogStash module Inputs class S3
  class EventProcessor
    def initialize(plugin, queue)
      @queue = queue
      @plugin = plugin
    end

    def process(line, metadata)
      @plugin.codec.decode(line) do |event| 
        event["[@metadata]"] = metadata
        @queue << event 
      end
    end
  end
end end end
