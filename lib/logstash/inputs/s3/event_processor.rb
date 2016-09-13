# encoding: utf-8
require "logstash/util/decorators"

module LogStash module Inputs class S3
  # Take the raw event from the files and apply the codec
  # and the metadata.
  class EventProcessor
    def initialize(logger, codec, queue, plugin)
      @logger = logger
      @queue = queue
      @codec = codec
      @plugin = plugin
    end

    def process(line, metadata)
      @codec.decode(line) do |event| 
        event.set("@metadata", metadata)
        event.set("type", @plugin.type) if @plugin.type and !event.include?("type")

        LogStash::Util::Decorators.add_fields(@plugin.add_field,event,"inputs/#{@plugin.class.name}")
        LogStash::Util::Decorators.add_tags(@plugin.tags,event,"inputs/#{@plugin.class.name}")

        @queue << event 
      end
    end
  end
end end end
