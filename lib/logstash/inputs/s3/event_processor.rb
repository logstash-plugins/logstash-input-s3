# encoding: utf-8
require "logstash/inputs/base"

module LogStash module Inputs class S3 < LogStash::Inputs::Base
  # Take the raw event from the files and apply the codec
  # and the metadata.
  class EventProcessor
    def initialize(logstash_inputs_s3, codec, queue, include_object_properties, logger)
      @queue = queue
      @codec = codec
      @logstash_inputs_s3 = logstash_inputs_s3
      @include_object_properties = include_object_properties
      @logger = logger
    end

    def process(line, metadata, remote_file_data)
      @codec.decode(line) do |event|
        # We are making an assumption concerning cloudfront
        # log format, the user will use the plain or the line codec
        # and the message key will represent the actual line content.
        # If the event is only metadata the event will be drop.
        # This was the behavior of the pre 1.5 plugin.
        #
        # The line need to go through the codecs to replace
        # unknown bytes in the log stream before doing a regexp match or
        # you will get a `Error: invalid byte sequence in UTF-8'
        if event_is_metadata?(event)
          @logger.debug('Event is metadata, updating the current cloudfront metadata', :event => event)
          return update_metadata(metadata, event)
        end

        @logger.debug('Event is not metadata, pushing to queue', :event => event, :metadata => metadata)
        push_decoded_event(@queue, metadata, remote_file_data, event)
      end
    end

    private

    def push_decoded_event(queue, metadata, remote_file_data, event)
      @logstash_inputs_s3.send(:decorate, event)

      if @include_object_properties
        event.set("[@metadata][s3]", remote_file_data.to_h)
      else
        event.set("[@metadata][s3]", {})
      end

      # event.set("[@metadata][s3][key]", remote_file.key) # key should already be in remote_file_data.to_h
      event.set(@cloudfront_version_key, metadata[:cloudfront_version]) unless metadata[:cloudfront_version].nil?
      event.set(@cloudfront_fields_key, metadata[:cloudfront_fields]) unless metadata[:cloudfront_fields].nil?

      queue << event
    end

    def event_is_metadata?(event)
      return false unless event.get("message").class == String
      line = event.get("message")
      version_metadata?(line) || fields_metadata?(line)
    end

    def version_metadata?(line)
      line.start_with?('#Version: ')
    end

    def fields_metadata?(line)
      line.start_with?('#Fields: ')
    end

    def update_metadata(metadata, event)
      line = event.get('message').strip

      if version_metadata?(line)
        metadata[:cloudfront_version] = line.split(/#Version: (.+)/).last
      end

      if fields_metadata?(line)
        metadata[:cloudfront_fields] = line.split(/#Fields: (.+)/).last
      end
    end
  end
end end end
