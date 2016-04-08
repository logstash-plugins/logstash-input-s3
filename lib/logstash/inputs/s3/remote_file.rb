# encoding: utf-8
#
require "forwardable"

module LogStash module Inputs class S3
  class RemoteFile
    class NoKeepAlive
      def self.notify
      end
    end

    extend Forwardable

    attr_reader :remote_objects, :metadata

    def_delegators :@remote_object, :key, :content_length
    def_delegators :@keep_alive, :notify

    def initialize(object, keep_alive = NoKeepAlive)
      @remote_object = object
      @keep_alive = keep_alive

      @metadata = retrieve_metadata
    end

    def read(&block)
    end

    def retrieve_metadata
      {}
    end
  end
end;end;end
