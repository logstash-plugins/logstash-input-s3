# encoding: utf-8
#
require "forwardable"

module LogStash module Inputs class S3
  class RemoteFile
    extend Forwardable

    attr_reader :remote_objects

    def_delegators :@remote_object, :key, :content_length

    def initialize(object)
      @remote_object = object
    end
  end
end;end;end
