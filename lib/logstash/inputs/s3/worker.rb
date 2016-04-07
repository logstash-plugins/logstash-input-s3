# encoding: utf-8

module LogStash module Inputs class S3
  class Worker
    def initialize(options = {})
    end

    def process()
      # download -> wrap uncompress
      # read the IO object
      # callback, {copy to bucket?, copy_to_tmp, delete?, update_sincedb)
    end

    def execute_callback(object)
    end
  end

  class UncompressPayload
  end
end; end; end; 
