# encoding: utf-8
require "thread_safe"

module LogStash module Inputs class S3
  # WIP
  class SinceDB
    def initialize
      @db = ThreadSafe::Hash.new
    end
    
    def processed?(remote_file)
      @db.include?(remote_file.key)
    end

    def completed(remote_file)
      @db[remote_file.key] = Time.now
    end
  end
end end end
