# encoding: utf-8
module LogStash module Inputs class S3
  class PostProcessor
    class UpdateSinceDB
      def initialize(sincedb)
        @sincedb = sincedb
      end

      def process(remote_file)
        @sincedb.completed(remote_file)
      end
    end
end end end end
