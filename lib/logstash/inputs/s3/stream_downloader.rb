# encoding: utf-8
#
require "stud/temporary"
module LogStash module Inputs class S3
  class StreamDownloader
    def self.get(remote_file)
      remote_object = remote_file.remote_object
      writer = remote_file.download_to
      response = remote_object.get({ :response_target => writer })

      if response.content_encoding.downcase == "gzip"
        Zlib::GzipReader.new(writer)
      else
        writer
      end
    end
  end
end;end;end
