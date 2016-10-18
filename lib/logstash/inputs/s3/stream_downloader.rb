# encoding: utf-8
#
require "stud/temporary"

module LogStash module Inputs class S3
  class StreamDownloader
    def self.get(remote_file)
      remote_object = remote_file.remote_object
      response = remote_object.get({ :response_target => remote_file.local_object.to_io })
      remote_file.local_object.to_io.reopen(remote_file.local_object.to_io.path, 'rb')

      if remote_file.force_gzip ||
         (response.content_encoding && response.content_encoding.downcase == "gzip")
        remote_file.local_object = Zlib::GzipReader.new(remote_file.local_object.to_io)
      end
    end
  end
end;end;end
