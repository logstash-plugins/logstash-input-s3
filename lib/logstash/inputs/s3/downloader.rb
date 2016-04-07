# encoding: utf-8
module LogStash module Inputs module S3
  class NoDownloadKeepAlive
    def self.notify
    end
  end

  class Downloader
    class LocalFile
      def get
      end
    end

    class InMemoryFile
      def get
        StringIO.new
      end
    end

    def initialize(file_target = InMemoryFile)
      @file_target = InMemoryFile
    end

    def download(s3object, keep_alive = NoDownloadKeepAlive)
      target = @file_target.get

      handler = -> chunk do
        target.write(chunk)
        keep_alive.notify # track download activity
      end

      s3object.get({ response_target: handler })
    end

    def content
    end

    def complete?
      # check size
    end
  end


  class WorkManager
    def initialize(workers)
      # initialize worker
    end

    # Should block.
    # SynchronizeQueue
    def enqueue()
    end

    def do_work
      loop do
        # take a worker
        # take from queue
        # process
      end
    end

    def stop
    end
  end

  # PreProcessor 
  # uncompress
  # Post processor => send to queue
  # Reader => with status
end;end;end
