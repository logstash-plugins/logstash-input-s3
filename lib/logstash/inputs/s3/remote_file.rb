# encoding: utf-8
require "logstash/inputs/s3/stream_downloader"
require "forwardable"

module LogStash module Inputs class S3
  class RemoteFile
    extend Forwardable

    attr_reader :remote_object, :metadata, :force_gzip
    attr_accessor :download_to_path, :prefix

    def_delegators :@remote_object, :key, :content_length, :last_modified, :etag, :bucket_name

    def initialize(logger, object, prefix, each_line = true, force_gzip = false )
      @logger = logger
      @remote_object = object
      @prefix = prefix
      @each_line = each_line
      @force_gzip = force_gzip
      @downloaded = false
      download_to_path = Dir.tmpdir
    end

    def download!
      retries = 0
      begin
        StreamDownloader.get(self)
        @downloaded = true
      rescue StandardError => error
        if retries < 5
          @logger.error("StreamDownloader failed, retrying", :object => @remote_object, :error => error, :retries => retries)
          cleanup
          Java::JavaLang::Thread::sleep(2 ** retries * 1000)
          retries += 1
          retry
        else
          @logger.error("StreamDownloader failed, max retries exceeded", :object => @remote_object, :error => error, :retries => retries)
        end
      end
      @downloaded
    end

    def local_object
      # Lazy create FD
      @local_object ||= begin
        FileUtils.mkdir_p(download_to_path)
        ::File.open(::File.join(download_to_path, ::File.basename(key)), 'wb+')
      end
    end

    def local_object=(file)
      @local_object = file
    end

    def each_line(&block)
      return if not download_finished?

      if @local_object.is_a? Zlib::GzipReader
        chunk_count = 0
        loop do
          begin
            internal_each_line(@local_object, &block)
            break if @local_object.unused.nil?
            chunk_count += 1
            file = @local_object.finish
            file.pos -= @local_object.unused.length
            @local_object = Zlib::GzipReader.new(file)
          rescue Zlib::GzipFile::Error => error
            @logger.warn("Error processing GZIP chunk",
                         :remote_object => @remote_object,
                         :local_object => @local_object.to_io,
                         :chunk_count => chunk_count,
                         :error => error)
            break
          end
        end
      else
        internal_each_line(@local_object, &block)
      end
    end

    def internal_each_line(file_part, &block)
      if @each_line
        file_part.each_line do |line|
          block.call(line, metadata)
        end
      else
        block.call(file_part.read, metadata)
      end
    end

    def download_finished?
      @downloaded
    end

    def metadata
      { 
        "s3" =>  {
          "key" => key,
          "bucket_name" => bucket_name,
          "last_modified" => last_modified
        }
      }
    end

    def cleanup
      if @local_object
        @local_object.close unless @local_object.closed?
        ::File.delete(@local_object.path)
        @local_object = nil
      end
    rescue Errno::ENOENT
    end

    def inspect
      "RemoteFile,##{object_id}: remote_object: s3://#{remote_object.bucket_name}/#{remote_object.key}  download_to_path: #{download_to_path}"
    end
    alias_method :to_s, :inspect
  end
end;end;end
