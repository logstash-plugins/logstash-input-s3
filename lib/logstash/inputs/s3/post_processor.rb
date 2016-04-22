# encoding: utf-8
require "fileutils"

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

    class BackupLocally
      def initialize(backup_to_dir)
        @backup_dir = backup_to_dir
        FileUtils.mkdir_p(@backup_dir)
      end

      def process(remote_file)
        destination = File.join(@backup_dir, remote_file.key)

        if File.exist?(destination)
          destination = File.join(@backup_dir, "#{remote_file.key}_#{remote_file.version}")
        end

        case remote_file.file
        when StringIO
          File.open(destination) { |f| f.write(remote_file.file.read) }
        when File
          FileUtils.cp(remote_file.file.path, destination)
        end
      end
    end

    class BackupToBucket
      attr_reader :backup_bucket, :backup_prefix

      def initialize(backup_bucket, backup_prefix = nil)
        @backup_bucket = backup_bucket
        @backup_prefix = backup_prefix
      end

      def process(remote_file)
        remote_file.remote_object.copy_to(destination(remote_file))
      end

      def destination(remote_file)
        "#{@backup_bucket}/#{rename(remote_file.key)}"
      end

      def rename(key)
        backup_prefix.nil? ? key : "#{backup_prefix}#{key}"
      end
    end

    class MoveToBucket < BackupToBucket
      def process(remote_file)
        remote_file.remote_object.move_to(destination(remote_file))
      end
    end

    class DeleteFromSourceBucket
      def process(remote_file)
        remote_file.remote_object.delete
      end
    end
end end end end
