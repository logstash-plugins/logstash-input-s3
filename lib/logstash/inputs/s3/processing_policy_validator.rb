# encoding: utf-8
module LogStash module Inputs class S3
  class ProcessingPolicyValidator
    class SkipEndingDirectory
      ENDING_DIRECTORY_STRING = "/"

      def self.process?(remote_file)
        !remote_file.key.end_with?(ENDING_DIRECTORY_STRING)
      end
    end

    class SkipEmptyFile
      def self.process?(remote_file)
        remote_file.content_length > 0
      end
    end

    class IgnoreOlderThan
      def initialize(seconds)
        @seconds = seconds
      end

      def process?(remote_file)
        Time.now - remote_file.last_modified < @seconds
      end
    end

    class AlreadyProcessed
      def initialize(sincedb)
        @sincedb = sincedb
      end

      def process?(remote_file)
        !@sincedb.processed?(remote_file)
      end
    end

    class ExcludePattern
      def initialize(pattern)
        @pattern = Regexp.new(pattern)
      end

      def process?(remote_file)
        remote_file.key !~ @pattern
      end
    end

    class ExcludeBackupedFiles < ExcludePattern
      def initialize(backup_prefix)
        super(/^#{backup_prefix}/)
      end
    end

    def initialize(*policies)
      @policies = []
      add_policy(policies)
    end

    def add_policy(*policies)
      @policies = @policies.concat([policies].flatten)
    end

    def process?(remote_file)
      # TODO log were we stop
      @policies.all? { |policy| policy.process?(remote_file) }
    end

    def count
      @policies.count
    end
  end
end; end; end
