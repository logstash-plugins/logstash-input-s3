# encoding: utf-8

module LogStash module Inputs class S3
  class ProcessingPolicyValidator
    class SkipEndingDirectory
      ENDING_DIRECTORING_STRING = "/"

      def process?(remote_file)
        !remote_file.key.end_with?(ENDING_DIRECTORING_STRING)
      end
    end

    class SkipEmptyFile
      def process?(remote_file)
        remote_file.content_length > 0
      end
    end

    class AlreadyProcessed
      def initialize(sincedb)
        @sincedb = sincedb
      end

      def process?(remote_file)
        # @sincedb.processed?(remote_file)
        return true
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

    class BackupedFiles < ExcludePattern
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
      @policies.all? { |policy| policy.process?(remote_file) }
    end

    def count
      @policies.count
    end
  end
end; end; end
