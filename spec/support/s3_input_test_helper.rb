# encoding: utf-8
require "flores/random"
require "zlib"
require "securerandom"
require "logstash/event"

class S3InputTestHelper
  class PlainTextFile
    RANGE_NUMBER_OF_EVENTS = 10..200
    RANGE_LABEL_TEXT = 15..150

    NL = "\n"

    attr_reader :extension
    def initialize
      @extension = "log"
      @ignored = false
    end

    def content
      StringIO.new(file_content)
    end

    def filename
      @filename ||= [basename, extension].join(".")
    end

    def ignored?
      @ignored
    end

    def events
      @events ||= generate_events
    end

    protected
    def file_content
      events.join(NL)
    end

    private
    def basename
      "#{klass_name}-#{SecureRandom.uuid}"
    end
    
    def klass_name
      self.class.to_s.split("::").last
    end

    def generate_events
      number_of_events = Flores::Random.integer(RANGE_NUMBER_OF_EVENTS)
      label = Flores::Random.text(RANGE_LABEL_TEXT)

      events = []

      number_of_events.times do |identifier|
        events << generate_event(identifier, label)
      end

      events
    end

    def generate_event(id, label)
      "#{label} - #{id}"
    end
  end

  class GzipFile < PlainTextFile
    def initialize
      super
      @extension = "log.gz"
    end

    def content
      compressed = StringIO.new
      gz = Zlib::GzipWriter.new(compressed)
      gz.write(file_content)
      gz.close
      compressed.string
    end
  end

  class NoopFile < PlainTextFile
    def initialize
      super
      @extension = "bz2"
      @ignored = true
    end
  end

  class ZeroFile < PlainTextFile
    def initialize
      super
      @extension = "log"
      @ignore = true
    end

    def events
      []
    end
  end

  RANGE_NUMBER_OF_FILES = 10..40

  def initialize(bucket)
    @bucket = bucket
    @files = []
  end

  def setup
    cleanup # make sure the bucket is in a clean state
    generate_files
    upload_files
  end

  def cleanup
    @bucket.objects.each { |key| key.delete }
  end

  def content
    @files.collect do |file|
      file.events.collect { |event_data| LogStash::Event.new({ "message" => event_data }) }
    end.flatten
  end

  def upload_files
    @files.collect do |file|
      Concurrent::Future.execute { @bucket.put_object({ :key => file.filename, :body => file.content }) }
    end.all? { |upload| !upload.value.nil? }
  end

  def generate_files
    [PlainTextFile, GzipFile, NoopFile, ZeroFile].each do |klass|
      Flores::Random.integer(RANGE_NUMBER_OF_FILES).times do
        @files << klass.new
      end
    end
  end
end
