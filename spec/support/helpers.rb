def fetch_events(settings)
  queue = []
  s3 = LogStash::Inputs::S3.new(settings)
  s3.register
  s3.process_files(queue)
  queue
end

# delete_files(prefix)
def upload_file(local_file, remote_name)
  bucket = s3object.bucket(ENV['AWS_LOGSTASH_TEST_BUCKET'])
  file = File.expand_path(File.join(File.dirname(__FILE__), local_file))
  bucket.object(remote_name).upload_file(file)
end

def delete_remote_files(prefix)
  bucket = s3object.bucket(ENV['AWS_LOGSTASH_TEST_BUCKET'])
  bucket.objects(:prefix => prefix).each { |object| object.delete }
end

def list_remote_files(prefix, target_bucket = ENV['AWS_LOGSTASH_TEST_BUCKET'])
  bucket = s3object.bucket(target_bucket)
  bucket.objects(:prefix => prefix).collect(&:key)
end

def create_bucket(name)
  s3object.bucket(name).create
end

def delete_bucket(name)
  s3object.bucket(name).objects.map(&:delete)
  s3object.bucket(name).delete
end

def s3object
  Aws::S3::Resource.new
end

class TestInfiniteS3Object
  def initialize(s3_obj)
    @s3_obj = s3_obj
  end

  def each
    counter = 1

    loop do
      yield @s3_obj
      counter +=1
    end
  end
end