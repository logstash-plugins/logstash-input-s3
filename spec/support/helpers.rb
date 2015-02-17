def fetch_events(settings)
  queue = []
  s3 = LogStash::Inputs::S3.new(settings)
  s3.register
  s3.process_files(queue)
  s3.teardown
  queue
end

# delete_files(prefix)
def upload_file(local_file, remote_name)
  bucket = s3object.buckets[ENV['AWS_LOGSTASH_TEST_BUCKET']]
  file = File.expand_path(File.join(File.dirname(__FILE__), local_file))
  bucket.objects[remote_name].write(:file => file)
end

def delete_remote_files(prefix)
  bucket = s3object.buckets[ENV['AWS_LOGSTASH_TEST_BUCKET']]
  bucket.objects.with_prefix(prefix).each { |object| object.delete }
end

def list_remote_files(prefix, target_bucket = ENV['AWS_LOGSTASH_TEST_BUCKET'])
  bucket = s3object.buckets[target_bucket]
  bucket.objects.with_prefix(prefix).collect(&:key)
end

def delete_bucket(name)
  s3object.buckets[name].objects.map(&:delete)
  s3object.buckets[name].delete
end

def s3object
  AWS::S3.new
end
