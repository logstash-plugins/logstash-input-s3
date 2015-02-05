def fetch_events(settings)
  queue = []
  s3 = LogStash::Inputs::S3.new(settings)
  s3.register
  s3.process_files(queue)
  s3.teardown
  queue
end
