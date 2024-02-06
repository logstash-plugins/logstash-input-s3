require "logstash/inputs/s3/sincedb"

describe LogStash::Inputs::S3::SinceDB do
  let(:sincedb_path) { Stud::Temporary.file.path }
  let(:ignore_older) { 86400 }
  let(:logger) { double("logger").as_null_object }
  let(:key) { LogStash::Inputs::S3::SinceDB::SinceDBKey.new("file.txt", "etag123", "bucket") }
  let(:value) { LogStash::Inputs::S3::SinceDB::SinceDBValue.new(Time.now, Time.now) }
  let(:options) { { sincedb_expire_secs: 120, bookkeeping_enabled: false } }
  let(:sincedb_args) { [sincedb_path, ignore_older, logger, options] }

  subject { described_class.new(*sincedb_args) }

  describe "#initialize" do
    it "initializes the sincedb with default options" do
      expect(subject.instance_variable_get(:@options)).to eq(options)
      expect(subject.instance_variable_get(:@db)).to be_a(ThreadSafe::Hash)
      expect(subject.instance_variable_get(:@need_sync)).to be_a(Concurrent::AtomicBoolean)
      expect(subject.instance_variable_get(:@stopped)).to be_a(Concurrent::AtomicBoolean)
    end
  end

  describe "#close" do
    it "cleans old keys and serializes the sincedb" do
      expect(subject.instance_variable_get(:@stopped)).to receive(:make_true)
      expect(subject).to receive(:clean_old_keys)
      expect(subject).to receive(:serialize)
      subject.close
    end

    context 'db contains value that is less than sincedb_expire_secs older than the newest value' do
      let(:old_value) { LogStash::Inputs::S3::SinceDB::SinceDBValue.new(Time.now - 119, Time.now - 119) }
      let(:new_value) { LogStash::Inputs::S3::SinceDB::SinceDBValue.new(Time.now, Time.now) }
      let(:new_key) { LogStash::Inputs::S3::SinceDB::SinceDBKey.new("file2.txt", "etag123", "bucket") }

      it 'preserves both entries in the db' do
        subject.instance_variable_get(:@db)[key] = old_value
        subject.instance_variable_get(:@db)[new_key] = new_value
        subject.close
        expect(subject.instance_variable_get(:@db)).to include(key)
        expect(subject.instance_variable_get(:@db)).to include(new_key)
      end
    end

    context 'db contains value that is more than sincedb_expire_secs older than the newest value' do
      let(:old_value) { LogStash::Inputs::S3::SinceDB::SinceDBValue.new(Time.now - 121, Time.now - 121) }
      let(:new_value) { LogStash::Inputs::S3::SinceDB::SinceDBValue.new(Time.now, Time.now) }
      let(:new_key) { LogStash::Inputs::S3::SinceDB::SinceDBKey.new("file2.txt", "etag123", "bucket") }

      it "cleans only the old entries from the db" do
        subject.instance_variable_get(:@db)[key] = old_value
        subject.instance_variable_get(:@db)[new_key] = new_value
        subject.close
        expect(subject.instance_variable_get(:@db)).to include(new_key)
        expect(subject.instance_variable_get(:@db)).not_to include(key)
      end
    end
  end

  describe "#completed" do
    let(:remote_file) { double('remote_file') }

    before do
      allow(remote_file).to receive(:etag).and_return("etag123")
      allow(remote_file).to receive(:key).and_return("file.txt")
      allow(remote_file).to receive(:bucket_name).and_return("bucket")
      allow(remote_file).to receive(:last_modified).and_return(Time.now)
    end

    it "requests sync" do
      expect(subject).to receive(:request_sync)
      subject.completed(remote_file)
    end
  end

  describe "#oldest_key" do
    it "returns the oldest key in the sincedb" do
      subject.instance_variable_get(:@db)[key] = value
      oldest_key = subject.oldest_key
      expect(oldest_key).to eq(key.key)
    end
  end

  describe "#processed?" do
    let(:remote_file) { double('remote_file') }

    before do
      allow(remote_file).to receive(:etag).and_return("etag123")
      allow(remote_file).to receive(:key).and_return("file.txt")
      allow(remote_file).to receive(:bucket_name).and_return("bucket")
    end

    it "returns true if the sincedb is not empty" do
      subject.instance_variable_get(:@db)[key] = value
      expect(subject.processed?(remote_file)).to be true
    end

    it "returns false if the sincedb is empty" do
      expect(subject.processed?(remote_file)).to be false
    end
  end

  describe "#reseed" do
    let(:remote_file) { "remote_file.txt" }

    it "calls completed with the remote file" do
      expect(subject).to receive(:completed).with(remote_file)
      subject.reseed(remote_file)
    end
  end

  # Add more tests for other methods as needed
end
