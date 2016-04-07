# encoding: utf-8
require "logstash/inputs/s3/processing_policy_validator"
require "logstash/inputs/s3/remote_file"

module LogStash module Inputs class S3
  describe ProcessingPolicyValidator do
    let(:remote_file) { RemoteFile.new(s3_object) }

    describe ProcessingPolicyValidator::SkipEndingDirectory do
      context "when the key is a directory" do
        let(:s3_object) { double("remote_file", :key => "hola/") }

        it "doesnt accept to process" do
          expect(subject.process?(remote_file)).to be_falsey
        end
      end

      context "when the key is not a directory" do
        let(:s3_object) { double("remote_file", :key => "hola") } 

        it "accepts to process" do
          expect(subject.process?(remote_file)).to be_truthy
        end
      end
    end

    describe ProcessingPolicyValidator::SkipEmptyFile do
      context "When the file is empty" do
        let(:s3_object) { double("remote_file", :content_length => 0) }

        it "doesnt accept to process" do
          expect(subject.process?(remote_file)).to be_falsey
        end
      end

      context "When the file has contents" do
        let(:s3_object) { double("remote_file", :content_length => 100) }

        it "accepts to process" do
          expect(subject.process?(remote_file)).to be_truthy
        end
      end
    end

    describe ProcessingPolicyValidator::ExcludePattern do
      subject { described_class.new(exclude_pattern) }

      let(:s3_object) { double("remote_file", :key => "bonjourlafamille" ) }

      context "When the pattern is valid" do
        context "When the file is match the pattern" do
          let(:exclude_pattern) { "^bonjour" }

          it "doesnt accept to process" do
            expect(subject.process?(remote_file)).to be_falsey
          end
        end

        context "When the file doesnt match the pattern" do
          let(:exclude_pattern) { "^notmatch" }

          it "accepts to process" do
            expect(subject.process?(remote_file)).to be_truthy
          end
        end
      end
    end

    describe ProcessingPolicyValidator::AlreadyProcessed

    describe ProcessingPolicyValidator::BackupedFiles do
      subject { described_class.new(backup_prefix) }

      let(:s3_object) { double("remote_file", :key => "bonjourlafamille" ) }

      context "When the file start with the backup prefix" do
        let(:backup_prefix) { "bonjour" }

        it "doesnt accept to process" do
          expect(subject.process?(remote_file)).to be_falsey
        end
      end

      context "when the file doesnt start with the backup prefix" do
        let(:backup_prefix) { "Aholabonjour" }

        it "accepts to process" do
          expect(subject.process?(remote_file)).to be_truthy
        end
      end
    end
  end
end;end;end
