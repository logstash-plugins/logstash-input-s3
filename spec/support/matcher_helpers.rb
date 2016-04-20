# encoding: utf-8
require 'rspec/expectations'

RSpec::Matchers.define :include_content_of do |expected|
  match do |actual|
    return false if actual.size != expected.size
    actual.all? { |item| expected.include?(item) }
  end
end
