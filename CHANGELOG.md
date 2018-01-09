## 3.2.0
 - Add support for auto-detecting gzip files with `.gzip` extension, in addition to existing support for `*.gz`
 - Improve performance of gzip decoding by 10x by using Java's Zlib

## 3.1.9
  - Change default sincedb path to live in `{path.data}/plugins/inputs/s3` instead of $HOME.
    Prior Logstash installations (using $HOME default) are automatically migrated.
  - Don't download the file if the length is 0 #2

## 3.1.8
  - Update gemspec summary

## 3.1.7
  - Fix missing last multi-line entry #120

## 3.1.6
  - Fix some documentation issues

## 3.1.4
 - Avoid parsing non string elements #109

## 3.1.3
 - The plugin will now include the s3 key in the metadata #105

## 3.1.2
 - Fix an issue when the remote file contains multiple blob of gz in the same file #101
 - Make the integration suite run
 - Remove uneeded development dependency

## 3.1.1
  - Relax constraint on logstash-core-plugin-api to >= 1.60 <= 2.99

## 3.1.0
 - breaking,config: Remove deprecated config `credentials` and `region_endpoint`. Please use AWS mixin.

## 3.0.1
 - Republish all the gems under jruby.

## 3.0.0
 - Update the plugin to the version 2.0 of the plugin api, this change is required for Logstash 5.0 compatibility. See https://github.com/elastic/logstash/issues/5141

## 2.0.6
 - Depend on logstash-core-plugin-api instead of logstash-core, removing the need to mass update plugins on major releases of logstash

## 2.0.5
 - New dependency requirements for logstash-core for the 5.0 release

## 2.0.4
 - Fix for Error: No Such Key problem when deleting

## 2.0.3
 - Do not raise an exception if the sincedb file is empty, instead return the current time #66

## 2.0.0
 - Plugins were updated to follow the new shutdown semantic, this mainly allows Logstash to instruct input plugins to terminate gracefully, 
   instead of using Thread.raise on the plugins' threads. Ref: https://github.com/elastic/logstash/pull/3895
 - Dependency on logstash-core update to 2.0

