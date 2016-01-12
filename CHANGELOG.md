## 2.0.4
 - Fix for Error: No Such Key problem when deleting
## 2.0.3
 - Do not raise an exception if the sincedb file is empty, instead return the current time #66
## 2.0.0
 - Plugins were updated to follow the new shutdown semantic, this mainly allows Logstash to instruct input plugins to terminate gracefully, 
   instead of using Thread.raise on the plugins' threads. Ref: https://github.com/elastic/logstash/pull/3895
 - Dependency on logstash-core update to 2.0

