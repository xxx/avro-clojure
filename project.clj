(defproject avro-clojure "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/core.async "0.2.385"]
                 [environ "1.1.0"]
                 [org.apache.kafka/kafka-streams "0.10.0.1"]
                 [org.apache.kafka/kafka-clients "0.10.0.1"]]
  :resource-paths ["dependencies/clj-avro-3dna-0.1.0-standalone.jar"]
  :plugins [[lein-environ "1.1.0"]])


