(ns avro-clojure.core
  (:require [abracad.avro :as avro]
            [clojure.java.shell :refer [sh]]))

(defn parse-schema-filepath
  [schema-file]
  (let [f (slurp schema-file)]
    (avro/parse-schema f)))

(defn write-employee
  [employee output-filename]
  (let [schema (parse-schema-filepath "schema/mpd-simple.avsc")]
    (with-open [adf (avro/data-file-writer "snappy" schema output-filename)]
      (.append adf employee)
      (.append adf {:name "a dog" :age 4}))))

(defn read-employee
  [filename]
  (with-open [adf (avro/data-file-reader filename)]
    (doall (seq adf))))

(comment
  (write-employee {:name "michael dungeon" :age 666} "mpd.out")
  (read-employee "mpd.out"))
