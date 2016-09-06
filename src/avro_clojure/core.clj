(ns avro-clojure.core
  (:require [abracad.avro :as avro]
            [clojure.java.shell :refer [sh]]
            [clojure.java.io :as io])
  (:import (java.io ByteArrayOutputStream)))

(defn parse-schema-filepath
  [schema-file]
  (let [f (slurp schema-file)]
    (avro/parse-schema f)))

(defn write-employees-to-file
  [employees output-filename]
  (let [schema (parse-schema-filepath "schema/mpd-simple.avsc")]
    (with-open [adf (avro/data-file-writer "snappy" schema output-filename)]
      (doseq [e employees] (.append adf e)))))

(defn read-employees-from-file
  [filename]
  (with-open [adf (avro/data-file-reader filename)]
    (doall (seq adf))))

(defn write-employees
  "Write one or more employees to a byte string, returning it"
  [employees schema-path]
  (let [schema (parse-schema-filepath schema-path)
        out (ByteArrayOutputStream.)]
    (with-open [adf (avro/data-file-writer "snappy" schema out)]
      (doseq [e employees] (.append adf e)))
    (.toByteArray out)))

(defn read-employees
  "Create employee map(s) from a byte string with schema, which can be overridden."
  ([employees] (with-open [adf (avro/data-file-reader employees)]
                 (doall (seq adf))))

  ([employees schema-path] (let [schema (parse-schema-filepath schema-path)]
                             (with-open [adf (avro/data-file-reader schema employees)]
                               (doall (seq adf))))))

(defn- slurp-bytes
  "Slurp the bytes from a slurpable thing"
  [x]
  (let [out (ByteArrayOutputStream.)]
    (io/copy (io/input-stream x) out)
    (.toByteArray out)))

(comment
  (String. (slurp-bytes "mpd.out"))
  (String. (write-employees [{:name "michael dungeon" :age 666} {:name "a dog" :age 9}] "schema/mpd-simple.avsc"))
  (read-employees (write-employees [{:name "michael dungeon" :age 666} {:name "a dog" :age 9}] "schema/mpd-simple.avsc") "schema/mpd-simple2.avsc")
  (read-employees (write-employees [{:name "michael dungeon" :age 666} {:name "a dog" :age 9}] "schema/mpd-simple.avsc"))

  (write-employees-to-file ({:name "michael dungeon" :age 666}) "mpd.out")
  (read-employees-from-file "mpd.out")
  (read-employees (slurp-bytes "mpd.out")))
