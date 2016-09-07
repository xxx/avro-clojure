(ns avro-conversion.conversion
  (:require [abracad.avro :as abracad]
            [clojure.java.io :as io])
  (:import (java.io ByteArrayOutputStream)))

(defn records->avro
  "Write one or more record maps to an Avro byte array, returning it"
  [records schema-str]
  (let [schema (abracad/parse-schema schema-str)
        out (ByteArrayOutputStream.)]
    (with-open [adf (abracad/data-file-writer "snappy" schema out)]
      (doseq [r records] (.append adf r)))
    (.toByteArray out)))

(defn avro->records
  "Create record map(s) from an Avro byte array with schema, which can be overridden."
  ([records]
   (with-open [adf (abracad/data-file-reader records)]
     (doall (seq adf))))

  ([records schema-str]
   (let [schema (abracad/parse-schema schema-str)]
     (with-open [adf (abracad/data-file-reader schema records)]
       (doall (seq adf))))))

(defn- slurp-bytes
  "Slurp the bytes from a slurpable thing"
  [x]
  (let [out (ByteArrayOutputStream.)]
    (io/copy (io/input-stream x) out)
    (.toByteArray out)))
