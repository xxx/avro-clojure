(ns avro-clojure.core
  (:require [avro-conversion.conversion :as avro]
            [kafka.streams :as k]
            [kafka.producer :as kp]))

(def input-topic "clj-input")
(def output-topic "clj-output")

(defn process-stuff [v]
  (println "type: " (type v))
  (println "raw input: " v)
  (println "str input: " (String. v))
  (let [read-schema (slurp "schema/mpd-simple2.avsc")
        r (avro/avro->records v read-schema)
        write-schema (slurp "schema/mpd-simple.avsc")]
    (println "converted: " r)
    (println "converted type: " (type r))
    (avro/records->avro r write-schema)))

(defn mah-streams []
  (let [builder (k/builder)
        input-stream (-> builder
                         (k/from input-topic)
                         (k/map-values process-stuff))]

    (k/to input-stream output-topic)

    (k/streams
      builder
      (k/streams-props "mpd-avro-test"))))

(defn start []
  {:streams (doto (mah-streams) (.start))})

(defn stop [sys]
  (k/close (:streams sys)))

(comment
  (avro/avro->records
    (avro/records->avro
      [{:name "michael dungeon" :age 666} {:name "a dog" :age 9}]
      (slurp "schema/mpd-simple.avsc"))
    (slurp "schema/mpd-simple2.avsc"))

  (avro/avro->records
    (avro/records->avro
      [{:name "michael dungeon" :age 666} {:name "a dog" :age 9}]
      (slurp "schema/mpd-simple.avsc")))

  (def producer (kp/create-producer))
  (kp/send
    producer
    input-topic
    (avro/records->avro
      [{:name "mp dizzle" :age 666666666} {:name "Josie" :age 15}]
      (slurp "schema/mpd-simple.avsc")))

  (def stuff (start))
  (stop stuff)

  (do
    (stop stuff)
    (def stuff (start))
    (kp/send
      producer
      input-topic
      (avro/records->avro
        [{:name "mp dizzle" :age 666666666} {:name "Josie" :age 15}]
        (slurp "schema/mpd-simple.avsc")))))
