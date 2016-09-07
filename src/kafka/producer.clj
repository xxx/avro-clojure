(ns kafka.producer
  (:refer-clojure :exclude [send])
  (:require [kafka.utils :as utils]
            ;[clojure.core.async :as a]
            [environ.core :refer [env]])
  (:import (org.apache.kafka.clients.producer ProducerConfig KafkaProducer ProducerRecord)
           (org.apache.kafka.common.serialization StringSerializer)
           (java.util Properties)))

(defn ^Properties producer-props
  ([]
   (producer-props {}))
  ([overrides]
   (utils/map->props
     (merge
       {ProducerConfig/BOOTSTRAP_SERVERS_CONFIG (:kafka-broker env)
        ProducerConfig/ACKS_CONFIG "all"
        ProducerConfig/RETRIES_CONFIG (int 0)
        ProducerConfig/BATCH_SIZE_CONFIG (int 16384)
        ProducerConfig/LINGER_MS_CONFIG (int 1)
        ProducerConfig/BUFFER_MEMORY_CONFIG (int 33554432)
        ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG (.getName StringSerializer)
        ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG (.getName StringSerializer)}
       overrides))))

(defn create-producer [^Properties properties]
  (KafkaProducer. properties))

(defn send [^KafkaProducer producer ^String topic message]
  (.send producer (ProducerRecord. topic message)))

;(defn chan->topic [producer-properties topic in]
;  (let [producer (create-producer producer-properties)]
;    (a/go
;      (try
;        (loop []
;          (when-let [msg (a/<! in)]
;            (send producer topic msg)
;            (recur)))
;        (finally
;          (.close producer))))))