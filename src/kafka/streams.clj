(ns kafka.streams
  (:refer-clojure :exclude [filter])
  (:require [clojure.edn :as edn]
            [kafka.utils :as utils]
            [environ.core :refer [env]])
  (:import (org.apache.kafka.streams.kstream Predicate ValueMapper ForeachAction KStream KStreamBuilder)
           (java.util Properties)
           (org.apache.kafka.streams.processor ProcessorSupplier)
           (org.apache.kafka.common.serialization Serdes)
           (org.apache.kafka.streams StreamsConfig KafkaStreams)))

(defn ^Properties streams-props
  ([application-id]
   (streams-props application-id {}))
  ([application-id overrides]
   (utils/map->props
     (merge
       {StreamsConfig/APPLICATION_ID_CONFIG application-id
        StreamsConfig/BOOTSTRAP_SERVERS_CONFIG (:kafka-broker env)
        StreamsConfig/ZOOKEEPER_CONNECT_CONFIG (:zookeeper env)
        StreamsConfig/NUM_STREAM_THREADS_CONFIG (int 4)
        StreamsConfig/KEY_SERDE_CLASS_CONFIG (-> (Serdes/String) .getClass .getName)
        StreamsConfig/VALUE_SERDE_CLASS_CONFIG (-> (Serdes/String) .getClass .getName)}
       overrides))))

(defn builder []
  (KStreamBuilder.))

(defn streams [^KStreamBuilder builder ^Properties props]
  (KafkaStreams. builder props))

(defn close [streams]
  (.close streams))

(defn predicate [f]
  (reify Predicate
    (test [_ _ v]
      (boolean (f v)))))

(defn v-mapper [f]
  (reify ValueMapper
    (apply [_ v]
      (f v))))

(defn action [f]
  (reify ForeachAction
    (apply [_ k v]
      (f k v))))

(defn processor-supplier [supplier-fn]
  (reify ProcessorSupplier
    (get [_]
      (supplier-fn))))

(defn ^KStream map-values [stream f]
  (.mapValues stream (v-mapper f)))

(defn ^KStream filter [stream f]
  (.filter stream (predicate f)))

(defn foreach [stream f]
  (.foreach stream (action f)))

(defn ^KStream from [^KStreamBuilder builder topic]
  (-> builder
      (.stream (utils/vargs topic))
      (map-values edn/read-string)))

(defn to [^KStream stream topic]
  (-> stream
      (map-values pr-str)
      (.to topic)))

(defn pprint [stream label]
  (foreach stream (fn [k v] (clojure.pprint/pprint [label k v]))))