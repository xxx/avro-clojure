(ns kafka.utils
  (:import (java.util Properties)))

(defn ^Properties map->props [m]
  (doto (Properties.) (.putAll m)))

(defn vargs [& args]
  (into-array (type (first args)) args))