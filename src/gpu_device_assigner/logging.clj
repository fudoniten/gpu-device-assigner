(ns gpu-device-assigner.logging
  (:require [clojure.spec.alpha :as s]
            [taoensso.timbre :as timbre]))

(defrecord TimbreLogger [min-level])

(defn print-logger
  "Create a Timbre-backed logger that leverages Timbre's built-in level handling."
  [log-level]
  (->TimbreLogger log-level))

(defn- with-min-level
  [^TimbreLogger logger f]
  (timbre/with-merged-config {:min-level (:min-level logger)}
    (f)))

(defn fatal [logger msg]
  (with-min-level logger #(timbre/fatal msg)))

(defn error [logger msg]
  (with-min-level logger #(timbre/error msg)))

(defn warn [logger msg]
  (with-min-level logger #(timbre/warn msg)))

(defn info [logger msg]
  (with-min-level logger #(timbre/info msg)))

(defn debug [logger msg]
  (with-min-level logger #(timbre/debug msg)))

(defn logger?
  "Check if an object is a Timbre-backed logger."
  [o]
  (instance? TimbreLogger o))

(s/def ::logger logger?)
