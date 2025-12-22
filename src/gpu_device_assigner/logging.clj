(ns gpu-device-assigner.logging
  (:require [clojure.spec.alpha :as s]
            [taoensso.timbre :as timbre]))

;; Ensure Timbre allows all levels; per-logger gating happens in this namespace.
(timbre/merge-config! {:min-level :trace})

(def LOG-LEVELS [:fatal :error :warn :info :debug])

(defn log-index
  "Return the position of `log-level` within `LOG-LEVELS`."
  [log-level]
  (.indexOf LOG-LEVELS log-level))

(defrecord TimbreLogger [min-level])

(defn print-logger
  "Create a Timbre-backed logger that respects the provided minimum level."
  [log-level]
  (->TimbreLogger log-level))

(defn- enabled?
  [^TimbreLogger logger log-level]
  (<= (log-index log-level) (log-index (:min-level logger))))

(defn fatal [logger msg]
  (when (enabled? logger :fatal)
    (timbre/fatal msg)))

(defn error [logger msg]
  (when (enabled? logger :error)
    (timbre/error msg)))

(defn warn [logger msg]
  (when (enabled? logger :warn)
    (timbre/warn msg)))

(defn info [logger msg]
  (when (enabled? logger :info)
    (timbre/info msg)))

(defn debug [logger msg]
  (when (enabled? logger :debug)
    (timbre/debug msg)))

(defn logger?
  "Check if an object is a Timbre-backed logger."
  [o]
  (instance? TimbreLogger o))

(s/def ::logger logger?)
