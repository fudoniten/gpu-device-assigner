(ns gpu-device-assigner.logging
  (:require [clojure.spec.alpha :as s]))

(defprotocol Logger
  "Protocol defining logging operations."
  (fatal [self msg])
  (error [self msg])
  (warn  [self msg])
  (info  [self msg])
  (debug [self msg]))

(def LOG-LEVELS [:fatal :error :warn :info :debug])
(defn log-index [log-level] (.indexOf LOG-LEVELS log-level))

(defn print-logger
  "Create a simple logger that prints messages to the console."
  [log-level]
  (let [log-idx (log-index log-level)]
    (reify Logger

      (fatal [_ msg]
        (when (<= log-idx (log-index :fatal))
          (println msg)))

      (error [_ msg]
        (when (<= log-idx (log-index :error))
          (println msg)))

      (warn [_ msg]
        (when (<= log-idx (log-index :warn))
          (println msg)))

      (info [_ msg]
        (when (<= log-idx (log-index :info))
          (println msg)))

      (debug [_ msg]
        (when (<= log-idx (log-index :debug))
          (println msg))))))

(defn logger?
  "Check if an object satisfies the Logger protocol."
  [o]
  (satisfies? Logger o))

(s/def ::logger logger?)
