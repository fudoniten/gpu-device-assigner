(ns gpu-device-assigner.logging
  (:require [clojure.spec.alpha :as s]))

(defprotocol Logger
  (fatal [self msg])
  (error [self msg])
  (warn  [self msg])
  (info  [self msg])
  (debug [self msg]))

(def LOG-LEVELS [:fatal :error :warn :info :debug])
(defn log-index [log-level] (.indexOf LOG-LEVELS log-level))

(defn print-logger
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
  [o]
  (satisfies? Logger o))

(s/def ::logger logger?)
