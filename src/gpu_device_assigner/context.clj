(ns gpu-device-assigner.context
  (:require [clojure.spec.alpha :as s]

            [gpu-device-assigner.logging :as log]
            [gpu-device-assigner.k8s-client :as k8s]))

(s/def ::context
  (s/keys :req-un [::log/logger ::k8s/client]))

(s/fdef create
  :args (s/keys* :req-un [::log/logger ::k8s/client])
  :ret  ::context)
(defn create
  "Create a new context with a logger and Kubernetes client."
  [& {:keys [logger client]
      :or   {logger (log/print-logger :warn)}}]
  {:logger     logger
   :k8s-client client})
