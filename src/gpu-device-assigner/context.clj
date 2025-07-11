(ns gpu-device-assigner.context
  (:require [clojure.spec.alpha :as s]

            [gpu-device-assigner.logging :as log]
            [gpu-device-assigner.k8s-client :as k8s-client]))

(s/def ::context
  (s/keys :req-un [::log/logger ::k8s-client/client]))

(s/fdef create
  :args (s/keys* :req-un [::logger ::k8s-client])
  :ret  ::context)
(defn create
  "Create a new context with a logger and Kubernetes client."
  [& {:keys [logger k8s-client]
      :or   {logger (log/print-logger)}}]
  {:logger     logger
   :k8s-client k8s-client})
