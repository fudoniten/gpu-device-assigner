(ns gpu-device-assigner.context
  (:require [clojure.spec.alpha :as s]

            [gpu-device-assigner.logging :as log]
            [gpu-device-assigner.k8s-client :as k8s]
            [gpu-device-assigner.lease-renewer :as renewer]))

(s/def ::context
  (s/keys :req-un [::log/logger ::k8s/client]))

(s/fdef create
  :args (s/keys* :req [::log/logger ::k8s/client])
  :ret  ::context)
(defn create
  "Create a new context with a logger and Kubernetes client."
  [& {:keys [::log/logger
             ::k8s/client
             ::renewer/claims-namespace
             ::renewer/renew-interval-ms
             ::renewer/jitter]
      :or   {logger (log/print-logger :warn)}}]
  {:logger     logger
   :k8s-client client
   :claims-namsepace claims-namespace
   :renew-interval-ms renew-interval-ms
   :jitter jitter})
