(ns gpu-device-assigner.context
  (:require [clojure.spec.alpha :as s]

            [gpu-device-assigner.k8s-client :as k8s]
            [gpu-device-assigner.lease-renewer :as renewer]))

(s/def ::context
  (s/keys :req-un [::k8s/client ::renewer/claims-namespace]
          :opt-un [::renewer/renew-interval-ms ::renewer/jitter]))

(s/fdef create
  :args (s/keys* :req [::k8s/client ::renewer/claims-namespace]
                 :opt [::renewer/renew-interval-ms ::renewer/jitter])
  :ret  ::context)
(defn create
  "Create a new context with Kubernetes client and lease renewal settings."
  [& {:keys [::k8s/client
             ::renewer/claims-namespace
             ::renewer/renew-interval-ms
             ::renewer/jitter]}]
  {:k8s-client client
   :claims-namespace claims-namespace
   :renew-interval-ms renew-interval-ms
   :jitter jitter})
