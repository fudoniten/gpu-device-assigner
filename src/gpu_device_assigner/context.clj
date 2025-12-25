(ns gpu-device-assigner.context
  (:require [clojure.spec.alpha :as s]

            [gpu-device-assigner.k8s-client :as k8s]))

(s/def ::claims-namespace string?)
(s/def ::renew-interval-ms integer?)
(s/def ::jitter number?)

(s/def ::context
  (s/keys :req-un [::k8s/client ::claims-namespace]
          :opt-un [::renew-interval-ms ::jitter]))

(s/fdef create
  :args (s/keys* :req [::k8s/client ::claims-namespace]
                 :opt [::renew-interval-ms ::jitter])
  :ret  ::context)
(defn create
  "Create a new context with Kubernetes client and lease renewal settings."
  [& {:keys [::k8s/client
             ::claims-namespace
             ::renew-interval-ms
             ::jitter]}]
  {:k8s-client client
   :claims-namespace claims-namespace
   :renew-interval-ms renew-interval-ms
   :jitter jitter})
