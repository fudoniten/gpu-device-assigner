(ns gpu-device-assigner.test-support
  "Helpers for faking the Kubernetes API in tests.

  The fakes mirror what `k8s-client/K8SBaseClient` really returns: on success
  the response *body* itself (e.g. the Lease object, or {:items [...]} for a
  list), and on failure {:status <code> :error true ...}."
  (:require [gpu-device-assigner.k8s-client :as k8s]
            [gpu-device-assigner.util :as util]))

(defn error [status]
  {:status status :error true :body {:kind "Status" :code status}})

(defn mock-client
  "Build a K8SClient whose base client dispatches on [kind action].
   `routes` maps [kind action] to either a value or a fn of the request.
   Every call is recorded in the `calls` atom as {:kind :action :request}."
  ([routes] (mock-client routes (atom [])))
  ([routes calls]
   (k8s/->K8SClient
    (reify k8s/IK8SBaseClient
      (invoke [_ {:keys [kind action request]}]
        (swap! calls conj {:kind kind :action action :request request})
        (if-let [r (get routes [kind action])]
          (if (fn? r) (r request) r)
          (throw (ex-info "unexpected request" {:kind kind :action action :request request}))))))))

(defn calls-of [calls kind action]
  (filter #(and (= kind (:kind %)) (= action (:action %))) @calls))

(defn gpu-node
  "A Ready, schedulable node advertising `devices` ({uuid [labels]})."
  [node-name devices & {:keys [ready? unschedulable? allocatable]
                        :or   {ready? true unschedulable? false}}]
  {:metadata {:name        node-name
              :annotations {:fudo.org/gpu.device.labels
                            (util/base64-encode (util/try-json-generate devices))}}
   :spec     (cond-> {} unschedulable? (assoc :unschedulable true))
   :status   {:conditions  [{:type "Ready" :status (if ready? "True" "False")}]
              :allocatable {:nvidia.com/gpu (str (or allocatable (count devices)))}}})
