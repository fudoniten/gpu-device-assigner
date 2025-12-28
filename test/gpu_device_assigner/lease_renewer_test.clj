(ns gpu-device-assigner.lease-renewer-test
  (:require [clojure.test :refer [deftest is testing]]
            [gpu-device-assigner.core :as core]
            [gpu-device-assigner.k8s-client :as k8s]
            [gpu-device-assigner.lease-renewer :as renewer]))

(deftest finalize-pending-lease-when-pod-active
  (testing "pending reservation is finalized when pod is active and using device"
    (let [patched (atom nil)
          lease   {:metadata {:name "gpu1"
                               :namespace "gpu-claims"
                               :labels {"fudo.org/gpu.uuid" "gpu1"
                                        "fudo.org/pod.namespace" "default"
                                        (name core/reservation-state-label) core/proposed-reservation}
                               :annotations {(name core/reservation-annotation) "res-123"}}
                   :spec {:holderIdentity "res-123"}}
          pod     {:metadata {:uid "pod-uid"
                               :name "demo"
                               :namespace "default"
                               :annotations {(name core/reservation-annotation) "res-123"
                                             (name core/gpu-annotation) "gpu1"
                                             "cdi.k8s.io/gpu-assignment" "nvidia.com/gpu=UUID=gpu1"}}
                   :status {:phase "Running"}}
          client  (k8s/->K8SClient
                   (reify k8s/IK8SBaseClient
                     (invoke [_ {:keys [kind action request]}]
                       (case [kind action]
                         [:Lease :list] {:items [lease]}
                         [:Pod :list] {:items [pod]}
                         [:Lease :get] {:body lease}
                         [:Lease :patch/json-merge] (do (reset! patched request)
                                                         {:status 200})
                         (throw (ex-info "unexpected request" {:kind kind :action action :request request}))))))]
      (renewer/renew-leases-once! {:k8s-client client :claims-namespace "gpu-claims"})
      (is (= "pod-uid" (get-in @patched [:body :spec :holderIdentity])))
      (is (= core/active-reservation (get-in @patched [:body :metadata :labels core/reservation-state-label]))))))

(deftest delete-pending-lease-without-pod
  (testing "pending lease is deleted when no matching pod is found"
    (let [deleted (atom nil)
          lease   {:metadata {:name "gpu1"
                               :namespace "gpu-claims"
                               :labels {"fudo.org/gpu.uuid" "gpu1"
                                        "fudo.org/pod.namespace" "default"
                                        (name core/reservation-state-label) core/proposed-reservation}
                               :annotations {(name core/reservation-annotation) "res-123"}}
                   :spec {:holderIdentity "res-123"}}
          client  (k8s/->K8SClient
                   (reify k8s/IK8SBaseClient
                     (invoke [_ {:keys [kind action request]}]
                       (case [kind action]
                         [:Lease :list] {:items [lease]}
                         [:Pod :list] {:items []}
                         [:Lease :delete] (do (reset! deleted request) {:status 200})
                         (throw (ex-info "unexpected request" {:kind kind :action action :request request}))))))]
      (renewer/renew-leases-once! {:k8s-client client :claims-namespace "gpu-claims"})
      (is (= "gpu1" (:name @deleted)))
      (is (= "gpu-claims" (:namespace @deleted))))))

(deftest delete-active-lease-when-pod-missing
  (testing "active lease is removed when holder pod is gone"
    (let [deleted (atom nil)
          lease   {:metadata {:name "gpu1"
                               :namespace "gpu-claims"
                               :labels {"fudo.org/gpu.uuid" "gpu1"
                                        "fudo.org/pod.namespace" "default"
                                        (name core/reservation-state-label) core/active-reservation}}
                   :spec {:holderIdentity "pod-uid"}}
          client  (k8s/->K8SClient
                   (reify k8s/IK8SBaseClient
                     (invoke [_ {:keys [kind action request]}]
                       (case [kind action]
                         [:Lease :list] {:items [lease]}
                         [:Pod :list] {:items []}
                         [:Lease :delete] (do (reset! deleted request) {:status 200})
                         (throw (ex-info "unexpected request" {:kind kind :action action :request request}))))))]
      (renewer/renew-leases-once! {:k8s-client client :claims-namespace "gpu-claims"})
      (is (= "gpu1" (:name @deleted))))))
