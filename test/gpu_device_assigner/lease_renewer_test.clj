(ns gpu-device-assigner.lease-renewer-test
  (:require [clojure.test :refer [deftest is testing]]
            [gpu-device-assigner.core :as core]
            [gpu-device-assigner.lease-renewer :as renewer]
            [gpu-device-assigner.test-support :as ts]
            [gpu-device-assigner.time :as gtime])
  (:import [java.time Instant]))

(defn- lease [& {:keys [state holder reservation age rv] :or {age 10 rv "5"}}]
  {:metadata {:name            "gpu1"
              :namespace       "gpu-claims"
              :resourceVersion rv
              :labels          {:fudo.org/gpu.uuid              "gpu1"
                                :fudo.org/pod.namespace         "default"
                                :fudo.org/gpu.reservation-state state}
              :annotations     (cond-> {}
                                 reservation (assoc :fudo.org/gpu.reservation-id reservation))}
   :spec     {:holderIdentity holder
              :renewTime      (gtime/now-rfc3339-micro-at (.minusSeconds (Instant/now) age))}})

(defn- run-pass [routes]
  (let [calls (atom [])]
    (renewer/renew-leases-once! {:k8s-client       (ts/mock-client routes calls)
                                 :claims-namespace "gpu-claims"})
    calls))

(deftest finalize-pending-lease-when-pod-active
  (let [l     (lease :state core/proposed-reservation :holder "res-123" :reservation "res-123")
        pod   {:metadata {:uid "pod-uid" :name "demo" :namespace "default"
                          :annotations {:fudo.org/gpu.reservation-id "res-123"
                                        :fudo.org/gpu.uuid           "gpu1"
                                        :cdi.k8s.io/gpu-assignment   "nvidia.com/gpu=UUID=gpu1"}}
               :status   {:phase "Running"}}
        calls (run-pass {[:Lease :list]             {:items [l]}
                         [:Pod :list]               {:items [pod]}
                         [:Lease :get]              l
                         [:Lease :patch/json-merge] {:kind "Lease"}})
        body  (-> (ts/calls-of calls :Lease :patch/json-merge) first :request :body)]
    (is (= "pod-uid" (get-in body [:spec :holderIdentity])))
    (is (= core/active-reservation (get-in body [:metadata :labels "fudo.org/gpu.reservation-state"])))))

(deftest delete-old-pending-lease-without-pod
  (let [calls   (run-pass {[:Lease :list]   {:items [(lease :state core/proposed-reservation
                                                            :holder "res-123" :reservation "res-123"
                                                            :age 1000 :rv "9")]}
                           [:Pod :list]     {:items []}
                           [:Lease :delete] {:kind "Status"}})
        deleted (-> (ts/calls-of calls :Lease :delete) first :request)]
    (is (= "gpu1" (:name deleted)))
    (is (= "gpu-claims" (:namespace deleted)))
    (testing "the delete is conditional on the resourceVersion we read"
      (is (= "9" (get-in deleted [:body :preconditions :resourceVersion]))))))

(deftest young-pending-lease-without-pod-is-kept
  (testing "the pod may not have been persisted yet"
    (let [calls (run-pass {[:Lease :list] {:items [(lease :state core/proposed-reservation
                                                          :holder "res-123" :reservation "res-123"
                                                          :age 5)]}
                           [:Pod :list]   {:items []}})]
      (is (empty? (ts/calls-of calls :Lease :delete))))))

(deftest delete-active-lease-when-pod-missing
  (let [calls (run-pass {[:Lease :list]   {:items [(lease :state core/active-reservation :holder "pod-uid")]}
                         [:Pod :list]     {:items []}
                         [:Lease :delete] {:kind "Status"}})]
    (is (= "gpu1" (-> (ts/calls-of calls :Lease :delete) first :request :name)))))

(deftest renew-active-lease-when-pod-running
  (let [calls (run-pass {[:Lease :list]             {:items [(lease :state core/active-reservation :holder "pod-uid")]}
                         [:Pod :list]               {:items [{:metadata {:uid "pod-uid" :name "demo"}
                                                              :status   {:phase "Running"}}]}
                         [:Lease :patch/json-merge] {:kind "Lease"}})]
    (is (= 1 (count (ts/calls-of calls :Lease :patch/json-merge))))
    (is (empty? (ts/calls-of calls :Lease :delete)))))

(deftest api-errors-never-delete-leases
  (testing "a failed pod list is 'unknown', not 'no pods'"
    (let [calls (run-pass {[:Lease :list] {:items [(lease :state core/active-reservation :holder "pod-uid")
                                                   (lease :state core/proposed-reservation :holder "r"
                                                          :reservation "r" :age 1000)]}
                           [:Pod :list]   (ts/error 500)})]
      (is (empty? (ts/calls-of calls :Lease :delete)))
      (testing "and the pod list is fetched once per namespace per pass"
        (is (= 1 (count (ts/calls-of calls :Pod :list))))))))
