(ns gpu-device-assigner.core-test
  (:require [clojure.test :refer [deftest is testing]]
            [gpu-device-assigner.core :as core]
            [gpu-device-assigner.http :as http]
            [gpu-device-assigner.lease-renewer :as renewer]
            [gpu-device-assigner.test-support :as ts]
            [gpu-device-assigner.time :as gtime]
            [gpu-device-assigner.util :as util])
  (:import [java.time Instant]))

;;;; ==== util

(deftest test-base64-encode
  (is (= "SGVsbG8gd29ybGQ=" (util/base64-encode "Hello world"))))

(deftest test-base64-decode
  (is (= "Hello world" (String. (util/base64-decode "SGVsbG8gd29ybGQ=") "UTF-8"))))

(deftest test-map-vals
  (is (= {:a 2 :b 3} (util/map-vals inc {:a 1 :b 2}))))

(deftest test-parse-json
  (is (= {:key "value"} (util/parse-json "{\"key\": \"value\"}"))))

(deftest test-full-name
  (is (= "fudo.org/gpu.x" (util/full-name :fudo.org/gpu.x)))
  (is (= "plain" (util/full-name :plain)))
  (is (= "already/string" (util/full-name "already/string"))))

;;;; ==== helpers

(defn- seconds-ago [n]
  (gtime/now-rfc3339-micro-at (.minusSeconds (Instant/now) n)))

(defn- lease
  [& {:keys [device holder state pod-ns age rv reservation]
      :or   {device "gpu1" pod-ns "default" age 10 rv "100"}}]
  {:metadata {:name            device
              :resourceVersion rv
              :labels          (cond-> {:fudo.org/gpu.uuid      device
                                        :fudo.org/pod.namespace pod-ns}
                                 state (assoc :fudo.org/gpu.reservation-state state))
              :annotations     (cond-> {}
                                 reservation (assoc :fudo.org/gpu.reservation-id reservation))}
   :spec     {:holderIdentity       holder
              :leaseDurationSeconds 300
              :renewTime            (seconds-ago age)}})

(defn- running-pod [& {:keys [uid reservation phase deleting?] :or {phase "Running"}}]
  {:metadata (cond-> {:uid uid :name "holder" :namespace "default"
                      :annotations (cond-> {}
                                     reservation (assoc :fudo.org/gpu.reservation-id reservation))}
               deleting? (assoc :deletionTimestamp "2026-01-01T00:00:00Z"))
   :status   {:phase phase}})

(defn- claim-ctx [routes calls]
  {:k8s-client       (ts/mock-client routes calls)
   :claims-namespace "gpu-claims"
   :namespace        "default"
   :pod              "new-pod"})

(defn- conflicting-claim
  "Run try-claim-uuid! against an existing lease and pod list; return
   [result calls]."
  [existing-lease pods-response & {:keys [patch-response] :or {patch-response {:kind "Lease"}}}]
  (let [calls (atom [])
        ctx   (claim-ctx {[:Lease :create]     (ts/error 409)
                          [:Lease :get]        existing-lease
                          [:Pod :list]         pods-response
                          [:Lease :patch/json] patch-response}
                         calls)]
    [(core/try-claim-uuid! ctx :gpu1 "new-reservation") calls]))

;;;; ==== lease claiming

(deftest create-lease-body
  (let [calls (atom [])
        ctx   (claim-ctx {[:Lease :create] {:kind "Lease"}} calls)]
    (is (true? (core/try-claim-uuid! ctx :gpu1 "reservation-uid")))
    (let [body (-> (ts/calls-of calls :Lease :create) first :request :body)]
      (is (= "default" (get-in body [:metadata :labels "fudo.org/pod.namespace"])))
      (is (= "gpu1" (get-in body [:metadata :labels "fudo.org/gpu.uuid"])))
      (is (= "proposal" (get-in body [:metadata :labels "fudo.org/gpu.reservation-state"])))
      (is (= "reservation-uid" (get-in body [:metadata :annotations "fudo.org/gpu.reservation-id"])))
      (is (= core/reservation-lease-seconds (get-in body [:spec :leaseDurationSeconds]))))))

(deftest conflict-with-live-active-holder-is-refused
  (testing "an active lease whose pod is running is never taken over, even if expired"
    (let [[result calls] (conflicting-claim (lease :holder "pod-uid" :state "active" :age 10000)
                                            {:items [(running-pod :uid "pod-uid")]})]
      (is (false? result))
      (is (empty? (ts/calls-of calls :Lease :patch/json))))))

(deftest conflict-with-gone-holder-is-taken-over-atomically
  (let [[result calls] (conflicting-claim (lease :holder "gone-uid" :state "active" :rv "42")
                                          {:items [(running-pod :uid "someone-else")]})
        patch (-> (ts/calls-of calls :Lease :patch/json) first :request :body util/try-json-parse)
        op    (fn [path] (some #(when (= path (:path %)) %) patch))]
    (is (true? result))
    (testing "the takeover is guarded by a resourceVersion test"
      (is (= {:op "test" :path "/metadata/resourceVersion" :value "42"} (first patch))))
    (testing "identifying labels survive and the annotation key is fully qualified"
      (is (= "gpu1" (get-in (op "/metadata/labels") [:value :fudo.org/gpu.uuid])))
      (is (= "default" (get-in (op "/metadata/labels") [:value :fudo.org/pod.namespace])))
      (is (= "proposal" (get-in (op "/metadata/labels") [:value :fudo.org/gpu.reservation-state])))
      (is (= "new-reservation" (get-in (op "/metadata/annotations") [:value :fudo.org/gpu.reservation-id]))))
    (is (= "new-reservation" (get-in (op "/spec") [:value :holderIdentity])))))

(deftest conflict-with-terminated-holder-is-taken-over
  (let [[result _] (conflicting-claim (lease :holder "pod-uid" :state "active")
                                      {:items [(running-pod :uid "pod-uid" :phase "Succeeded")]})]
    (is (true? result))))

(deftest lost-race-on-takeover
  (testing "if the guarded patch is rejected, the claim fails"
    (let [[result _] (conflicting-claim (lease :holder "gone-uid" :state "active")
                                        {:items []}
                                        :patch-response (ts/error 422))]
      (is (false? result)))))

(deftest young-proposal-is-protected
  (testing "a proposal inside the grace period is held even with no pod yet"
    (let [[result calls] (conflicting-claim (lease :holder "req-1" :state "proposal" :age 5
                                                   :reservation "req-1")
                                            {:items []})]
      (is (false? result))
      (is (empty? (ts/calls-of calls :Lease :patch/json))))))

(deftest old-proposal-with-pod-is-protected
  (testing "a proposal whose pod exists (found by reservation annotation) is held"
    (let [[result _] (conflicting-claim (lease :holder "req-1" :state "proposal" :age 1000
                                               :reservation "req-1")
                                        {:items [(running-pod :uid "pod-uid" :reservation "req-1")]})]
      (is (false? result)))))

(deftest old-orphan-proposal-is-taken-over
  (let [[result _] (conflicting-claim (lease :holder "req-1" :state "proposal" :age 1000
                                             :reservation "req-1")
                                      {:items []})]
    (is (true? result))))

(deftest pod-list-error-means-held
  (testing "an API error looking up the holder never frees the lease"
    (let [[result calls] (conflicting-claim (lease :holder "pod-uid" :state "active" :age 10000)
                                            (ts/error 500))]
      (is (false? result))
      (is (empty? (ts/calls-of calls :Lease :patch/json))))))

;;;; ==== device selection

(deftest pick-devices-tries-every-device-on-a-node
  (testing "one held GPU on a node does not stop the other being claimed"
    (dotimes [_ 10]
      (let [calls (atom [])
            ctx   (claim-ctx {[:Node :list]    {:items [(ts/gpu-node "node1" {"gpu1" ["fudo.org/gpu.a"]
                                                                              "gpu2" ["fudo.org/gpu.a"]})]}
                              [:Lease :create] (fn [req]
                                                 (if (= "gpu1" (get-in req [:body :metadata :name]))
                                                   (ts/error 409)
                                                   {:kind "Lease"}))
                              [:Lease :get]    (lease :device "gpu1" :holder "pod-uid" :state "active")
                              [:Pod :list]     {:items [(running-pod :uid "pod-uid")]}}
                             calls)]
        (is (= [{:device-id :gpu2 :node "node1"}]
               (core/pick-devices ctx "res" #{:fudo.org/gpu.a} 1)))))))

(deftest pick-devices-prefers-best-fit
  (testing "the least versatile matching card is handed out first"
    (dotimes [_ 10]
      (let [calls (atom [])
            ctx   (claim-ctx {[:Node :list]    {:items [(ts/gpu-node "big" {"v100" ["fudo.org/gpu.tts" "fudo.org/gpu.ollama"
                                                                                    "fudo.org/gpu.stt"]})
                                                        (ts/gpu-node "small" {"p4" ["fudo.org/gpu.tts"]})]}
                              [:Lease :create] {:kind "Lease"}}
                             calls)]
        (is (= [{:device-id :p4 :node "small"}]
               (core/pick-devices ctx "res" #{:fudo.org/gpu.tts} 1)))))))

(deftest pick-devices-skips-unassignable-nodes
  (doseq [[desc node] [["not ready" (ts/gpu-node "n" {"gpu1" ["fudo.org/gpu.a"]} :ready? false)]
                       ["cordoned"  (ts/gpu-node "n" {"gpu1" ["fudo.org/gpu.a"]} :unschedulable? true)]
                       ["no allocatable GPUs" (ts/gpu-node "n" {"gpu1" ["fudo.org/gpu.a"]} :allocatable 0)]]]
    (testing desc
      (let [calls (atom [])
            ctx   (claim-ctx {[:Node :list]    {:items [node]}
                              [:Lease :create] {:kind "Lease"}}
                             calls)]
        (is (nil? (core/pick-devices ctx "res" #{:fudo.org/gpu.a} 1)))
        (is (empty? (ts/calls-of calls :Lease :create)))))))

(deftest pick-devices-rolls-back-partial-multi-gpu-claims
  (let [calls (atom [])
        ctx   (claim-ctx {[:Node :list]    {:items [(ts/gpu-node "node1" {"gpu1" ["fudo.org/gpu.a"]
                                                                          "gpu2" ["fudo.org/gpu.a"]})]}
                          [:Lease :create] (fn [req]
                                             (if (= "gpu1" (get-in req [:body :metadata :name]))
                                               (ts/error 409)
                                               {:kind "Lease"}))
                          [:Lease :get]    (lease :device "gpu1" :holder "pod-uid" :state "active")
                          [:Pod :list]     {:items [(running-pod :uid "pod-uid")]}
                          [:Lease :delete] {:kind "Status"}}
                         calls)]
    (is (nil? (core/pick-devices ctx "res" #{:fudo.org/gpu.a} 2)))
    (is (= ["gpu2"] (map (comp :name :request) (ts/calls-of calls :Lease :delete))))))

(deftest assign-device-without-match
  (let [ctx (claim-ctx {[:Node :list] {:items [(ts/gpu-node "node1" {"gpu1" ["fudo.org/gpu.test"]})]}}
                       (atom []))]
    (is (nil? (core/assign-device ctx {:pod "p" :namespace "default"
                                       :requested-labels #{:fudo.org/gpu.other}
                                       :reservation-id "res-1"})))))

(deftest assign-device-with-match
  (let [ctx (claim-ctx {[:Node :list]    {:items [(ts/gpu-node "node1" {"gpu1" ["fudo.org/gpu.test"]})]}
                        [:Lease :create] {:kind "Lease"}}
                       (atom []))
        result (core/assign-device ctx {:pod "p" :namespace "default"
                                        :requested-labels #{:fudo.org/gpu.test}
                                        :reservation-id "res-1"})]
    (is (= [{:device-id :gpu1 :node "node1"}] (:devices result)))
    (is (= "node1" (:node result)))))

;;;; ==== admission handling

(defn- review [uid metadata spec]
  {:kind    "AdmissionReview"
   :request {:uid uid :object {:metadata metadata :spec spec}}})

(def ^:private one-gpu {:containers [{:name "app" :resources {:requests {:nvidia.com/gpu "1"}}}]})

(defn- decode-patch [response]
  (-> response :response :patch util/base64-decode String. util/try-json-parse))

(deftest test-handle-mutation
  (with-redefs [core/assign-device (fn [_ _] {:devices [{:device-id :gpu1 :node "node1"}]
                                              :node "node1"
                                              :reservation-id "res-123"})]
    (testing "valid request without annotations"
      (let [response (-> ((http/handle-mutation {})
                          (review "123abc" {:generateName "test-pod" :namespace "default"
                                            :labels {:fudo.org/gpu.test "true"}}
                                  one-gpu)))
            patch    (decode-patch response)]
        (is (= "123abc" (get-in response [:response :uid])))
        (is (true? (get-in response [:response :allowed])))
        (is (= "JSONPatch" (get-in response [:response :patchType])))
        (is (= {:op "add" :path "/metadata/annotations" :value {}} (first patch)))
        (is (= "gpu1" (:value (some #(when (= "/metadata/annotations/fudo.org~1gpu.uuid" (:path %)) %) patch))))
        (is (= "node1" (:value (some #(when (= "/spec/nodeName" (:path %)) %) patch))))))

    (testing "existing pod annotations are not replaced"
      (let [patch (decode-patch ((http/handle-mutation {})
                                 (review "1" {:name "p" :namespace "default"
                                              :labels {:fudo.org/gpu.test "true"}
                                              :annotations {:prometheus.io/scrape "true"}}
                                         one-gpu)))]
        (is (not-any? #(= "/metadata/annotations" (:path %)) patch))))))

(deftest unexpected-kind
  (let [response ((http/handle-mutation {}) {:kind "UnexpectedKind" :request {:uid "123abc"}})]
    (is (= "123abc" (get-in response [:response :uid])))
    (is (false? (get-in response [:response :allowed])))))

(deftest no-device-available
  (with-redefs [core/assign-device (constantly nil)]
    (let [response ((http/handle-mutation {})
                    (review "123abc" {:name "test-pod" :namespace "default"
                                      :labels {:fudo.org/gpu.test "true"}}
                            one-gpu))]
      (is (false? (get-in response [:response :allowed])))
      (is (= 429 (get-in response [:response :status :code]))))))

(deftest gpu-counting
  (is (= 1 (http/nvidia-gpu-count one-gpu)))
  (testing "init containers count as max(init, sum(app)), not a sum"
    (is (= 1 (http/nvidia-gpu-count {:initContainers [{:resources {:requests {:nvidia.com/gpu "1"}}}]
                                     :containers     [{:resources {:requests {:nvidia.com/gpu "1"}}}]}))))
  (testing "multiple app containers sum"
    (is (= 2 (http/nvidia-gpu-count {:containers [{:resources {:requests {:nvidia.com/gpu "1"}}}
                                                  {:resources {:requests {:nvidia.com/gpu 1}}}]}))))
  (testing "sidecar (restartable) init containers add to the app total"
    (is (= 2 (http/nvidia-gpu-count {:initContainers [{:restartPolicy "Always"
                                                       :resources {:requests {:nvidia.com/gpu "1"}}}]
                                     :containers     [{:resources {:requests {:nvidia.com/gpu "1"}}}]}))))
  (testing "limits are used when requests are absent"
    (is (= 1 (http/nvidia-gpu-count {:containers [{:resources {:limits {:nvidia.com/gpu "1"}}}]})))))

(deftest false-labels-are-not-requirements
  (let [seen (atom nil)]
    (with-redefs [core/assign-device (fn [_ req] (reset! seen req)
                                       {:devices [{:device-id :gpu1 :node "n"}] :reservation-id "r"})]
      ((http/handle-mutation {})
       (review "1" {:name "p" :namespace "default"
                    :labels {:fudo.org/gpu.assign "true"
                             :fudo.org/gpu.suitable-stt "true"
                             :fudo.org/gpu.fp16 "false"}}
               one-gpu))
      (is (= #{:fudo.org/gpu.suitable-stt} (:requested-labels @seen))))))

(deftest opted-in-pod-without-gpu-request-gets-no-lease
  (let [called (atom false)]
    (with-redefs [core/assign-device (fn [_ _] (reset! called true) nil)]
      (let [response ((http/handle-mutation {})
                      (review "1" {:name "p" :namespace "default"
                                   :labels {:fudo.org/gpu.assign "true" :fudo.org/gpu.test "true"}}
                              {:containers [{:name "app"}]}))]
        (is (true? (get-in response [:response :allowed])))
        (is (nil? (get-in response [:response :patch])))
        (is (false? @called))))))

;;;; ==== renewer / inventory

(deftest finalize-reservation-promotes-lease
  (let [calls (atom [])
        l     (lease :holder "res-123" :state "proposal" :reservation "res-123" :rv "7")
        ctx   {:k8s-client       (ts/mock-client {[:Lease :get]              l
                                                  [:Lease :patch/json-merge] {:kind "Lease"}}
                                                 calls)
               :claims-namespace "gpu-claims"}]
    (renewer/finalize-reservation! ctx {:reservation-id "res-123" :device-id "gpu1"
                                        :namespace "default" :pod-uid "pod-uid" :name "demo"})
    (let [{:keys [body namespace]} (-> (ts/calls-of calls :Lease :patch/json-merge) first :request)]
      (is (= "gpu-claims" namespace))
      (is (= "pod-uid" (get-in body [:spec :holderIdentity])))
      (is (= core/default-lease-seconds (get-in body [:spec :leaseDurationSeconds])))
      (is (= "active" (get-in body [:metadata :labels "fudo.org/gpu.reservation-state"])))
      (is (= "7" (get-in body [:metadata :resourceVersion])))
      (is (nil? (get-in body [:metadata :ownerReferences]))))))

(deftest device-inventory-shows-assignments
  (let [ctx {:k8s-client (ts/mock-client
                          {[:Node :list]  {:items [(ts/gpu-node "node1" {"gpu1" ["label1"]})]}
                           [:Lease :list] {:items [(lease :holder "pod-uid" :state "active")]}
                           [:Pod :list]   {:items [{:metadata {:uid "pod-uid" :name "demo"
                                                               :namespace "default"}}]}})
             :claims-namespace "gpu-claims"}]
    (is (= {:node "node1"
            :labels [:label1]
            :assignment {:name "demo" :namespace "default" :uid "pod-uid"
                         :exists? true :state "active"}}
           (:gpu1 (core/device-inventory ctx))))))
