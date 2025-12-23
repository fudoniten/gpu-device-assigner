(ns gpu-device-assigner.core-test
  (:require [gpu-device-assigner.core :as core]
            [gpu-device-assigner.http :as http]
            [gpu-device-assigner.logging :as log]
            [gpu-device-assigner.k8s-client :as k8s]
            [gpu-device-assigner.util :as util]
            [clojure.test :refer [deftest is testing run-tests]]))

(deftest test-base64-encode
  (testing "Base64 encoding"
    (is (= "SGVsbG8gd29ybGQ=" (util/base64-encode "Hello world")))))

(deftest test-base64-decode
  (testing "Base64 decoding"
    (is (= "Hello world" (String. (util/base64-decode "SGVsbG8gd29ybGQ=") "UTF-8")))))

(deftest test-map-vals
  (testing "Applying a function to all values in a map"
    (is (= {:a 2 :b 3} (util/map-vals inc {:a 1 :b 2})))))

(deftest test-parse-json
  (testing "Parsing JSON string into Clojure data structure"
    (is (= {:key "value"} (util/parse-json "{\"key\": \"value\"}")))))

(deftest test-handle-mutation
  (let [mock-logger (reify log/Logger
                      (fatal [_ _])
                      (error [_ _])
                      (warn  [_ _])
                      (info  [_ _])
                      (debug [_ _]))
        mock-k8s-client (fn [& {:keys [gpu-label-map gpu-reservations]
                               :or   {gpu-label-map {:gpu1 #{:fudo.org/gpu.test}}
                                      gpu-reservations {:gpu1 {:pod "other-pod" :namespace "default"}}}}]
                          (k8s/->K8SClient
                           (reify k8s/IK8SBaseClient
                             (invoke [_ {:keys [kind action request]}]
                               (let [node {:metadata {:name "node1"
                                                      :annotations {:fudo.org/gpu.device.labels
                                                                    (util/base64-encode (util/try-json-generate gpu-label-map))
                                                                    :fudo.org/gpu.device.reservations
                                                                    (util/base64-encode (util/try-json-generate gpu-reservations))}}}]
                                 (case [kind action]
                                   [:Node :list] {:items [node]}
                                   [:Node :get]  node
                                   [:Pod :get] (if (= "other-pod" (:name request))
                                                 {:name (:name request) :namespace (:namespace request)}
                                                 (throw (ex-info "Not found" {:type :not-found})))
                                   [:Node :patch/json] true))))
                           mock-logger))]
    (testing "Handle valid mutation request"
      (with-redefs [core/assign-device (fn [_ _] {:device-id :gpu1 :node "node1"})]
        (let [ctx {:logger mock-logger}
              handle-mutation-fn (http/handle-mutation ctx)
              request {:kind "AdmissionReview"
                       :request {:uid "123abc"
                                 :object {:metadata {:generateName "test-pod"
                                                     :namespace "default"
                                                     :labels {:fudo.org/gpu.test "true"}}}}}
              response (handle-mutation-fn request)
              patch-body (-> response :response :patch util/base64-decode String. util/try-json-parse)]
          (is (= "AdmissionReview" (:kind response)))
          (is (= "123abc" (get-in response [:response :uid])))
          (is (= true (get-in response [:response :allowed])))
          (is (= "JSONPatch" (get-in response [:response :patchType])))
          (is (= "gpu1" (get-in (nth patch-body 1) [:value])))
          (is (= "/metadata/annotations/fudo.org~1gpu.uuid" (get-in (nth patch-body 1) [:path]))))))

    (testing "Handle request with unexpected kind"
      (let [ctx {:logger mock-logger}
            handle-mutation-fn (http/handle-mutation ctx)
            request {:kind "UnexpectedKind"
                     :request {:uid "123abc"}}
            response (handle-mutation-fn request)]
        (is (= "AdmissionReview" (:kind response)))
        (is (= "123abc" (get-in response [:response :uid])))
        (is (= false (get-in response [:response :allowed])))))

    (testing "Handle request where no device can be assigned"
      (with-redefs [core/assign-device (constantly nil)]
        (let [ctx {:logger mock-logger}
              handle-mutation-fn (http/handle-mutation ctx)
              request {:kind "AdmissionReview"
                       :request {:uid "123abc"
                                 :object {:metadata {:name "test-pod"
                                                     :namespace "default"
                                                     :labels {:fudo.org/gpu.test "true"}},
                                          :spec {:nodeName "node1"}}}}}
              response (handle-mutation-fn request)]
          (is (= "AdmissionReview" (:kind response)))
          (is (= "123abc" (get-in response [:response :uid])))
          (is (= false (get-in response [:response :allowed]))))))))

(let [mock-logger (reify log/Logger
                    (fatal [_ _])
                    (error [_ _])
                    (warn  [_ _])
                    (info  [_ _])
                    (debug [_ _]))
      mock-k8s-client (fn [& {:keys [gpu-label-map gpu-reservations]
                             :or   {gpu-label-map {:gpu1 #{:fudo.org/gpu.test}}
                                    gpu-reservations {}}}]
                        (k8s/->K8SClient
                         (reify k8s/IK8SBaseClient
                           (invoke [_ {:keys [kind action request]}]
                             (let [node {:metadata {:annotations {:fudo.org/gpu.device.labels
                                                                  (util/base64-encode (util/try-json-generate gpu-label-map))
                                                                  :fudo.org/gpu.device.reservations
                                                                  (util/base64-encode (util/try-json-generate gpu-reservations))
}}}]
                               (case [kind action]
                                 [:Node :list] {:items [node]}
                                 [:Node :get]  node))))
                         mock-logger))]
  (testing "Fail if no matching device is found"
    (let [ctx {:logger mock-logger :k8s-client (mock-k8s-client)}
          result (core/assign-device ctx {:node "node1" :pod "test-pod" :namespace "default" :requested-labels #{:fudo.org/gpu.other}})]
      (is (nil? result))))

  (testing "Succeed when a matching device can be claimed"
    (with-redefs [core/try-claim-uuid! (fn [_ _ _] true)]
      (let [ctx {:logger mock-logger :k8s-client (mock-k8s-client)}
            result (core/assign-device ctx {:node "node1" :pod "test-pod" :namespace "default" :requested-labels #{:fudo.org/gpu.test}})]
        (is (= "gpu1" (:device-id result)))
        (is (= "node1" (:node result))))))

  (testing "Succeed if a matching device is found, and is reserved but by a pod that no longer exists"
    (let [ctx {:logger mock-logger
               :k8s-client (k8s/->K8SClient
                            (reify k8s/IK8SBaseClient
                              (invoke [_ {:keys [kind action]}]
                                (case [kind action]
                                  [:Node :list] [{:metadata {:annotations {:fudo.org/gpu.device.labels
                                                                           (util/base64-encode (util/try-json-generate {"gpu1" #{"label1"}}))
                                                                           :fudo.org/gpu.device.reservations
                                                                           (util/try-json-generate {"gpu1" {:pod "nonexistent-pod" :namespace "default"}})}}}]
                                  [:Pod :get] (throw (ex-info "Not found" {:type :not-found}))
                                  [:Node :patch/json] true))}
                            mock-logger)}
          result (core/assign-device ctx {:node "node1" :pod "test-pod" :namespace "default" :requested-labels #{"label1"}})]
      (is (= "gpu1" result)))))

(run-tests 'gpu-device-assigner.core-test)
