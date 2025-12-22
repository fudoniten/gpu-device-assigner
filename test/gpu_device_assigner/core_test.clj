(ns gpu-device-assigner.core-test
  (:require [gpu-device-assigner.core :as core]
            [gpu-device-assigner.http :as http]
            [gpu-device-assigner.k8s-client :as k8s]
            [gpu-device-assigner.util :as util]
            [clojure.test :refer [deftest is testing run-tests]]))

(deftest test-handle-mutation
  (testing "Handle valid mutation request"
    (with-redefs [core/assign-device (fn [_ _] {:device-id :gpu1 :node "node1"})]
      (let [ctx {:claims-namespace "gpu-claims"}
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
    (let [ctx {:claims-namespace "gpu-claims"}
          handle-mutation-fn (http/handle-mutation ctx)
          request {:kind "UnexpectedKind"
                   :request {:uid "123abc"}}
          response (handle-mutation-fn request)]
      (is (= "AdmissionReview" (:kind response)))
      (is (= "123abc" (get-in response [:response :uid])))
      (is (false? (get-in response [:response :allowed])))))

  (testing "Handle request where no device can be assigned"
    (with-redefs [core/assign-device (constantly nil)]
      (let [ctx {:claims-namespace "gpu-claims"}
            handle-mutation-fn (http/handle-mutation ctx)
            request {:kind "AdmissionReview"
                     :request {:uid "123abc"
                               :object {:metadata {:name "test-pod"
                                                   :namespace "default"
                                                   :labels {:fudo.org/gpu.test "true"}},
                                        :spec {:nodeName "node1"}}}}
            response (handle-mutation-fn request)]
        (is (= "AdmissionReview" (:kind response)))
        (is (= "123abc" (get-in response [:response :uid])))
        (is (false? (get-in response [:response :allowed])))))))

(let [mock-k8s-client (fn [& {:keys [gpu-label-map]
                             :or   {gpu-label-map {:gpu1 #{:fudo.org/gpu.test}}}}]
                        (k8s/->K8SClient
                         (reify k8s/IK8SBaseClient
                           (invoke [_ {:keys [kind action request]}]
                             (let [node {:metadata {:name "node1"
                                                    :annotations {:fudo.org/gpu.device.labels
                                                                  (util/base64-encode (util/try-json-generate gpu-label-map))}}}]
                               (case [kind action]
                                 [:Node :list] {:items [node]}
                                 [:Node :get]  node))))))]
    (testing "Fail if no matching device is found"
      (let [ctx {:claims-namespace "gpu-claims" :k8s-client (mock-k8s-client)}
            result (core/assign-device ctx {:node "node1" :pod "test-pod" :namespace "default" :requested-labels #{:fudo.org/gpu.other}})]
        (is (nil? result))))

    (testing "Succeed when a matching device can be claimed"
      (with-redefs [core/try-claim-uuid! (fn [_ _ _] true)]
        (let [ctx {:claims-namespace "gpu-claims" :k8s-client (mock-k8s-client)}
              result (core/assign-device ctx {:node "node1" :pod "test-pod" :namespace "default" :requested-labels #{:fudo.org/gpu.test}})]
          (is (= "gpu1" (:device-id result)))
          (is (= "node1" (:node result))))))

    (testing "Fail when a matching device cannot be claimed"
      (with-redefs [core/try-claim-uuid! (fn [_ _ _] false)]
        (let [ctx {:claims-namespace "gpu-claims" :k8s-client (mock-k8s-client)}
              result (core/assign-device ctx {:node "node1" :pod "test-pod" :namespace "default" :requested-labels #{:fudo.org/gpu.test}})]
          (is (nil? result))))))

(run-tests 'gpu-device-assigner.core-test)
