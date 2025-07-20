(ns gpu-device-assigner.core-test
  (:require [gpu-device-assigner.core :as core]
            [gpu-device-assigner.logging :as log]
            [gpu-device-assigner.k8s-client :as k8s]
            [clojure.test :refer [deftest is testing run-tests]]))

(deftest test-base64-encode
  (testing "Base64 encoding"
    (is (= "SGVsbG8gd29ybGQ=" (core/base64-encode "Hello world")))))

(deftest test-base64-decode
  (testing "Base64 decoding"
    (is (= "Hello world" (String. (core/base64-decode "SGVsbG8gd29ybGQ=") "UTF-8")))))

(deftest test-map-vals
  (testing "Applying a function to all values in a map"
    (is (= {:a 2 :b 3} (core/map-vals inc {:a 1 :b 2})))))

(deftest test-parse-json
  (testing "Parsing JSON string into Clojure data structure"
    (is (= {:key "value"} (core/parse-json "{\"key\": \"value\"}")))))

(deftest test-handle-mutation
  (let [mock-logger (reify log/Logger
                      (fatal [_ _])
                      (error [_ _])
                      (warn  [_ _])
                      (info  [_ _])
                      (debug [_ _]))
        mock-k8s-client (fn [& {:keys [gpu-label-map gpu-reservations]
                               :or   {gpu-label-map {:gpu1 #{"label1"}}
                                      gpu-reservations {:gpu1 {:pod "other-pod" :namespace "default"}}}}]
                          (k8s/->K8SClient
                           (reify k8s/IK8SBaseClient
                             (invoke [_ {:keys [kind action request]}]
                               (case [kind action]
                                 [:Node :get] {:metadata {:annotations {:fudo.org/gpu.device.labels
                                                                        (core/base64-encode (core/try-json-generate gpu-label-map))
                                                                        :fudo.org/gpu.device.reservations
                                                                        (core/try-json-generate gpu-reservations)}}}
                                 [:Pod :get] (if (= "other-pod" (:name request))
                                               {:name (:name request) :namespace (:namespace request)}
                                               (throw (ex-info "Not found" {:type :not-found})))
                                 [:Node :patch/json] true)))))]
    (testing "Handle valid mutation request"
      (let [ctx {:logger mock-logger :k8s-client (mock-k8s-client :gpu-reservations {})}
            handle-mutation-fn (core/handle-mutation ctx)
            request {:kind "AdmissionReview"
                     :request {:uid "123abc"
                               :object {:metadata {:generateName "test-pod"
                                                   :namespace "default"
                                                   :annotations {"label1" "true"}}
                                        :spec {:nodeName "node1"}}}}
            response (handle-mutation-fn request)]
        (is (= "AdmissionReview" (:kind response)))
        (is (= "123abc" (get-in response [:response :uid])))
        (is (= true (get-in response [:response :allowed])))))

    (testing "Handle request with unexpected kind"
      (let [ctx {:logger mock-logger :k8s-client (mock-k8s-client)}
            handle-mutation-fn (core/handle-mutation ctx)
            request {:kind "UnexpectedKind"
                     :request {:uid "123abc"}}
            response (handle-mutation-fn request)]
        (is (= "AdmissionReview" (:kind response)))
        (is (= "123abc" (get-in response [:response :uid])))
        (is (= false (get-in response [:response :allowed])))))

    (testing "Handle request where no device can be assigned"
      (let [ctx {:logger mock-logger :k8s-client (mock-k8s-client)}
            handle-mutation-fn (core/handle-mutation ctx)
            request {:kind "AdmissionReview"
                     :request {:uid "123abc"
                               :object {:metadata {:generateName "test-pod"
                                                   :namespace "default"
                                                   :annotations {"nonexistent-label" "true"}}
                                        :spec {:nodeName "node1"}}}}
            response (handle-mutation-fn request)]
        (is (= "AdmissionReview" (:kind response)))
        (is (= "123abc" (get-in response [:response :uid])))
        (is (= false (get-in response [:response :allowed])))))))

(deftest test-handle-mutation
  (let [mock-logger (reify log/Logger
                      (fatal [_ _])
                      (error [_ _])
                      (warn  [_ _])
                      (info  [_ _])
                      (debug [_ _]))
        mock-k8s-client (fn [& {:keys [gpu-label-map gpu-reservations]
                               :or   {gpu-label-map {:gpu1 #{"label1"}}
                                      gpu-reservations {:gpu1 {:pod "other-pod" :namespace "default"}}}}]
                          (k8s/->K8SClient
                           (reify k8s/IK8SBaseClient
                             (invoke [_ {:keys [kind action request]}]
                               (case [kind action]
                                 [:Node :get] {:metadata {:annotations {:fudo.org/gpu.device.labels
                                                                        (core/base64-encode (core/try-json-generate gpu-label-map))
                                                                        :fudo.org/gpu.device.reservations
                                                                        (core/try-json-generate gpu-reservations)}}}
                                 [:Pod :get] (if (= "other-pod" (:name request))
                                               {:name (:name request) :namespace (:namespace request)}
                                               (throw (ex-info "Not found" {:type :not-found})))
                                 [:Node :patch/json] true)))))]
    (testing "Handle valid mutation request"
      (let [ctx {:logger mock-logger :k8s-client (mock-k8s-client :gpu-reservations {})}
            handle-mutation-fn (core/handle-mutation ctx)
            request {:kind "AdmissionReview"
                     :request {:uid "123abc"
                               :object {:metadata {:generateName "test-pod"
                                                   :namespace "default"
                                                   :annotations {"label1" "true"}}
                                        :spec {:nodeName "node1"}}}}
            response (handle-mutation-fn request)]
        (is (= "AdmissionReview" (:kind response)))
        (is (= "123abc" (get-in response [:response :uid])))
        (is (= true (get-in response [:response :allowed])))))

    (testing "Handle request with unexpected kind"
      (let [ctx {:logger mock-logger :k8s-client (mock-k8s-client)}
            handle-mutation-fn (core/handle-mutation ctx)
            request {:kind "UnexpectedKind"
                     :request {:uid "123abc"}}
            response (handle-mutation-fn request)]
        (is (= "AdmissionReview" (:kind response)))
        (is (= "123abc" (get-in response [:response :uid])))
        (is (= false (get-in response [:response :allowed])))))

    (testing "Handle request where no device can be assigned"
      (let [ctx {:logger mock-logger :k8s-client (mock-k8s-client)}
            handle-mutation-fn (core/handle-mutation ctx)
            request {:kind "AdmissionReview"
                     :request {:uid "123abc"
                               :object {:metadata {:name "test-pod"
                                                   :namespace "default"
                                                   :annotations {"nonexistent-label" "true"}}
                                        :spec {:nodeName "node1"}}}}
            response (handle-mutation-fn request)]
        (is (= "AdmissionReview" (:kind response)))
        (is (= "123abc" (get-in response [:response :uid])))
        (is (= false (get-in response [:response :allowed])))))))
(let [mock-logger (reify log/Logger
                    (fatal [_ _])
                    (error [_ _])
                    (warn  [_ _])
                    (info  [_ _])
                    (debug [_ _]))
      mock-k8s-client (fn [& {:keys [gpu-label-map gpu-reservations]
                             :or   {gpu-label-map {:gpu1 #{"label1"}}
                                    gpu-reservations {:gpu1 {:pod "other-pod" :namespace "default"}}}}]
                        (k8s/->K8SClient
                         (reify k8s/IK8SBaseClient
                           (invoke [_ {:keys [kind action request]}]
                             (case [kind action]
                               [:Node :get] {:metadata {:annotations {:fudo.org/gpu.device.labels
                                                                      (core/base64-encode (core/try-json-generate gpu-label-map))
                                                                      :fudo.org/gpu.device.reservations
                                                                      (core/try-json-generate gpu-reservations)}}}
                               [:Pod :get] (if (= "other-pod" (:name request))
                                             {:name (:name request) :namespace (:namespace request)}
                                             (throw (ex-info "Not found" {:type :not-found})))
                               [:Node :patch/json] true)))))]
  (testing "Fail if no matching device is found"
    (let [ctx {:logger mock-logger :k8s-client (mock-k8s-client)}
          result (core/assign-device ctx {:node-name "node1" :pod "test-pod" :namespace "default" :requested-labels #{"nonexistent-label"}})]
      (is (nil? result))))

  (testing "Succeed if a matching device is found and available"
    (let [ctx {:logger mock-logger :k8s-client (mock-k8s-client :gpu-reservations {})}
          result (core/assign-device ctx {:node-name "node1" :pod "test-pod" :namespace "default" :requested-labels #{"label1"}})]
      (is (= "gpu1" result)))

    (let [ctx {:logger mock-logger :k8s-client (mock-k8s-client :gpu-reservations {:gpu1 nil})}
          result (core/assign-device ctx {:node-name "node1" :pod "test-pod" :namespace "default" :requested-labels #{"label1"}})]
      (is (= "gpu1" result))))

  (testing "Fail if a matching device is found but reserved by a still-existing pod"
    (let [ctx {:logger mock-logger :k8s-client (mock-k8s-client :gpu-reservations {:gpu1 {:pod "other-pod" :namespace "default"}})}
          result (core/assign-device ctx {:node-name "node1" :pod "test-pod" :namespace "default" :requested-labels #{"label1"}})]
      (is (nil? result))))

  (testing "Succeed if a matching device is found, and is reserved but by a pod that no longer exists"
    (let [ctx {:logger mock-logger
               :k8s-client (k8s/->K8SClient
                            (reify k8s/IK8SBaseClient
                              (invoke [_ {:keys [kind action]}]
                                (case [kind action]
                                  [:Node :get] {:metadata {:annotations {:fudo.org/gpu.device.labels
                                                                         (core/base64-encode (core/try-json-generate {"gpu1" #{"label1"}}))
                                                                         :fudo.org/gpu.device.reservations
                                                                         (core/try-json-generate {"gpu1" {:pod "nonexistent-pod" :namespace "default"}})}}}
                                  [:Pod :get] (throw (ex-info "Not found" {:type :not-found}))
                                  [:Node :patch/json] true))))}
          result (core/assign-device ctx {:node-name "node1" :pod "test-pod" :namespace "default" :requested-labels #{"label1"}})]
      (is (= "gpu1" result)))))

(run-tests 'gpu-device-assigner.core-test)
