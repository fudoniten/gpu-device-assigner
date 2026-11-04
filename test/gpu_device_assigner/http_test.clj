(ns gpu-device-assigner.http-test
  (:require [clojure.test :refer [deftest is testing]]
            [gpu-device-assigner.http :as http]
            [gpu-device-assigner.util :as util]
            [gpu-device-assigner.core :as core])
  (:import java.io.ByteArrayInputStream))

(defn- json-body [m]
  (-> m util/try-json-generate (.getBytes "UTF-8") (ByteArrayInputStream.)))

(deftest json-middleware-round-trip
  (testing "encodes map responses and decodes request bodies"
    (let [handler (http/json-middleware (fn [req]
                                         (is (= {:hello "world"}
                                                (:body req)))
                                         {:status 200
                                          :body   {:ok true}}))
          {:keys [status body headers]} (handler {:body (json-body {:hello "world"})})]
      (is (= 200 status))
      (is (= "application/json" (get headers "Content-Type")))
      (is (= {:ok true} (util/parse-json body))))))

(deftest json-middleware-turns-handler-errors-into-denials
  (testing "handler exceptions become a well-formed AdmissionReview denial with the request uid"
    (let [handler (http/json-middleware (fn [_] (throw (ex-info "boom" {}))))
          {:keys [body]} (handler {:body (json-body {:kind "AdmissionReview" :request {:uid "abc"}})})
          result (util/parse-json body)]
      (is (= "AdmissionReview" (:kind result)))
      (is (= "abc" (get-in result [:response :uid])))
      (is (= 500 (get-in result [:response :status :code])))
      (is (= false (get-in result [:response :allowed]))))))

(deftest error-middleware-returns-http-500
  (let [handler ((http/error-middleware nil) (fn [_] (throw (ex-info "boom" {}))))
        result  (handler {})]
    (is (= 500 (:status result)))))

(deftest mutate-route-end-to-end
  (testing "the /mutate route decodes the review and returns JSON"
    (let [app (http/api-app {})
          {:keys [status body]} (app {:request-method :post
                                      :uri            "/mutate"
                                      :body           (json-body {:kind "Nope" :request {:uid "u1"}})})
          result (util/parse-json body)]
      (is (= 200 status))
      (is (= "u1" (get-in result [:response :uid])))
      (is (= false (get-in result [:response :allowed]))))))

(deftest healthz
  (is (= 200 (:status ((http/api-app {}) {:request-method :get :uri "/healthz"})))))

(deftest nvidia-gpu-tag-validation
  (testing "rejects pod requesting nvidia.com/gpu with no fudo.org labels whatsoever"
    (let [handle-mutation-fn (http/handle-mutation {})
          request {:kind    "AdmissionReview"
                   :request {:uid    "abc123"
                             :object {:metadata {:name      "bad-pod"
                                                 :namespace "default"
                                                 :labels    {}}
                                      :spec     {:containers [{:name      "app"
                                                               :resources {:requests {"nvidia.com/gpu" "1"}}}]}}}}
          response (handle-mutation-fn request)]
      (is (= "AdmissionReview" (:kind response)))
      (is (= "abc123" (get-in response [:response :uid])))
      (is (= false (get-in response [:response :allowed])))
      (is (= 403 (get-in response [:response :status :code])))))

  (testing "rejects pod with nvidia.com/gpu in init container and no fudo.org labels"
    (let [handle-mutation-fn (http/handle-mutation {})
          request {:kind    "AdmissionReview"
                   :request {:uid    "abc456"
                             :object {:metadata {:name      "bad-init-pod"
                                                 :namespace "default"
                                                 :labels    {}}
                                      :spec     {:containers     [{:name "app"}]
                                                 :initContainers [{:name      "init"
                                                                   :resources {:requests {"nvidia.com/gpu" "1"}}}]}}}}
          response (handle-mutation-fn request)]
      (is (= false (get-in response [:response :allowed])))
      (is (= 403 (get-in response [:response :status :code])))))

  (testing "allows pod with nvidia.com/gpu AND fudo.org/gpu.assign (lease-system opt-in)"
    ;; Pods with only fudo.org/gpu.assign were previously allowed through and
    ;; assigned a random GPU.  They must still be allowed through so that the
    ;; webhook service itself (and other pods that opt in via the assign label
    ;; without a specific type label) are not blocked.
    (with-redefs [core/assign-device (fn [_ _] {:devices [{:device-id :gpu1 :node "node1"}]
                                                :node "node1"
                                                :reservation-id "res-1"})]
      (let [handle-mutation-fn (http/handle-mutation {})
            request {:kind    "AdmissionReview"
                     :request {:uid    "abc-assign"
                               :object {:metadata {:name      "assign-only-pod"
                                                   :namespace "default"
                                                   :labels    {:fudo.org/gpu.assign true}}
                                        :spec     {:containers [{:name      "app"
                                                                 :resources {:requests {"nvidia.com/gpu" "1"}}}]}}}}
            response (handle-mutation-fn request)]
        (is (= true (get-in response [:response :allowed]))))))

  (testing "allows pod with nvidia.com/gpu request AND fudo.org/gpu.* type labels"
    (with-redefs [core/assign-device (fn [_ _] {:devices [{:device-id :gpu1 :node "node1"}]
                                                :node "node1"
                                                :reservation-id "res-1"})]
      (let [handle-mutation-fn (http/handle-mutation {})
            request {:kind    "AdmissionReview"
                     :request {:uid    "abc789"
                               :object {:metadata {:name      "good-pod"
                                                   :namespace "default"
                                                   :labels    {:fudo.org/gpu.compute true}}
                                        :spec     {:containers [{:name      "app"
                                                                 :resources {:requests {"nvidia.com/gpu" "1"}}}]}}}}
            response (handle-mutation-fn request)]
        (is (= true (get-in response [:response :allowed]))))))

  (testing "allows pod with no nvidia.com/gpu request and no fudo.org labels"
    (with-redefs [core/assign-device (fn [_ _] (throw (ex-info "should not be called" {})))]
      (let [handle-mutation-fn (http/handle-mutation {})
            request {:kind    "AdmissionReview"
                     :request {:uid    "abc000"
                               :object {:metadata {:name      "cpu-pod"
                                                   :namespace "default"
                                                   :labels    {}}
                                        :spec     {:containers [{:name "app"}]}}}}
            response (handle-mutation-fn request)]
        (is (true? (get-in response [:response :allowed])))
        (is (nil? (get-in response [:response :patch]))))))

  (testing "returns 400 for non-AdmissionReview kind"
    (let [handle-mutation-fn (http/handle-mutation {})
          request {:kind    "SomethingElse"
                   :request {:uid "xyz"}}
          response (handle-mutation-fn request)]
      (is (= "AdmissionReview" (:kind response)))
      (is (= false (get-in response [:response :allowed])))
      (is (= 400 (get-in response [:response :status :code]))))))
