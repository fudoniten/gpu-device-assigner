(ns gpu-device-assigner.http-test
  (:require [clojure.test :refer [deftest is testing]]
            [gpu-device-assigner.http :as http]
            [gpu-device-assigner.util :as util])
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

(deftest open-fail-middleware-handles-errors
  (testing "wraps exceptions in admission review response"
    (let [handler ((http/open-fail-middleware nil) (fn [_] (throw (ex-info "boom" {}))))
          result  (handler {:request {:uid "abc"}})]
      (is (= "AdmissionReview" (:kind result)))
      (is (= "abc" (get-in result [:response :uid])))
      (is (= 500 (get-in result [:response :status :code])))
      (is (= false (get-in result [:response :allowed]))))))
