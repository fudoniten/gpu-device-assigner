(ns gpu-device-assigner.k8s-client-test
  (:require [gpu-device-assigner.k8s-client :as k8s]
            [clojure.test :as t :refer [deftest is testing]]))

(deftest test-get-node
  (testing "get-node should retrieve node information"
    (let [mock-base-client (reify k8s/IK8SBaseClient
                             (invoke [_ {:keys [kind action request]}]
                               (case [kind action]
                                 [:Node :get] {:name (:name request) :status "Ready"})))
          client (k8s/->K8SClient mock-base-client nil)]
      (is (= {:name "test-node" :status "Ready"}
             (k8s/get-node client "test-node"))))))

(deftest test-patch-node
  (testing "patch-node should apply a patch to a node"
    (let [mock-base-client (reify k8s/IK8SBaseClient
                             (invoke [_ {:keys [kind action request]}]
                               (case [kind action]
                                 [:Node :patch/strategic] {:name (:name request) :patched true})))
          client (k8s/->K8SClient mock-base-client nil)]
      (is (= {:name "test-node" :patched true}
             (k8s/patch-node client "test-node" {:op "add" :path "/metadata/labels" :value "new-label"}))))))

(deftest test-pod-exists?
  (testing "pod-exists? should return true if pod exists"
    (let [mock-base-client (reify k8s/IK8SBaseClient
                             (invoke [_ {:keys [kind action request]}]
                               (case [kind action]
                                 [:Pod :get] {:name (:name request) :namespace (:namespace request)})))
          client (k8s/->K8SClient mock-base-client nil)]
      (is (true? (k8s/pod-exists? client "test-pod" "default")))))

  (testing "pod-exists? should return false if pod does not exist"
    (let [mock-base-client (reify k8s/IK8SBaseClient
                             (invoke [_ {:keys [kind action request]}]
                               (case [kind action]
                                 [:Pod :get] (throw (ex-info "Not found" {:type :not-found})))))
          client (k8s/->K8SClient mock-base-client nil)]
      (is (false? (k8s/pod-exists? client "nonexistent-pod" "default"))))))

(t/run-tests 'gpu-device-assigner.k8s-client-test)
