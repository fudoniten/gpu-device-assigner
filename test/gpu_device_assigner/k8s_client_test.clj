(ns gpu-device-assigner.k8s-client-test
  (:require [gpu-device-assigner.k8s-client :as k8s]
            [clojure.test :as t :refer [deftest is testing]]))

(deftest test-get-node
  (testing "get-node should retrieve node information"
    (let [mock-client (reify k8s/IK8SClient
                        (get-node [_ node-name] {:name node-name :status "Ready"}))
          client (k8s/->K8SClient mock-client)]
      (is (= {:name "test-node" :status "Ready"}
             (k8s/get-node client "test-node"))))))

(deftest test-patch-node
  (testing "patch-node should apply a patch to a node"
    (let [mock-client (reify k8s/IK8SClient
                        (patch-node [_ node-name patch] {:name node-name :patched true}))
          client (k8s/->K8SClient mock-client)]
      (is (= {:name "test-node" :patched true}
             (k8s/patch-node client "test-node" {:op "add" :path "/metadata/labels" :value "new-label"}))))))

(deftest test-pod-exists?
  (testing "pod-exists? should return true if pod exists"
    (let [mock-client (reify k8s/IK8SClient
                        (pod-exists? [_ pod-name namespace] true))
          client (k8s/->K8SClient mock-client)]
      (is (true? (k8s/pod-exists? client "test-pod" "default")))))

  (testing "pod-exists? should return false if pod does not exist"
    (let [mock-client (reify k8s/IK8SClient
                        (pod-exists? [_ pod-name namespace] false))
          client (k8s/->K8SClient mock-client)]
      (is (false? (k8s/pod-exists? client "nonexistent-pod" "default"))))))

(t/run-tests 'gpu-device-assigner.k8s-client-test)
