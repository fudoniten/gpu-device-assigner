(ns gpu-device-assigner.all-tests
  (:require  [clojure.test :refer [run-tests]]

             [gpu-device-assigner.core-test :as core-test]
             [gpu-device-assigner.k8s-client-test :as k8s-client-test]))

(defn -main
  "Run the full GPU device assigner test suite."
  [& _]
  (run-tests 'gpu-device-assigner.core-test)
  (run-tests 'gpu-device-assigner.k8s-client-test))
