(ns gpu-device-assigner.all-tests
  (:require  [clojure.test :refer [run-tests]]

             [gpu-device-assigner.util-test :as util-test]
             [gpu-device-assigner.core-test :as core-test]
             [gpu-device-assigner.k8s-client-test :as k8s-client-test]))

(defn -main [& _]
  (run-tests 'gpu-device-assigner.util-test)
  (run-tests 'gpu-device-assigner.core-test)
  (run-tests 'gpu-device-assigner.k8s-client-test))
