(ns gpu-device-assigner.all-tests
  (:require [clojure.test :refer [run-tests]]
            [gpu-device-assigner.cli-test]
            [gpu-device-assigner.core-test]
            [gpu-device-assigner.http-test]
            [gpu-device-assigner.k8s-client-test]
            [gpu-device-assigner.lease-renewer-test]))

(defn -main
  "Run the full GPU device assigner test suite; exit non-zero on failure."
  [& _]
  (let [{:keys [fail error]} (run-tests 'gpu-device-assigner.cli-test
                                        'gpu-device-assigner.core-test
                                        'gpu-device-assigner.http-test
                                        'gpu-device-assigner.k8s-client-test
                                        'gpu-device-assigner.lease-renewer-test)]
    (shutdown-agents)
    (System/exit (if (zero? (+ fail error)) 0 1))))
