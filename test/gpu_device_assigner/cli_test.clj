(ns gpu-device-assigner.cli-test
  (:require [clojure.test :refer [deftest is testing]]
            [gpu-device-assigner.cli :as cli])
  (:import (java.io File)))

(defn- temp-file
  "Create a readable temp file and return its File instance."
  []
  (doto (File/createTempFile "gpu-device-assigner" "tmp")
    (.deleteOnExit)))

(deftest parse-opts-respects-fatal-log-level
  (testing "fatal is accepted as a log level"
    (let [token-file (temp-file)
          ca-file (temp-file)
          {:keys [options errors]} (cli/parse-opts ["--access-token" (.getPath token-file)
                                                    "--ca-certificate" (.getPath ca-file)
                                                    "--kubernetes-url" "http://example"
                                                    "--port" "8443"
                                                    "--log-level" "fatal"]
                                                   #{:access-token :ca-certificate :kubernetes-url :port}
                                                   cli/cli-opts)]
      (is (empty? errors))
      (is (= :fatal (:log-level options))))))

(deftest parse-opts-rejects-invalid-log-level
  (testing "invalid log level triggers validation error"
    (let [token-file (temp-file)
          ca-file (temp-file)
          {:keys [errors]} (cli/parse-opts ["--access-token" (.getPath token-file)
                                            "--ca-certificate" (.getPath ca-file)
                                            "--kubernetes-url" "http://example"
                                            "--port" "8443"
                                            "--log-level" "invalid"]
                                           #{:access-token :ca-certificate :kubernetes-url :port}
                                           cli/cli-opts)]
      (is (seq errors)))))
