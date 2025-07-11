(ns gpu-device-assigner.core-test
  (:require [gpu-device-assigner.core :as core]
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

(run-tests 'gpu-device-assigner.core-test)
