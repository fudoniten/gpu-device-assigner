(ns gpu-device-assigner.util-test
  (:require [clojure.test :refer [deftest is testing]]
            [gpu-device-assigner.util :as util]))

(deftest base64-roundtrip
  (testing "Base64 encoding and decoding"
    (let [encoded (util/base64-encode "Hello world")]
      (is (= "SGVsbG8gd29ybGQ=" encoded))
      (is (= "Hello world"
             (String. ^bytes (util/base64-decode encoded) "UTF-8"))))))
