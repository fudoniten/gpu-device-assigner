(ns gpu-device-assigner.logging-test
  (:require [clojure.test :refer :all]
            [gpu-device-assigner.logging :as log]))

(deftest test-print-logger
  (testing "Logger at :fatal level"
    (let [logger (log/print-logger :fatal)]
      (is (= "Fatal message\n" (with-out-str (log/fatal logger "Fatal message"))))
      (is (= "" (with-out-str (log/error logger "Error message"))))
      (is (= "" (with-out-str (log/warn logger "Warn message"))))
      (is (= "" (with-out-str (log/info logger "Info message"))))
      (is (= "" (with-out-str (log/debug logger "Debug message"))))))

  (testing "Logger at :error level"
    (let [logger (log/print-logger :error)]
      (is (= "Fatal message\n" (with-out-str (log/fatal logger "Fatal message"))))
      (is (= "Error message\n" (with-out-str (log/error logger "Error message"))))
      (is (= "" (with-out-str (log/warn logger "Warn message"))))
      (is (= "" (with-out-str (log/info logger "Info message"))))
      (is (= "" (with-out-str (log/debug logger "Debug message"))))))

  (testing "Logger at :warn level"
    (let [logger (log/print-logger :warn)]
      (is (= "Fatal message\n" (with-out-str (log/fatal logger "Fatal message"))))
      (is (= "Error message\n" (with-out-str (log/error logger "Error message"))))
      (is (= "Warn message\n" (with-out-str (log/warn logger "Warn message"))))
      (is (= "" (with-out-str (log/info logger "Info message"))))
      (is (= "" (with-out-str (log/debug logger "Debug message"))))))

  (testing "Logger at :info level"
    (let [logger (log/print-logger :info)]
      (is (= "Fatal message\n" (with-out-str (log/fatal logger "Fatal message"))))
      (is (= "Error message\n" (with-out-str (log/error logger "Error message"))))
      (is (= "Warn message\n" (with-out-str (log/warn logger "Warn message"))))
      (is (= "Info message\n" (with-out-str (log/info logger "Info message"))))
      (is (= "" (with-out-str (log/debug logger "Debug message"))))))

  (testing "Logger at :debug level"
    (let [logger (log/print-logger :debug)]
      (is (= "Fatal message\n" (with-out-str (log/fatal logger "Fatal message"))))
      (is (= "Error message\n" (with-out-str (log/error logger "Error message"))))
      (is (= "Warn message\n" (with-out-str (log/warn logger "Warn message"))))
      (is (= "Info message\n" (with-out-str (log/info logger "Info message"))))
      (is (= "Debug message\n" (with-out-str (log/debug logger "Debug message")))))))
