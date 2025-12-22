(ns gpu-device-assigner.logging-test
  (:require [clojure.test :refer [deftest is testing run-tests]]
            [taoensso.timbre :as timbre]
            [gpu-device-assigner.logging :as log]))

(defn- with-log-capture
  [level f]
  (let [captured (atom [])
        capture-appender {:enabled? true
                          :async? false
                          :min-level nil
                          :fn (fn [{:keys [level msg_]}]
                                (swap! captured conj {:level level :msg (force msg_)}))}]
    (timbre/with-merged-config
      {:appenders {:capture capture-appender
                   :println {:enabled? false}}
       :min-level :trace}
      (let [logger (log/print-logger level)]
        (f logger captured)))))

(defn- logged-messages
  [entries]
  (map :msg entries))

(deftest test-print-logger
  (testing "Logger at :fatal level"
    (with-log-capture :fatal
      (fn [logger captured]
        (log/fatal logger "Fatal message")
        (log/error logger "Error message")
        (log/warn logger "Warn message")
        (log/info logger "Info message")
        (log/debug logger "Debug message")
        (is (= ["Fatal message"] (logged-messages @captured))))))

  (testing "Logger at :error level"
    (with-log-capture :error
      (fn [logger captured]
        (log/fatal logger "Fatal message")
        (log/error logger "Error message")
        (log/warn logger "Warn message")
        (log/info logger "Info message")
        (log/debug logger "Debug message")
        (is (= ["Fatal message" "Error message"]
               (logged-messages @captured))))))

  (testing "Logger at :warn level"
    (with-log-capture :warn
      (fn [logger captured]
        (log/fatal logger "Fatal message")
        (log/error logger "Error message")
        (log/warn logger "Warn message")
        (log/info logger "Info message")
        (log/debug logger "Debug message")
        (is (= ["Fatal message" "Error message" "Warn message"]
               (logged-messages @captured))))))

  (testing "Logger at :info level"
    (with-log-capture :info
      (fn [logger captured]
        (log/fatal logger "Fatal message")
        (log/error logger "Error message")
        (log/warn logger "Warn message")
        (log/info logger "Info message")
        (log/debug logger "Debug message")
        (is (= ["Fatal message" "Error message" "Warn message" "Info message"]
               (logged-messages @captured))))))

  (testing "Logger at :debug level"
    (with-log-capture :debug
      (fn [logger captured]
        (log/fatal logger "Fatal message")
        (log/error logger "Error message")
        (log/warn logger "Warn message")
        (log/info logger "Info message")
        (log/debug logger "Debug message")
        (is (= ["Fatal message" "Error message" "Warn message" "Info message" "Debug message"]
               (logged-messages @captured)))))))

(run-tests)
