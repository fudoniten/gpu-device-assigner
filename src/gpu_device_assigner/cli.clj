(ns gpu-device-assigner.cli
  (:require [clojure.tools.cli :as cli]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.stacktrace :refer [print-stack-trace]]

            [gpu-device-assigner.k8s-client :as k8s]
            [gpu-device-assigner.context :as ctx]
            [gpu-device-assigner.logging :as log]
            [gpu-device-assigner.http :as http]
            [gpu-device-assigner.lease-renewer :as renewer])
  (:import java.lang.Double)
  (:gen-class))

(def cli-opts
  [["-l" "--log-level LOG-LEVEL" "Level at which to log output."
    :default  :warn
    :parse-fn keyword
    :validate [(set log/LOG-LEVELS)
               (str "invalid log level, must be one of "
                    (str/join ", " (map name log/LOG-LEVELS)))]]
   ["-h" "--help" "Print this message."]

   ["-t" "--access-token ACCESS-TOKEN" "Path to token file for Kubernetes access."
    :validate [#(.exists (io/as-file %)) "access-token file does not exist"
               #(.canRead (io/as-file %)) "access-token file is not readable"
               #(not (.isDirectory (io/as-file %))) "access-token file not a regular file"]]
   ["-c" "--ca-certificate CA-CERTIFICATE" "Path to base64-encoded CA certificate for Kubernetes"
    :validate [#(.exists (io/as-file %)) "ca-certificate file does not exist"
               #(.canRead (io/as-file %)) "ca-certificate file is not readable"
               #(not (.isDirectory (io/as-file %))) "ca-certificate file not a regular file"]]
   ["-k" "--kubernetes-url URL" "URL to Kubernetes master."]
   ["-p" "--port PORT" "Port on which to listen for incoming requests."
    :default  443
    :parse-fn #(Integer/parseInt %)]
   ["-C" "--claims-namespace NAMESPACE" "Namespace in which to store GPU leases."
    :default "gpu-claims"]
   ["-i" "--renew-interval INTERVAL" "Interval at which to renew leases if pod still exists."
    :default 60000
    :parse-fn #(Integer/parseInt %)]
   ["-j" "--renew-jitter JITTER" "Jitter on renew timing."
    :default 0.2
    :parse-fn #(Double/parseDouble %)]])

(defn msg-quit
  "Print a message and exit the process with the given status."
  [status msg]
  (println msg)
  (System/exit status))

(defn usage
  "Render a usage string with optional error messages."
  ([summary] (usage summary []))
  ([summary errs] (str/join \newline
                            (concat errs
                                    ["usage: gpu-device-assigner [opts]"
                                     ""
                                     "options:"
                                     summary]))))

(defn parse-opts
  "Parse CLI arguments and report missing required options."
  [args reqs cli-opts]
  (let [{:keys [options] :as result} (cli/parse-opts args cli-opts)
        missing (set/difference reqs (-> options (keys) (set)))
        missing-errs (map #(format "missing required parameter: %s" (name %))
                          missing)]
    (update result :errors concat missing-errs)))

(defn -main
  "Entry point for the gpu-device-assigner CLI."
  [& args]
  (let [default-logger (log/print-logger :info)
        required-args #{:access-token :ca-certificate :kubernetes-url :port}
        {:keys [options _ errors summary]} (parse-opts args required-args cli-opts)]
    (when (:help options) (msg-quit 0 (usage summary)))
    (when (seq errors) (msg-quit 1 (usage summary errors)))
    (log/info default-logger "starting gpu-device-assigner...")
    (try
      (let [{:keys [access-token
                    ca-certificate
                    kubernetes-url
                    port
                    log-level
                    claims-namespace
                    renew-interval
                    renew-jitter]} options
            logger (log/print-logger log-level)
            client (k8s/create :url kubernetes-url
                               :timeout 120000 ; Set timeout to 120 seconds
                               :certificate-authority-data (k8s/load-certificate ca-certificate)
                               :token (k8s/load-access-token access-token)
                               :logger logger)
            ctx (ctx/create ::log/logger logger
                            ::k8s/client client
                            ::renewer/claims-namespace claims-namespace
                            ::renewer/renew-interval-ms renew-interval
                            ::renewer/jitter renew-jitter)
            shutdown-signal (promise)]
        (log/debug logger "creating shutdown hook...")
        (.addShutdownHook (Runtime/getRuntime)
                          (Thread. (fn []
                                     (log/info logger "received shutdown request")
                                     (deliver shutdown-signal true))))
        (log/info logger "Starting gpu-device-assigner web service...")
        (log/debug logger (format "Configuration: access-token=%s, ca-certificate=%s, kubernetes-url=%s, port=%d, log-level=%s"
                                  access-token ca-certificate kubernetes-url port log-level))
        (let [server (http/start-server ctx port)
              renewer-future (future (renewer/run-renewer! ctx))]
          @shutdown-signal
          (log/warn logger "Stopping gpu-device-assigner lease renewal service...")
          (future-cancel renewer-future)
          (log/warn logger "Stopping gpu-device-assigner web service...")
          (.stop server)))
      (catch Exception e
        (log/error default-logger
                   (format "error in main: %s" (.getMessage e)))
        (log/debug default-logger
                   (print-stack-trace e))))
    (msg-quit 0 "stopping gpu-device-assigner...")))
