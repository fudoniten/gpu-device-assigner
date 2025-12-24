(ns gpu-device-assigner.cli
  (:require [clojure.tools.cli :as cli]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.stacktrace :refer [print-stack-trace]]

            [gpu-device-assigner.k8s-client :as k8s]
            [gpu-device-assigner.context :as ctx]
            [gpu-device-assigner.http :as http]
            [gpu-device-assigner.lease-renewer :as renewer]
            [taoensso.telemere :as log :refer [log!]])
  (:import java.lang.Double)
  (:gen-class))

(def log-levels #{:trace :debug :info :warn :error :fatal :report})

(def cli-opts
  [["-l" "--log-level LOG-LEVEL" "Level at which to log output."
    :default  :warn
    :parse-fn keyword
    :validate [(set log-levels)
               (str "invalid log level, must be one of "
                    (str/join ", " (map name log-levels)))]]
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
   ["-p" "--port PORT" "Port on which to listen for incoming AdmissionReview requests."
    :default  443
    :parse-fn #(Integer/parseInt %)]
   ["-U" "--ui-port PORT" "Port for the human-friendly UI server."
    :default 8080
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
  (let [required-args #{:access-token :ca-certificate :kubernetes-url :port}
        {:keys [options _ errors summary]} (parse-opts args required-args cli-opts)]
    (when (:help options) (msg-quit 0 (usage summary)))
    (when (seq errors) (msg-quit 1 (usage summary errors)))
    (try
      (let [{:keys [access-token
                    ca-certificate
                    kubernetes-url
                    port
                    log-level
                    claims-namespace
                    ui-port
                    renew-interval
                    renew-jitter]} options
            client (k8s/create :url kubernetes-url
                               :timeout 120000 ; Set timeout to 120 seconds
                               :certificate-authority-data (k8s/load-certificate ca-certificate)
                               :token (k8s/load-access-token access-token))
            ctx (ctx/create ::k8s/client client
                            ::renewer/claims-namespace claims-namespace
                            ::renewer/renew-interval-ms renew-interval
                            ::renewer/jitter renew-jitter)
            shutdown-signal (promise)]
        (log! :info "starting gpu-device-assigner...")
        (log! :debug "creating shutdown hook...")
        (.addShutdownHook (Runtime/getRuntime)
                          (Thread. (fn []
                                     (log! :info "received shutdown request")
                                     (deliver shutdown-signal true))))
        (log! :info "Starting gpu-device-assigner web services...")
        (log! :debug (format "Configuration: access-token=%s, ca-certificate=%s, kubernetes-url=%s, port=%d, ui-port=%d, log-level=%s"
                           access-token ca-certificate kubernetes-url port ui-port log-level))
        (let [api-server (http/start-api-server ctx port)
              ui-server  (http/start-ui-server ctx ui-port)
              renewer-future (future (renewer/run-renewer! ctx))]
          @shutdown-signal
          (log! :info "Stopping gpu-device-assigner lease renewal service...")
          (future-cancel renewer-future)
          (log! :info "Stopping gpu-device-assigner UI service...")
          (.stop ui-server)
          (log! :info "Stopping gpu-device-assigner API service...")
          (.stop api-server)))
      (catch Exception e
        (log/error! (format "error in main: %s" (.getMessage e)))
        (log! :debug (print-stack-trace e))))
    (msg-quit 0 "stopping gpu-device-assigner...")))
