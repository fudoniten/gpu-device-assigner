(ns gpu-device-assigner.cli
  (:require [clojure.core.async :refer [>!! <!! chan go-loop alt!]]
            [clojure.tools.cli :as cli]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.stacktrace :refer [print-stack-trace]]

            [gpu-device-assigner.k8s-client :as k8s]
            [gpu-device-assigner.context :as ctx]
            [gpu-device-assigner.logging :as log]
            [gpu-device-assigner.core :as core])
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
    :default  80
    :parse-fn #(Integer/parseInt %)]])

(defn msg-quit
  [status msg]
  (println msg)
  (System/exit status))

(defn usage
  ([summary] (usage summary []))
  ([summary errs] (str/join \newline
                            (concat errs
                                    ["usage: gpu-device-assigner [opts]"
                                     ""
                                     "options:"
                                     summary]))))

(defn parse-opts
  [args reqs cli-opts]
  (let [{:keys [options] :as result} (cli/parse-opts args cli-opts)
        missing (set/difference reqs (-> options (keys) (set)))
        missing-errs (map #(format "missing required parameter: %s" (name %))
                          missing)]
    (update result :errors concat missing-errs)))

(defn -main
  [& args]
  (let [default-logger (log/print-logger :info)
        required-args #{:access-token :ca-certificate :kubernetes-url :port}
        {:keys [options _ errors summary]} (parse-opts args required-args cli-opts)]
    (when (:help options) (msg-quit 0 (usage summary)))
    (when (seq errors) (msg-quit 1 (usage summary errors)))
    (try
      (let [{:keys [access-token ca-certificate kubernetes-url port log-level]} options
            logger (log/print-logger log-level)
            client (k8s/create :url kubernetes-url
                               :certificate-authority-data (k8s/load-certificate ca-certificate)
                               :token (k8s/load-access-token access-token))
            ctx (ctx/create ::log/logger logger ::k8s/client client)
            shutdown-chan (chan)]
        (.addShutdownHook (Runtime/getRuntime)
                          (Thread. (fn [] (>!! shutdown-chan true))))
        (log/info logger "starting gpu-device-assigner web service...")
        (let [server (core/start-server ctx port)]
          (<!! shutdown-chan)
          (log/warn logger "stopping gpu-device-assigner web service...")
          (.stop server)))
      (catch Exception e
        (log/error default-logger
                   (format "error in main: %s" (.getMessage e)))
        (log/debug default-logger
                   (print-stack-trace e))))
    (msg-quit 0 "stopping gpu-device-assigner...")))
