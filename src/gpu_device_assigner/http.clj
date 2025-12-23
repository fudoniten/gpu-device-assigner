(ns gpu-device-assigner.http
  (:require [clojure.string :as str]
            [clojure.stacktrace :refer [print-stack-trace]]
            [reitit.ring :as ring]
            [ring.adapter.jetty :as jetty]
            [ring.util.response :as response]

            [gpu-device-assigner.core :as core]
            [gpu-device-assigner.logging :as log]
            [gpu-device-assigner.util :as util]))

(defn json-middleware
  "Middleware to encode/decode the JSON body of requests/responses."
  [handler]
  (fn [req]
    (let [body (:body req)]
      (when-not body
        (throw (ex-info "missing request body!" {:req req})))
      (-> body
          (slurp)
          (util/try-json-parse)
          (handler)
          (util/try-json-generate)
          (response/response)
          (assoc-in [:headers "Content-Type"] "application/json")))))

(defn admission-review-response
  "Create a response for an AdmissionReview request."
  [& {:keys [uid allowed?
             status message
             patch]}]
  (assert (or status patch) "one of :status or :patch must be specified")
  {:apiVersion "admission.k8s.io/v1"
   :kind       "AdmissionReview"
   :response   (if status
                 {:uid       uid
                  :allowed   allowed?
                  :status    {:code    status
                              :message message}}
                 {:uid       uid
                  :allowed   allowed?
                  :patchType "JSONPatch"
                  :patch     patch})})

(defn open-fail-middleware
  "Middleware to ensure some response is passed to the caller."
  [{:keys [logger]}]
  (fn [handler]
    (fn [req]
      (try
        (handler req)
        (catch Exception e
          (log/error logger (format "error handling request: %s" (str e)))
          (log/debug logger (with-out-str (print-stack-trace e)))
          (let [uid (get-in req [:request :uid])]
            (admission-review-response :uid uid
                                       :allowed? false
                                       :status 500
                                       :message (.getMessage e))))))))

(defn device-assignment-patch
  "Generate JSONPatch that adds CDI assignment + node pin + breadcrumbs."
  [{:keys [logger]} device-id node]
  (let [patch
        [{:op "add" :path "/metadata/annotations" :value {}}
         ;; Breadcrumbs for ops / GC tools
         {:op "add" :path "/metadata/annotations/fudo.org~1gpu.uuid" :value (name device-id)}
         {:op "add" :path "/metadata/annotations/fudo.org~1gpu.node" :value (name node)}
         ;; Your CDI assignment (unchanged form)
         {:op "add" :path "/metadata/annotations/cdi.k8s.io~1gpu-assignment"
          :value (format "nvidia.com/gpu=UUID=%s" (name device-id))}
         ;; Hard bind to the node that actually has this UUID
         {:op "add" :path "/spec/nodeName" :value (name node)}]]
    (log/debug logger (str "\n##########\n#  PATCH\n##########\n\n" (util/pprint-string patch)))
    (-> patch
        (util/try-json-generate)
        (util/base64-encode))))

(defn log-requests-middleware
  [{:keys [logger]}]
  (fn [handler]
    (fn [req]
      (log/debug logger (str "\n\n##########\n# REQUEST\n##########\n\n" (util/pprint-string req)))
      (let [res (handler req)]
        (log/debug logger (str "\n\n##########\n# RESPONSE\n##########\n\n" (util/pprint-string res)))
        res))))

(defn handle-mutation
  "Handle an AdmissionReview request for mutating a pod's annotations."
  [{:keys [logger] :as ctx}]
  (fn [{:keys [kind] :as req}]
    (when-not (= kind "AdmissionReview")
      {:apiVersion "admission.k8s.io/v1"
       :kind       "AdmissionReview"
       :response   {:uid     (get-in req [:request :uid])
                    :allowed false
                    :status  {:code    400
                              :message (format "Unexpected request kind: %s" kind)}}})
    (log/debug logger (format "Received AdmissionReview request: %s" (util/pprint-string req)))
    (let [dry-run?         (true? (get-in req [:request :dryRun]))
          fudo-label?      (fn [[k _]] (= "fudo.org" (namespace k)))
          label-enabled?   (fn [[_ v]] v)
          gpu-label?       (fn [[k _]] (= "gpu" (first (str/split (name k) #"\."))))
          remove-assign    (fn [[k _]] (not= k :fudo.org/gpu.assign))
          uid              (get-in req [:request :uid])
          pod-uid          (core/get-claim-id req)
          pod              (or (get-in req [:request :object :metadata :name])
                               (get-in req [:request :object :metadata :generateName]))
          namespace        (get-in req [:request :object :metadata :namespace])
          all-labels       (get-in req [:request :object :metadata :labels])
          requested-labels (->> all-labels
                               (filter (every-pred fudo-label? label-enabled? gpu-label? remove-assign))
                               (keys)
                               (set))]
      (if dry-run?
        (do (log/info logger "dry-run AdmissionReview; skipping Lease allocation")
            (admission-review-response :uid uid :allowed? true
                                       :status 200
                                       :message "dry-run: no mutation"))

        (do (log/info logger (format "processing pod %s/%s, requesting labels [%s]"
                                     namespace pod (str/join "," (map name requested-labels))))
            (if-let [assigned-device (core/assign-device ctx {:pod              pod
                                                              :uid              pod-uid
                                                              :namespace        namespace
                                                              :requested-labels requested-labels})]
              (let [{:keys [device-id node]} assigned-device]
                (admission-review-response :uid uid :allowed? true
                                           :patch (device-assignment-patch ctx device-id node)))
              (admission-review-response :uid uid :status 429 :allowed? false
                                         :message (format "no GPUs with requested labels free for pod %s/%s"
                                                          namespace pod))))))))

(defn app [ctx]
  (ring/ring-handler
   (ring/router [["/mutate" {:post (handle-mutation ctx)}]]
                {:data {:middleware [(log-requests-middleware ctx)
                                     json-middleware
                                     (open-fail-middleware ctx)]}})
   (constantly {:status 404 :body "not found"})))

(defn start-server
  "Start the web server with the given context and port."
  [ctx port]
  (jetty/run-jetty (app ctx)
                   {:port port
                    :join? false
                    :ssl? false
                    :host "0.0.0.0"}))
