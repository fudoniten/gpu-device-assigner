(ns gpu-device-assigner.core
  (:require [clojure.string :as str]
            [clojure.stacktrace :refer [print-stack-trace]]
            [clojure.set :refer [subset?]]

            [gpu-device-assigner.logging :as log]
            [gpu-device-assigner.k8s-client :as k8s]

            [reitit.ring :as ring]
            [cheshire.core :as json]
            [ring.util.response :as response]
            [ring.adapter.jetty :as jetty])
  (:import java.util.Base64
           java.time.Instant))

(defn get-node-annotations
  "Retrieve annotations from a Kubernetes node."
  [{:keys [k8s-client]} node-name]
  (-> (k8s/get-node k8s-client node-name)
      :metadata
      :annotations))

(defn base64-encode
  "Encode a string to Base64."
  [str]
  (let [encoder (Base64/getEncoder)]
    (.encodeToString encoder (.getBytes str "UTF-8"))))

(defn base64-decode
  "Decode a Base64 encoded string."
  [b64-str]
  (let [decoder (Base64/getDecoder)]
    (.decode decoder b64-str)))

(defn map-vals
  "Apply a function to all values in a map."
  [f m]
  (into {} (map (fn [[k v]] [k (f v)])) m))

(defn parse-json
  "Parse a JSON string into a Clojure data structure."
  [str]
  (json/parse-string str true))

(defn get-device-labels
  "Get GPU device labels from node annotations."
  [ctx node-name]
  (some->> (get-node-annotations ctx node-name)
           :fudo.org/gpu.device.labels
           (base64-decode)
           (String.)
           (parse-json)
           (map-vals set)))

(defn get-device-reservations
  "Retrieve current device reservations from node annotations."
  [ctx node-name]
  (let [annotations (get-node-annotations ctx node-name)]
    (some->> annotations
             :fudo.org/gpu.device.reservations
             (parse-json)
             (assoc (select-keys annotations [:resourceVersion]) :reservations))))

(defn node-patch
  "Apply a patch to a Kubernetes node."
  [{:keys [k8s-client]} node-name patch]
  (k8s/patch-node k8s-client node-name patch))

(defn reserve-device
  "Reserve a GPU device for a pod if available."
  [{:keys [logger] :as ctx} {:keys [node-name namespace pod device-id]}]
  (let [{version :resourceVersion reservations :reservations} (get-device-reservations ctx node-name)
        updated-reservations (assoc reservations (name device-id) {:pod pod :namespace namespace :timestamp (.toString (Instant/now))})
        reservation-patch    {:metadata {:annotations
                                         {:fudo.org/gpu.device.reservations
                                          (json/generate-string updated-reservations)}}
                              :resourceVersion version}]
    (try (node-patch ctx node-name reservation-patch)
         (name device-id)
         (catch Exception e
           (println e)
           (let [status (ex-data e)]
             (if (= 409 (:status status))
               (log/error logger (format "conflict: reservation was modified before patch was applied. unable to reserve device %s for pod %s."
                                         device-id pod))
               (log/error logger (format "error %s: unable to reserve device %s for pod %s: %s"
                                         (:status status) device-id pod (:message e))))
             nil)))))

(defn pthru [val o]
  (println (str val ": " o))
  o)

(defn device-reserved?
  "Check if a device is reserved by a different pod and if that pod still exists."
  [{:keys [k8s-client]} reservations dev-id this-pod]
  (let [reservation (get-in reservations [:reservations dev-id])
        reserved-pod (:pod reservation)]
    (and reserved-pod
         (not= reserved-pod this-pod)
         (k8s/pod-exists? k8s-client reserved-pod (:namespace reservation)))))

(defn assign-device
  "Assign a GPU device to a pod based on requested labels."
  [{:keys [logger] :as ctx} {:keys [node-name pod namespace requested-labels]}]
  (let [node-dev-labels (get-device-labels ctx node-name)
        candidates (filter (fn [[_ dev-labels]] (subset? requested-labels dev-labels))
                           node-dev-labels)]
    (when (empty? candidates)
      (log/error logger
                 (format "error: no candidate devices found! pod %s was misassigned. requested labels: [%s]"
                         pod (str/join ", " requested-labels))))
    (let [reservations (get-device-reservations ctx node-name)
          available (map name (filter (fn [dev-id] (not (device-reserved? ctx reservations dev-id pod)))
                                      (keys candidates)))]
      (if (empty? available)
        (do (log/error logger (format "error: no available devices found for pod %s on node %s." pod node-name))
            nil)
        (let [selected-id (rand-nth available)]
          (reserve-device ctx
                          {:node-name node-name
                           :namespace namespace
                           :pod       pod
                           :device-id (str selected-id)}))))))

(defn try-json-parse [str]
  (try
    (json/parse-string str true)
    (catch Exception e
      (throw (ex-info "exception encountered when parsing json string"
                      {:body str :exception e})))))

(defn try-json-generate [json]
  (try
    (json/generate-string json true)
    (catch Exception e
      (throw (ex-info "exception encountered when generating json string"
                      {:body str :exception e})))))

(defn json-middleware
  "Middleware to encode/decode the JSON body of requests/responses."
  [handler]
  (fn [req]
    (if-let [body {:body req}]
      (-> body
          (slurp)
          (try-json-parse)
          (handler)
          (try-json-generate)
          (response/response)
          (assoc-in [:headers "Content-Type"] "application/json"))
      (throw (ex-info "missing request body!" {:req req})))))

(defn open-fail-middleware
  "Middleware to ensure some response is passed to the caller."
  [{:keys [logger]}]
  (fn [handler]
    (fn [{:keys [uid] :as req}]
      (try
        (handler req)
        (catch Exception e
          (log/error logger (format "error handling request: %s" (str e)))
          (log/debug logger (print-stack-trace
                             (-> {:apiVersion "admission.k8s.io/v1"
                                  :kind "AdmissionReview"
                                  :response {:uid     uid
                                             :allowed true}}
                                 (try-json-generate)))))))))

(defn admission-review-response
  "Create a response for an AdmissionReview request."
  [& {:keys [uid allowed?
             status message
             patch]}]
  (assert (or status patch) "one of :status or :patch must be specified")
  {:apiVersion "admission.k8s.io/v1"
   :kind       "AdmissionReview"
   :response   (if status
                 {:uid     uid
                  :allowed allowed?
                  :status  {:code    status
                            :message message}}
                 {:uid       uid
                  :allowed   allowed?
                  :patchType "JSONPatch"
                  :patch     patch})})

(defn device-assignment-patch
  "Generate a JSON patch for assigning a device to a pod."
  [device-id]
  (-> {:op    "add"
       :path  "/metadata/annotations/cdi.k8s.io~1nvidia.com~1gpu"
       :value device-id}
      (json/generate-string)
      (base64-encode)))

(defn handle-mutation
  "Handle an AdmissionReview request for mutating a pod's annotations."
  [ctx]
  (fn [{:keys [kind] :as req}]
    (when-not (= kind "AdmissionReview")
      {:apiVersion "admission.k8s.io/v1"
       :kind       "AdmissionReview"
       :response   {:uid     (get-in req [:request :uid])
                    :allowed false
                    :status {:code    400
                             :message (format "Unexpected request kind: %s" kind)}}})
    (log/debug (:logger ctx) (format "Received AdmissionReview request: %s" (pr-str req)))
    (let [uid (get-in req [:request :uid])
          pod (get-in req [:request :object :metadata :name])
          ns  (get-in req [:request :object :metadata :namespace])
          annotations (get-in req [:request :object :metadata :annotations])
          labels (keys (filter (fn [[_ v]] (= v "true")) annotations))
          node-name (get-in req [:request :object :spec :nodeName])]
      (log/debug (:logger ctx) (format "Processing pod: %s in namespace: %s on node: %s with requested labels: %s"
                                       pod ns node-name (str/join ", " labels)))
      (if-let [assigned-device (assign-device ctx {:node-name node-name
                                                   :pod       pod
                                                   :namespace ns
                                                   :requested-labels labels})]
        (do
          (log/info (:logger ctx) (format "Assigned device %s to pod %s on node %s" assigned-device pod node-name))
          (admission-review-response :uid uid :allowed? true
                                     :patch (device-assignment-patch assigned-device)))
        (do
          (log/error (:logger ctx) (format "Failed to find unreserved device for pod %s on node %s" pod node-name))
          (admission-review-response :uid uid :status 500 :allowed? false
                                     :message (format "failed to find unreserved device for pod %s on node %s."
                                                      pod node-name)))))))

(defn app [ctx]
  (ring/ring-handler
   (ring/router [["/mutate" {:post (handle-mutation ctx)}]]
                {:data {:middleware [json-middleware
                                     (open-fail-middleware ctx)]}})))

(defn start-server
  "Start the web server with the given context and port."
  [ctx port keystore keystore-password]
  (let [configurator (fn [server]
                       (.setDumpAfterStart server true)
                       (.setDumpBeforestop server true)
                       (.setStopAtShutdown server  true))]
    (jetty/run-jetty (app ctx)
                     {:port port
                      :join? false
                      :ssl? true
                      :keystore keystore
                      :keystore-type "PKCS12"
                      :key-password keystore-password
                      :configurator configurator})))


;; {
;;   "apiVersion": "admission.k8s.io/v1",
;;   "kind": "AdmissionReview",
;;   "request": {
;;     "uid": "123abc...",
;;     "object": {
;;       "metadata": {
;;         "name": "mypod",
;;         "namespace": "default",
;;         "annotations": {
;;           "gpu.openai.com/needs": "himem"
;;         }
;;       },
;;       "spec": {
;;         "nodeName": "gpu-node-1"
;;       }
;;     }
;;   }
;; }

;; {
;;   "apiVersion": "admission.k8s.io/v1",
;;   "kind": "AdmissionReview",
;;   "response": {
;;     "uid": "123abc...",
;;     "allowed": true,
;;     "patchType": "JSONPatch",
;;     "patch": "<base64-encoded JSON patch>"
;;   }
;; }
