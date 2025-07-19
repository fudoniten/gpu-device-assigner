(ns gpu-device-assigner.core
  (:require [clojure.string :as str]
            [clojure.stacktrace :refer [print-stack-trace]]
            [clojure.set :refer [subset?]]
            [clojure.pprint :refer [pprint]]
            [clojure.spec.alpha :as s]

            [gpu-device-assigner.context :as context]
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

(defn get-all-node-annotations
  [{:keys [k8s-client]}]
  (into {}
        (map (fn [node]
               [(-> node :metadata :name)
                (-> node :metadata :annotations)]))
        (k8s/get-nodes k8s-client)))

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
  (try
    (let [annotations (get-node-annotations ctx node-name)]
      (some->> annotations
               :fudo.org/gpu.device.reservations
               (parse-json)
               (assoc (select-keys annotations [:resourceVersion]) :reservations)))
    (catch Exception e
      (throw (ex-info "Failed to retrieve device reservations"
                      {:node-name node-name
                       :exception e})))))

(s/def ::device-labels
  (s/map-of ::device-id
            (s/keys :req-un [::node-name ::device-labels])))

(s/fdef -unpack-device-labels
  :args (s/cat :annotations (s/map-of symbol? any?))
  :ret  ::device-labels)
(defn -unpack-device-labels
  [annotations]
  (some-> annotations
          :fudo.org/gpu.device.labels
          (base64-decode)
          (String.)
          (parse-json)))

(s/def ::pod string?)
(s/def ::namespace string?)
(s/def ::timestamp string?)

(defn pthru [o] (clojure.pprint/pprint o) o)

(s/def ::device-reservations
  (s/map-of ::device-id (s/keys :req-un [::pod ::namespace ::timestamp])))

(s/fdef -unpack-device-reservations
  :args (s/cat :annotations (s/map-of symbol? any?))
  :ret  ::device-reservations)
(defn -unpack-device-reservations
  [annotations]
  (some-> annotations
          :fudo.org/gpu.device.reservations
          (base64-decode)
          (String.)
          (parse-json)))

(s/fdef get-all-device-labels
  :args ::context/context
  :ret  ::device-labels)
(defn get-all-device-labels
  [ctx]
  (apply merge
         (map (fn [[node annos]]
                (into {}
                      (map (fn [[device labels]]
                             [device {:node-name node
                                      :labels (set (map keyword labels))}]))
                      (-unpack-device-labels annos)))
              (get-all-node-annotations ctx))))

(s/fdef find-available-devices
  :args (s/cat :devices          ::device-labels
               :reserved-devices (s/coll-of ::device-id))
  :ret  ::device-labels)
(defn find-available-devices
  [devices reserved-devices]
  (into {}
        (filter
         (fn [[device _]]
           (some #(= device %) reserved-devices)))
        devices))

(s/fdef find-matching-devices
  :args (s/cat :device-labels ::device-labels
               :req-labels (s/and set? (s/coll-of ::device-label)))
  :ret  ::device-labels)
(defn find-matching-devices
  [device-labels req-labels]
  (into {}
        (filter
         (fn [[_ {labels ::device-labels}]]
           (subset? req-labels labels)))
        device-labels))

(s/fdef get-all-device-reservations
  :args (s/cat :ctx ::context/context)
  :ret  ::device-reservations)
(defn get-all-device-reservations
  [{:keys [k8s-client] :as ctx}]
  (into {}
        (for [[_ node-annos] (get-all-node-annotations ctx)
              [device-id {:keys [pod namespace] :as reservation}] (-unpack-device-reservations node-annos)
              :when (k8s/pod-exists? k8s-client pod namespace)]
          [device-id reservation])))

(defn device-reserved?
  [ctx device-id]
  (let [reservations (get-all-device-reservations ctx)]
    (get reservations device-id)))

(defn pick-device [ctx labels]
  (try
    (let [device-labels (get-all-device-labels ctx)
          matching      (find-matching-devices device-labels labels)
          reservations  (-> (get-all-device-reservations ctx) (keys) (set))
          available     (filter (fn [[dev-id _]] (some reservations dev-id)) matching)]
      (if-let [selected-id (rand-nth (keys available))]
        {:device-id selected-id :node (-> available selected-id :node-name)}
        nil))
    (catch Exception e
      (throw (ex-info "Failed to pick device"
                      {:labels labels
                       :exception e})))))

(defn node-patch
  "Apply a patch to a Kubernetes node."
  [{:keys [k8s-client]} node-name patch]
  (try
    (k8s/patch-node k8s-client node-name patch)
    (catch Exception e
      (throw (ex-info "Failed to patch node"
                      {:node-name node-name
                       :patch patch
                       :exception e})))))

(defn reserve-device
  [{:keys [logger] :as ctx} node device-id pod namespace]
  (let [{version :resourceVersion reservations :reservations} (get-device-reservations ctx node)
        updated-reservations (assoc reservations (name device-id) {:pod pod :namespace namespace :timestamp (.toString (Instant/now))})
        reservation-patch {:metadata {:annotations
                                      {:fudo.org/gpu.device.reservations
                                       (json/generate-string updated-reservations)}}
                           :resourceVersion version}]
    (try (node-patch ctx node reservation-patch)
         {:device-id device-id :pod pod :namespace namespace :node node}
         (catch Exception e
           (let [status (ex-data e)]
             (if (= 409 (:status status))
               (log/error logger (format "conflict: reservation was modified before patch was applied. unable to reserve device %s for pod %s."
                                         device-id pod))
               (log/error logger (format "error %s: unable to reserve device %s for pod %s: %s"
                                         (:status status) device-id pod (:message e))))
             nil)))))

(defn assign-device
  "Assign a GPU device to a pod based on requested labels."
  [{:keys [logger] :as ctx} {:keys [pod namespace requested-labels]}]
  (if-let [{:keys [device-id node]} (pick-device ctx requested-labels)]
    (do (log/info logger
                  (format "attempting to reserve device %s on node %s to pod %s/%s"
                          device-id node namespace pod))
        (reserve-device ctx node device-id pod namespace))
    (do (log/error logger
                   (format "unable to find device to reserve for pod %s/%s, labels [%s]"
                           namespace pod (str/join "," (map name requested-labels))))
        (ex-info (format "unable to find device to reserve for pod %s/%s" namespace pod)
                 {:pod       pod
                  :namespace namespace
                  :labels    requested-labels}))))

(defn try-json-parse [str]
  (try
    (json/parse-string str true)
    (catch Exception e
      (throw (ex-info "exception encountered when parsing json string"
                      {:body str :exception e})))))

(defn try-json-generate [json]
  (try
    (json/generate-string json)
    (catch Exception e
      (throw (ex-info "exception encountered when generating json string"
                      {:body str :exception e})))))

(defn json-middleware
  "Middleware to encode/decode the JSON body of requests/responses."
  [handler]
  (fn [req]
    (if-let [body {:body req}]
      (do (println (str "GOT BODY: " body))
          (-> body
              :body
              :body ;; Who knows wtf??
              (slurp)
              (try-json-parse)
              (handler)
              (try-json-generate)
              (response/response)
              (assoc-in [:headers "Content-Type"] "application/json")))
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
                                 (try-json-generate))))
          {:status 500
           :headers {:Content-Type "application/json"}
           :body    (json/generate-string {:error (.getMessage e)})})))))

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

(defn pprint-string [o]
  (with-out-str (pprint o)))

(defn log-requests-middleware
  [{:keys [logger]}]
  (fn [handler]
    (fn [req]
      (log/debug logger (str "\n\n##########\n# REQUEST\n##########\n\n" (pprint-string req)))
      (let [res (handler req)]
        (log/debug logger (str "\n\n##########\n# RESPONSE\n##########\n\n" (pprint-string res)))
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
                    :status {:code    400
                             :message (format "Unexpected request kind: %s" kind)}}})
    (log/debug (:logger ctx) (format "Received AdmissionReview request: %s" (pr-str req)))
    (let [uid (get-in req [:request :uid])
          pod (get-in req [:request :object :metadata :name])
          ns  (get-in req [:request :object :metadata :namespace])
          annotations (get-in req [:request :object :metadata :annotations])
          requested-labels (keys (filter (fn [[_ v]] (= v "true")) annotations))]
      (log/info logger (format "processing pod %s/%s, requesting labels [%s]"
                               namespace pod (str/join "," (map name requested-labels))))
      (if-let [{:keys [device-id pod node]} (assign-device ctx
                                                                     {:pod       pod
                                                                      :namespace ns
                                                                      :requested-labels requested-labels})]
        (do
          (log/info (:logger ctx) (format "Assigned device %s to pod %s/%s on node %s" device-id ns pod node))
          (admission-review-response :uid uid :allowed? true
                                     :patch (device-assignment-patch device-id)))
        (do
          (log/error (:logger ctx) (format "Failed to find unreserved device for pod %s/%s" ns pod))
          (admission-review-response :uid uid :status 500 :allowed? false
                                     :message (format "failed to find unreserved device for pod %s/%s." ns pod)))))))

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
