(ns gpu-device-assigner.core
  (:require [clojure.string :as str]
            [clojure.stacktrace :refer [print-stack-trace]]
            [clojure.set :refer [subset?]]
            [clojure.pprint :refer [pprint]]
            [clojure.spec.alpha :as s]
            [clojure.spec.test.alpha :as stest]

            [gpu-device-assigner.context :as context]
            [gpu-device-assigner.logging :as log]
            [gpu-device-assigner.k8s-client :as k8s]

            [reitit.ring :as ring]
            [cheshire.core :as json]
            [ring.util.response :as response]
            [ring.adapter.jetty :as jetty])
  (:import java.util.Base64
           java.time.Instant))

(defn pprint-string [o]
  (with-out-str (pprint o)))

(defn pthru-label [lbl o]
  (println (str "###### " lbl))
  (pprint o)
  o)

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

(defn get-node-annotations
  "Retrieve annotations from a Kubernetes node."
  [{:keys [k8s-client]} node]
  (-> (k8s/get-node k8s-client node)
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
    (try
      (.decode decoder b64-str)
      (catch Exception e
        (println (format "failed to decode base64 string: %s: %s"
                         b64-str (.getMessage e)))
        (throw e)))))

(defn map-vals
  "Apply a function to all values in a map."
  [f m]
  (into {} (map (fn [[k v]] [k (f v)])) m))

(defn parse-json
  "Parse a JSON string into a Clojure data structure."
  [str]
  (json/parse-string str true))

(defn get-device-reservations
  "Retrieve current device reservations from node annotations."
  [ctx node]
  (try
    (let [annotations (get-node-annotations ctx node)]
      (some->> annotations
               :fudo.org/gpu.device.reservations
               (base64-decode)
               (String.)
               (parse-json)
               (assoc (select-keys annotations [:resourceVersion]) :reservations)))
    (catch Exception e
      (throw (ex-info "Failed to retrieve device reservations"
                      {:node      node
                       :exception e})))))

(defn fudo-ns? [o] (= (namespace o) "fudo.org"))

(s/def ::device-labels
  (s/and set?
         (s/coll-of keyword?)
         (s/every fudo-ns?)))

(s/def ::device-node-map
  (s/map-of ::device-id
            (s/keys :req-un [::node ::device-labels])))

(s/fdef -unpack-device-labels
  :args (s/cat :annotations (s/map-of symbol? any?))
  :ret  ::device-labels)
(defn -unpack-device-labels
  [annotations]
  (some->> annotations
           :fudo.org/gpu.device.labels
           (base64-decode)
           (String.)
           (parse-json)))

(s/def ::pod string?)
(s/def ::namespace string?)
(s/def ::timestamp string?)

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

(stest/instrument '-unpack-device-reservations)

(s/fdef get-all-device-labels
  :args ::context/context
  :ret  ::device-node-map)
(defn get-all-device-labels
  [ctx]
  (apply merge
         (map (fn [[node annos]]
                (into {}
                      (map (fn [[device labels]]
                             [device {:node   node
                                      :labels (set (map keyword labels))}]))
                      (-unpack-device-labels annos)))
              (get-all-node-annotations ctx))))

(stest/instrument 'get-all-device-labels)

(s/fdef find-matching-devices
  :args (s/cat :device-labels ::device-node-map
               :req-labels    ::device-labels)
  :ret  ::device-labels)
(defn find-matching-devices
  [device-labels req-labels]
  (pthru-label "MATCHING DEVICES"
               (into {}
                     (filter
                      (fn [[_ {device-labels :labels}]]
                        (subset? (pthru-label "REQ" req-labels) (pthru-label "AVAIL" device-labels))))
                     device-labels)))

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

(s/fdef pick-device
  :args (s/cat :ctx    ::context/context
               :labels ::device-labels)
  :ret  (s/keys :req-un [::device-id ::node]))
(defn pick-device [{:keys [logger] :as ctx} labels]
  (try
    (let [device-labels (get-all-device-labels ctx)
          reservations  (-> (get-all-device-reservations ctx) (keys) (set))]
      (log/debug logger (str "\n##########\n#  REQUESTED\n##########\n\n"
                             (pprint-string labels)))
      (log/debug logger (str "\n##########\n#  DEVICES\n##########\n\n"
                             (pprint-string device-labels)))
      (log/debug logger (str "\n##########\n#  RESERVATIONS\n##########\n\n"
                             (pprint-string reservations)))
      (let [matching      (find-matching-devices device-labels labels)
            available     (into {} (filter (fn [[dev-id _]] (not (reservations dev-id)))) matching)]
        (if (empty? (keys available))
          nil
          (let [selected-id (rand-nth (keys available))]
            {:device-id selected-id :node (-> (pthru-label "AVAILABLE" available) selected-id :node)}))))
    (catch Exception e
      (throw (ex-info "Failed to pick device"
                      {:labels labels
                       :exception e})))))

(stest/instrument 'pick-device)

(defn node-patch
  "Apply a patch to a Kubernetes node."
  [{:keys [k8s-client]} node patch]
  (try
    (k8s/patch-node k8s-client node patch)
    (catch Exception e
      (throw (ex-info "Failed to patch node"
                      {:node      node
                       :patch     patch
                       :exception e})))))

(defn reserve-device
  [{:keys [logger] :as ctx} node device-id pod namespace]
  (let [{version :resourceVersion reservations :reservations} (get-device-reservations ctx node)
        updated-reservations (assoc reservations (name device-id) {:pod pod :namespace namespace :timestamp (.toString (Instant/now))})
        reservation-patch {:kind "Node"
                           :metadata {:annotations
                                      {:fudo.org/gpu.device.reservations
                                       (base64-encode (json/generate-string updated-reservations))}}
                           :resourceVersion version}]
    (try (node-patch ctx node reservation-patch)
         {:device-id device-id :pod pod :namespace namespace :node node}
         (catch Exception e
           (log/debug logger (with-out-str (pprint e)))
           (let [status (ex-data e)]
             (if (= 409 (:status status))
               (log/error logger (format "conflict: reservation was modified before patch was applied. unable to reserve device %s for pod %s."
                                         device-id pod))
               (do (log/error logger (format "error %s: unable to reserve device %s for pod %s: %s"
                                             (:status status) device-id pod (:message e)))
                   (log/debug logger (with-out-str (print-stack-trace e)))))
             nil)))))

(defn assign-device
  "Assign a GPU device to a pod based on requested labels."
  [{:keys [logger] :as ctx} {:keys [pod namespace requested-labels]}]
  (if-let [device (pthru-label "PICKED DEVICE" (pick-device ctx requested-labels))]
    (let [{:keys [device-id node]} device]
      (log/info logger
                (format "attempting to reserve device %s on node %s to pod %s/%s"
                        device-id node namespace pod))
      (reserve-device ctx node device-id pod namespace))
    (do (log/error logger
                   (format "unable to find device to reserve for pod %s/%s, labels [%s]"
                           namespace pod (str/join "," (map name requested-labels))))
        nil)))

(defn json-middleware
  "Middleware to encode/decode the JSON body of requests/responses."
  [handler]
  (fn [req]
    (if-let [body {:body req}]
      (-> body
          :body
          :body ;; Who knows wtf??
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
                                  :kind       "AdmissionReview"
                                  :response   {:uid     uid
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
                 {:uid       uid
                  :allowed   allowed?
                  :status    {:code    status
                              :message message}}
                 {:uid       uid
                  :allowed   allowed?
                  :patchType "JSONPatch"
                  :patch     patch})})

(defn device-assignment-patch
  "Generate a JSON patch for assigning a device to a pod."
  [{:keys [logger]} device-id node]
  (let [patch [{:op    "add"
                :path  "/metadata/annotations"
                :value {}}
               {:op    "add"
                :path  "/metadata/annotations/cdi.k8s.io~1deviceName"
                :value (format "nvidia.com/%s" (name device-id))}
               {:op    "add"
                :path  "/spec/nodeName"
                :value (name node)}]]
    (log/debug logger (str "\n##########\n#  PATCH\n##########\n\n"
                           (pprint-string patch)))
    (-> patch
        (json/generate-string)
        (base64-encode))))

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
                    :status  {:code    400
                              :message (format "Unexpected request kind: %s" kind)}}})
    (log/debug (:logger ctx) (format "Received AdmissionReview request: %s" (pprint-string req)))
    (let [fudo-label?      (fn [[k _]] (= "fudo.org" (namespace k)))
          label-enabled?   (fn [[_ v]] v)
          gpu-label?       (fn [[k _]] (= "gpu" (first (str/split (name k) #"\."))))
          remove-assign    (fn [[k _]] (not= k :fudo.org/gpu.assign))
          uid              (get-in req [:request :uid])
          pod              (get-in req [:request :object :metadata :generateName])
          namespace        (get-in req [:request :object :metadata :namespace])
          all-labels       (get-in req [:request :object :metadata :labels])
          requested-labels (->> all-labels
                               (filter (every-pred fudo-label? label-enabled? gpu-label? remove-assign))
                               (keys)
                               (set))]
      (log/info logger (format "processing pod %s/%s, requesting labels [%s]"
                               namespace pod (str/join "," (map name requested-labels))))
      (if-let [assigned-device (assign-device ctx {:pod              pod
                                                   :namespace        namespace
                                                   :requested-labels requested-labels})]
        (let [{:keys [device-id pod node]} assigned-device]
          (log/info (:logger ctx) (format "Assigned device %s to pod %s/%s on node %s" device-id namespace pod node))
          (admission-review-response :uid uid :allowed? true
                                     :patch (device-assignment-patch ctx device-id node)))
        (do (log/error (:logger ctx) (format "Failed to find unreserved device for pod %s/%s" namespace pod))
            (admission-review-response :uid uid :status 500 :allowed? false
                                       :message (format "failed to find unreserved device for pod %s/%s." namespace pod)))))))

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
