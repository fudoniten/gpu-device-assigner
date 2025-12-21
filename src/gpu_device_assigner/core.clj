(ns gpu-device-assigner.core
  (:require [clojure.string :as str]
            [clojure.stacktrace :refer [print-stack-trace]]
            [clojure.set :refer [subset?]]
            [clojure.spec.alpha :as s]
            [clojure.spec.test.alpha :as stest]

            [gpu-device-assigner.context :as context]
            [gpu-device-assigner.logging :as log]
            [gpu-device-assigner.k8s-client :as k8s]
            [gpu-device-assigner.time :as time]
            [gpu-device-assigner.util :as util]

            [reitit.ring :as ring]
            [cheshire.core :as json]
            [ring.util.response :as response]
            [ring.adapter.jetty :as jetty])
  (:import [java.time OffsetDateTime Duration]))

;;;; ==== Lease helpers

(defn get-claim-id
  "Extract the AdmissionReview request UID for the pod being mutated."
  [req]
  (or (get-in req [:request :object :metadata :uid])
      (get-in req [:request :uid])))

(defn claims-namespace
  "Namespace for Lease objects (centralized). You can also plumb this via ctx."
  [ctx]
  (get ctx :claims-namespace "gpu-claims"))

(def default-lease-seconds 300)

(defn lease-name
  "Format a Kubernetes Lease name from a GPU UUID."
  [gpu-uuid]
  (util/sanitize-for-dns (name gpu-uuid)))

(defn lease-body
  "Build a Lease object.
   `opts` may include {:node <node-name> :extra-labels {\"k\":\"v\" ...}}.
   The client sets :metadata.namespace when POSTing."
  ([device-uuid pod-uid]
   (lease-body device-uuid pod-uid {}))
  ([device-uuid pod-uid {:keys [node extra-labels]}]
   {:apiVersion "coordination.k8s.io/v1"
    :kind "Lease"
    :metadata {:name      (lease-name device-uuid)
               :namespace nil ;; set by client
               :labels    (cond-> {:fudo.org/gpu.uuid (name device-uuid)}
                            node (assoc :fudo.org/gpu.node (name node))
                            (seq extra-labels) (merge extra-labels))}
    :spec {:holderIdentity       pod-uid
           :leaseDurationSeconds default-lease-seconds
           :acquireTime          (time/now-rfc3339-micro)
           :renewTime            (time/now-rfc3339-micro)}}))

(defn lease-expired?
  "Return true if now - renewTime > leaseDurationSeconds (or missing renewTime)."
  [lease]
  (let [spec (:spec lease)
        dur  (long (or (:leaseDurationSeconds spec) default-lease-seconds))]
    (if-let [rt (:renewTime spec)]
      (let [then (OffsetDateTime/parse rt)
            age  (Duration/between then (OffsetDateTime/now))]
        (> (.getSeconds age) dur))
      ;; No renew time in lease
      true)))

(defn try-claim-uuid!
  "Atmoic claim attempt:
   - POST Lease -> 201 => win
   - 409 => GET; if expired => PATCH renew+holderIdentity => win
   - else lose"
  [{:keys [k8s-client logger namespace pod] :as ctx} device-uuid pod-uid]
  (let [pod-ns namespace
        ns   (claims-namespace ctx)
        nm   (lease-name device-uuid)
        body (lease-body device-uuid pod-uid
                         {"fudo.org/pod.namespace" pod-ns
                          "fudo.org/pod.name" pod})]
    (try
      (let [{:keys [status]} (util/pthru-label "LEASE-CREATE-RESPONSE" (k8s/create-lease k8s-client ns nm body))]
        (cond
          (= 201 status)
          (do (log/info logger (format "successfully claimed gpu %s for pod %s"
                                       device-uuid pod-uid))
              true)

          (= 409 status)
          (let [{lease :body} (k8s/get-lease k8s-client ns nm)]
            (if (lease-expired? lease)
              (let [{:keys [status]} (k8s/patch-lease k8s-client ns nm
                                                      {:spec {:holderIdentity pod-uid
                                                              :renewTime (time/now-rfc3339-micro)}})]
                (log/info logger (format "attempting to claim gpu %s for pod %s"
                                         device-uuid pod-uid))
                (<= 200 status 299))
              (do (log/warn logger (format "failed to claim gpu %s for pod %s, unexpired lease exists"
                                           device-uuid pod-uid))
                  false)))

          :else
          (do (log/error logger (format "unexpected error claiming gpu %s for pod %s"
                                        device-uuid pod-uid))
              false)))
      (catch Exception e
        (log/error logger (str "lease claim error for " (name device-uuid) ": " (.getMessage e)))
        (log/debug logger (with-out-str (print-stack-trace e)))
        false))))

;;;; ==== node annotations

(defn get-node-annotations
  "Retrieve annotations from a Kubernetes node."
  [{:keys [k8s-client]} node]
  (-> (k8s/get-node k8s-client node)
      :metadata
      :annotations))

(defn get-all-node-annotations
  "Fetch annotations for every node in the cluster."
  [{:keys [k8s-client]}]
  (into {}
        (map (fn [node]
               [(-> node :metadata :name)
                (-> node :metadata :annotations)]))
        (k8s/get-nodes k8s-client)))

(defn fudo-ns?
  "True when a keyword or symbol belongs to the fudo.org namespace."
  [o]
  (= (namespace o) "fudo.org"))

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
  "Decode and parse device label data from node annotations."
  [annotations]
  (some->> annotations
           :fudo.org/gpu.device.labels
           (util/base64-decode)
           (String.)
           (util/parse-json)))

(s/fdef get-all-device-labels
  :args ::context/context
  :ret  ::device-node-map)
(defn get-all-device-labels
  "Extract device label metadata for all nodes."
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
  "Filter devices whose labels satisfy the requested label set."
  [device-labels req-labels]
  (util/pthru-label "MATCHING DEVICES"
               (into {}
                     (filter
                      (fn [[_ {device-labels :labels}]]
                        (subset? (util/pthru-label "REQ" req-labels) (util/pthru-label "AVAIL" device-labels))))
                     device-labels)))

(s/fdef pick-device
  :args (s/cat :ctx      ::context/context
               :host-uid string?
               :labels   ::device-labels)
  :ret  (s/keys :req-un [::device-id ::node]))
(defn pick-device
  "Pick the first candidate device whose Lease we can claim atomically.
   Returns {:device-id <uuid> :node <node>} or nil."
  [{:keys [logger] :as ctx} pod-uid labels]
  (try
    (let [device-labels (get-all-device-labels ctx)]
      (log/debug logger (str "\n##########\n#  REQUESTED\n##########\n\n"
                             (util/pprint-string labels)))
      (log/debug logger (str "\n##########\n#  DEVICES\n##########\n\n"
                             (util/pprint-string device-labels)))
      (let [matching (find-matching-devices device-labels labels)]
        (log/debug logger (str "\n##########\n#  MATCHING\n##########\n\n"
                               (util/pprint-string matching)))
        ;; Iterate deterministically or randomly; here we randomize to spread load
        (let [order (shuffle (keys matching))]
          (some (fn [dev-uuid]
                  (when (try-claim-uuid! ctx dev-uuid pod-uid)
                    (log/info logger (str "\n******\n*** CLAIMED DEVICE %s\n******" dev-uuid))
                    {:device-id dev-uuid
                     :node      (-> matching dev-uuid :node)}))
                order))))
    (catch Exception e
      (throw (ex-info "Failed to pick device via Lease"
                      {:labels labels :exception e})))))

(stest/instrument 'pick-device)

(defn assign-device
  "Claim a GPU via Lease and return the JSONPatch-ready info."
  [{:keys [logger] :as ctx} {:keys [pod namespace requested-labels uid]}]
  ;; Make UID available to pick-device -> try-claim-uuid!
  (if-let [{:keys [device-id node]} (util/pthru-label "PICKED DEVICE"
                                                      (pick-device (assoc ctx
                                                                          :namespace namespace
                                                                          :pod pod)
                                                                   uid requested-labels))]
    (do (log/info logger (format "claimed lease for %s; assigning to pod %s/%s on node %s"
                                 device-id namespace pod node))
        {:device-id device-id :pod pod :namespace namespace :node node})
    (do (log/error logger (format "no free device (by Lease) for pod %s/%s, labels [%s]"
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
          (util/try-json-parse)
          (handler)
          (util/try-json-generate)
          (response/response)
          (assoc-in [:headers "Content-Type"] "application/json"))
      (throw (ex-info "missing request body!" {:req req})))))

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
        (json/generate-string)
        (util/base64-encode))))

(defn log-requests-middleware
  "Middleware that logs requests and responses at debug level."
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
    (log/debug (:logger ctx) (format "Received AdmissionReview request: %s" (util/pprint-string req)))
    (let [dry-run?         (true? (get-in req [:request :dryRun]))
          fudo-label?      (fn [[k _]] (= "fudo.org" (namespace k)))
          label-enabled?   (fn [[_ v]] v)
          gpu-label?       (fn [[k _]] (= "gpu" (first (str/split (name k) #"\."))))
          remove-assign    (fn [[k _]] (not= k :fudo.org/gpu.assign))
          uid              (get-in req [:request :uid])
          pod-uid          (get-claim-id req)
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
            (if-let [assigned-device (assign-device ctx {:pod              pod
                                                         :uid              pod-uid
                                                         :namespace        namespace
                                                         :requested-labels requested-labels})]
              (let [{:keys [device-id node]} assigned-device]
                (admission-review-response :uid uid :allowed? true
                                           :patch (device-assignment-patch ctx device-id node)))
              (admission-review-response :uid uid :status 429 :allowed? false
                                         :message (format "no GPUs with requested labels free for pod %s/%s"
                                                          namespace pod))))))))

(defn app
  "Construct the Ring application with routing and middleware."
  [ctx]
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
