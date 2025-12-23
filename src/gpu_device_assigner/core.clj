(ns gpu-device-assigner.core
  (:require [clojure.string :as str]
            [clojure.stacktrace :refer [print-stack-trace]]
            [clojure.set :refer [subset?]]
            [clojure.spec.alpha :as s]
            [clojure.spec.test.alpha :as stest]

            [gpu-device-assigner.context :as context]
            [gpu-device-assigner.k8s-client :as k8s]
            [gpu-device-assigner.time :as time]
            [gpu-device-assigner.util :as util]
            [taoensso.timbre :as log])
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

(defn format-labels
  "Comma-separated label names for logging." 
  [labels]
  (if (seq labels)
    (str/join ", " (map name labels))
    "none"))

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
  [{:keys [k8s-client namespace pod] :as ctx} device-uuid pod-uid]
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
          (do (log/info (format "successfully claimed gpu %s for pod %s"
                                device-uuid pod-uid))
              true)

          (= 409 status)
          (let [{lease :body} (k8s/get-lease k8s-client ns nm)]
            (if (lease-expired? lease)
              (let [{:keys [status]} (k8s/patch-lease k8s-client ns nm
                                                      {:spec {:holderIdentity pod-uid
                                                              :renewTime (time/now-rfc3339-micro)}})]
                (log/info (format "attempting to claim gpu %s for pod %s"
                                  device-uuid pod-uid))
                (<= 200 status 299))
              (do (log/warn (format "failed to claim gpu %s for pod %s, unexpired lease exists"
                                    device-uuid pod-uid))
                  false)))

          :else
          (do (log/error (format "unexpected error claiming gpu %s for pod %s"
                                 device-uuid pod-uid))
              false)))
      (catch Throwable e
        (log/error (str "lease claim error for " (name device-uuid) ": " (.getMessage e)))
        (log/debug (with-out-str (print-stack-trace e)))
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
  (log/debugf "evaluating %s devices against requested labels %s"
              (count device-labels)
              (pr-str req-labels))
  (let [matching (into {}
                       (filter
                        (fn [[_ {device-labels :labels}]]
                          (subset? req-labels device-labels)))
                       device-labels)]
    (log/debugf "matching devices: %s" (pr-str matching))
    matching))

(s/fdef pick-device
  :args (s/cat :ctx      ::context/context
               :host-uid string?
               :labels   ::device-labels)
  :ret  (s/nilable (s/keys :req-un [::device-id ::node])))
(defn pick-device
  "Pick the first candidate device whose Lease we can claim atomically.
   Returns {:device-id <uuid> :node <node>} or nil."
  [ctx pod-uid labels]
  (try
    (let [device-labels (get-all-device-labels ctx)
          pod-name      (str (:namespace ctx) "/" (:pod ctx))
          available     (->> device-labels vals (mapcat :labels) set)]
      (log/infof "requested tags for pod %s: %s" pod-name (format-labels labels))
      (log/infof "available tags: %s" (format-labels available))
      (log/debugf "device label map: %s" (util/pprint-string device-labels))
      (if (empty? device-labels)
        (log/infof "no devices discovered when scheduling pod %s" pod-name)
        (let [matching (find-matching-devices device-labels labels)]
          (if (empty? matching)
            (log/infof "no matching devices available for pod %s" pod-name)
            ;; Iterate deterministically or randomly; here we randomize to spread load
            (let [result (when-let [order (shuffle (keys matching))]
                           (some (fn [dev-uuid]
                                   (try
                                     (when (try-claim-uuid! ctx dev-uuid pod-uid)
                                       (log/infof "claimed device %s for pod %s on node %s"
                                                  dev-uuid pod-name (-> matching dev-uuid :node))
                                       {:device-id dev-uuid
                                        :node      (-> matching dev-uuid :node)})
                                     (catch Throwable e
                                       (log/error e (format "Failed to claim device %s for pod %s"
                                                            dev-uuid pod-name))
                                       (log/debug (with-out-str (print-stack-trace e)))
                                       nil)))
                                 order))]
              result)))))
    (catch Throwable e
      (log/error e "Failed to pick device via Lease")
      (log/debug (with-out-str (print-stack-trace e)))
      nil)))

(stest/instrument 'pick-device)

(defn assign-device
  "Claim a GPU via Lease and return the JSONPatch-ready info."
  [ctx {:keys [pod namespace requested-labels uid]}]
  ;; Make UID available to pick-device -> try-claim-uuid!
  (if-let [{:keys [device-id node]} (util/pthru-label "PICKED DEVICE"
                                                      (pick-device (assoc ctx
                                                                          :namespace namespace
                                                                          :pod pod)
                                                                   uid requested-labels))]
    (do (log/info (format "claimed lease for %s; assigning to pod %s/%s on node %s"
                          device-id namespace pod node))
        {:device-id device-id :pod pod :namespace namespace :node node})
    (do (log/error (format "no free device (by Lease) for pod %s/%s, labels [%s]"
                           namespace pod (format-labels requested-labels)))
        nil)))

