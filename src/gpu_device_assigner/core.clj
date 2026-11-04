(ns gpu-device-assigner.core
  (:require [clojure.string :as str]
            [clojure.stacktrace :refer [print-stack-trace]]
            [clojure.set :refer [subset?]]
            [gpu-device-assigner.k8s-client :as k8s]
            [gpu-device-assigner.time :as time]
            [gpu-device-assigner.util :as util]
            [taoensso.telemere :as log :refer [log!]])
  (:import [java.time OffsetDateTime Duration]))

;;;; ==== Lease helpers

(defn get-claim-id
  "Extract the identifier used for the initial reservation.

  When pods are first created they do not yet have a stable pod UID, so we
  fall back to the AdmissionReview request UID. This value becomes the initial
  lease `holderIdentity` and is written to `fudo.org/gpu.reservation-id` so it
  can later be promoted to the real pod UID during reservation finalization."
  [req]
  (or (get-in (log/trace! :admission/request req) [:request :object :metadata :uid])
      (get-in req [:request :uid])))

(defn claims-namespace
  "Namespace for Lease objects (centralized). You can also plumb this via ctx."
  [ctx]
  (get ctx :claims-namespace "gpu-claims"))

(def default-lease-seconds 300)

;; A proposal is only promoted to an active lease when the renewer next runs
;; (every ~60s ± jitter), so its nominal duration must comfortably exceed one
;; renewer interval. Expiry alone never makes a lease claimable any more (see
;; `lease-claimable?`), so this is informational, but keep it honest.
(def reservation-lease-seconds 180)

;; How long a proposal is protected unconditionally. Between the webhook
;; creating the Lease and the API server persisting the Pod there is a window
;; in which no pod carries the reservation id yet; within this grace period a
;; proposal is never stolen nor garbage collected.
(def proposal-grace-seconds 120)

(def gpu-annotation :fudo.org/gpu.uuid)
(def reservation-annotation :fudo.org/gpu.reservation-id)
(def reservation-state-label :fudo.org/gpu.reservation-state)
(def proposed-reservation "proposal")
(def active-reservation   "active")

(def gpu-uuid-label     "fudo.org/gpu.uuid")
(def gpu-node-label     "fudo.org/gpu.node")
(def pod-namespace-label "fudo.org/pod.namespace")

(defn format-labels
  "Comma-separated label names for logging."
  [labels]
  (if (seq labels)
    (str/join ", " (map util/full-name labels))
    "none"))

(defn map-value
  "Look up `k` in a map whose keys may be keywords (as parsed from the API) or
   strings (as built locally). `k` may be either."
  [m k]
  (or (get m k)
      (get m (util/full-name k))
      (get m (keyword (util/full-name k)))))

(defn annotation-value
  [annotations k]
  (map-value annotations k))

(defn string-keys
  "Convert a map's (possibly namespaced keyword) keys to full strings."
  [m]
  (into {} (map (fn [[k v]] [(util/full-name k) v])) m))

(defn lease-name
  "Format a Kubernetes Lease name from a GPU UUID."
  [gpu-uuid]
  (util/sanitize-for-dns (name gpu-uuid)))

(defn lease-body
  "Build a Lease object.
   `opts` may include {:node <node-name>
                       :extra-labels {\"k\":\"v\" ...}
                       :lease-duration-seconds <int>
                       :reservation-id <string>}.
   The client sets :metadata.namespace when POSTing."
  ([device-uuid holder]
   (lease-body device-uuid holder {}))
  ([device-uuid holder {:keys [node extra-labels lease-duration-seconds reservation-id]}]
   (let [now (time/now-rfc3339-micro)]
     {:apiVersion "coordination.k8s.io/v1"
      :kind "Lease"
      :metadata {:name        (lease-name device-uuid)
                 :namespace   nil ;; set by client
                 :labels      (cond-> {gpu-uuid-label (name device-uuid)}
                                node               (assoc gpu-node-label (name node))
                                reservation-id     (assoc (util/full-name reservation-state-label)
                                                          proposed-reservation)
                                (seq extra-labels) (merge (string-keys extra-labels)))
                 :annotations (cond-> {}
                                reservation-id (assoc (util/full-name reservation-annotation)
                                                      reservation-id))}
      :spec {:holderIdentity       holder
             :leaseDurationSeconds (long (or lease-duration-seconds default-lease-seconds))
             :acquireTime          now
             :renewTime            now}})))

(defn lease-age-seconds
  "Seconds since the lease's renewTime, or nil when it has none."
  [lease]
  (when-let [rt (get-in lease [:spec :renewTime])]
    (.getSeconds (Duration/between (OffsetDateTime/parse rt) (OffsetDateTime/now)))))

(defn lease-expired?
  "Return true if now - renewTime > leaseDurationSeconds (or missing renewTime)."
  [lease]
  (let [dur (long (or (get-in lease [:spec :leaseDurationSeconds]) default-lease-seconds))]
    (if-let [age (lease-age-seconds lease)]
      (> age dur)
      true)))

(defn lease-state [lease]
  (map-value (get-in lease [:metadata :labels]) reservation-state-label))

(defn lease-pod-namespace [lease]
  (map-value (get-in lease [:metadata :labels]) pod-namespace-label))

(defn lease-device-id [lease]
  (map-value (get-in lease [:metadata :labels]) gpu-uuid-label))

(defn lease-reservation-id [lease]
  (or (annotation-value (get-in lease [:metadata :annotations]) reservation-annotation)
      (get-in lease [:spec :holderIdentity])))

;;;; ==== Pod lookups

(defn active-pod?
  "Return true when the pod is running or pending and not deleting."
  [pod]
  (let [phase     (keyword (or (get-in pod [:status :phase]) "Unknown"))
        deleting? (some? (get-in pod [:metadata :deletionTimestamp]))]
    (and (not deleting?)
         (not (contains? #{:Succeeded :Failed} phase)))))

(defn namespace-pods
  "List pods in `namespace`. Returns {:ok? true :pods [...]} or {:ok? false}.
   Callers must treat {:ok? false} as *unknown*, never as \"no pods\"."
  [{:keys [k8s-client]} namespace]
  (let [resp (k8s/get-namespace-pods k8s-client namespace)]
    (if (and (k8s/ok? resp) (contains? resp :items))
      {:ok? true :pods (vec (:items resp))}
      (do (log! :warn (format "failed to list pods in namespace %s: %s"
                              namespace (pr-str (select-keys resp [:status :body]))))
          {:ok? false}))))

(defn pod-reservation-id [pod]
  (annotation-value (get-in pod [:metadata :annotations]) reservation-annotation))

(defn find-lease-holder
  "Find the pod currently holding `lease`, given the pods of its namespace.
   For proposals the holder is matched via the reservation annotation (the
   holderIdentity is an admission request UID); for active leases via pod UID."
  [lease pods]
  (let [holder (get-in lease [:spec :holderIdentity])]
    (if (= proposed-reservation (lease-state lease))
      (some #(when (= holder (pod-reservation-id %)) %) pods)
      (some #(when (= holder (get-in % [:metadata :uid])) %) pods))))

(defn lease-claimable?
  "Decide whether an existing lease may be taken over by a new reservation.

   A lease is claimable only when its holder is *verifiably* gone or inactive.
   API errors, and proposals younger than `proposal-grace-seconds`, mean the
   lease stays held. Expiry alone is not enough: a renewer outage must not let
   a running pod's GPU be handed to someone else."
  [ctx lease]
  (let [holder (get-in lease [:spec :holderIdentity])
        pod-ns (lease-pod-namespace lease)
        age    (lease-age-seconds lease)]
    (cond
      (str/blank? holder)
      true

      (str/blank? pod-ns)
      ;; Malformed lease (no namespace to look the holder up in).
      (lease-expired? lease)

      (and (= proposed-reservation (lease-state lease))
           (or (nil? age) (< age proposal-grace-seconds)))
      false

      :else
      (let [{:keys [ok? pods]} (namespace-pods ctx pod-ns)]
        (if-not ok?
          false
          (let [pod (find-lease-holder lease pods)]
            (or (nil? pod) (not (active-pod? pod)))))))))

(defn lease-claim-patch
  "Construct a JSONPatch that takes over `lease` for `holder-identity`.

  The leading `test` op on resourceVersion makes the takeover atomic: if any
  other writer (another replica, the renewer) touched the Lease since we read
  it, the API server rejects the whole patch."
  [lease device-uuid holder-identity lease-seconds pod-ns]
  (let [now         (time/now-rfc3339-micro)
        labels      (-> (string-keys (get-in lease [:metadata :labels]))
                        (assoc gpu-uuid-label (name device-uuid)
                               (util/full-name reservation-state-label) proposed-reservation)
                        (cond-> pod-ns (assoc pod-namespace-label pod-ns)))
        annotations (-> (string-keys (get-in lease [:metadata :annotations]))
                        (assoc (util/full-name reservation-annotation) holder-identity))
        patch       [{:op "test" :path "/metadata/resourceVersion"
                      :value (get-in lease [:metadata :resourceVersion])}
                     {:op "add" :path "/metadata/labels" :value labels}
                     {:op "add" :path "/metadata/annotations" :value annotations}
                     {:op "add" :path "/spec" :value {:holderIdentity       holder-identity
                                                      :leaseDurationSeconds lease-seconds
                                                      :acquireTime          now
                                                      :renewTime            now}}]]
    (log/trace! (str "\n##########\n# LEASE PATCH\n##########\n\n" (util/pprint-string patch)))
    (util/try-json-generate patch)))

(defn try-claim-uuid!
  "Atomic claim attempt:
   - POST Lease -> 2xx => win
   - 409 => GET; if the holder is verifiably gone, PATCH (guarded by a
     resourceVersion test) => win if the patch applies
   - else lose (false), or nil on unexpected errors."
  [{:keys [k8s-client namespace] :as ctx} device-uuid holder-identity & {:keys [lease-duration-seconds]}]
  (let [pod-ns        namespace
        ns            (claims-namespace ctx)
        nm            (lease-name device-uuid)
        lease-seconds (long (or lease-duration-seconds reservation-lease-seconds))
        body          (lease-body device-uuid holder-identity
                                  {:lease-duration-seconds lease-seconds
                                   :reservation-id         holder-identity
                                   :extra-labels           {pod-namespace-label pod-ns}})]
    (try
      (loop [attempt 1]
        (let [{:keys [status] :as resp} (log/trace! :lease/response
                                                    (k8s/create-lease k8s-client ns nm
                                                                      (log/trace! :lease/request body)))]
          (cond
            (k8s/ok? resp)
            (do (log! :info (format "successfully claimed gpu %s for reservation %s"
                                    (name device-uuid) holder-identity))
                true)

            (= 409 status)
            (let [lease-resp (k8s/get-lease k8s-client ns nm)]
              (cond
                ;; Deleted between our POST and GET: try the POST once more.
                (and (k8s/not-found? lease-resp) (< attempt 2))
                (recur (inc attempt))

                (not (k8s/ok? lease-resp))
                (do (log! :warn (format "could not read existing lease for gpu %s: %s"
                                        (name device-uuid) (pr-str (select-keys lease-resp [:status :body]))))
                    false)

                (not (lease-claimable? ctx lease-resp))
                (do (log! :info (format "gpu %s is held by %s; not claiming for reservation %s"
                                        (name device-uuid)
                                        (get-in lease-resp [:spec :holderIdentity])
                                        holder-identity))
                    false)

                (nil? (get-in lease-resp [:metadata :resourceVersion]))
                (do (log! :warn (format "lease for gpu %s has no resourceVersion; refusing unsafe takeover"
                                        (name device-uuid)))
                    false)

                :else
                (let [patch-json (lease-claim-patch lease-resp device-uuid holder-identity lease-seconds pod-ns)
                      patch-resp (k8s/patch-lease k8s-client ns nm patch-json)]
                  (if (k8s/ok? patch-resp)
                    (do (log! :info (format "took over stale lease on gpu %s for reservation %s"
                                            (name device-uuid) holder-identity))
                        true)
                    (do (log! :info (format "lost race taking over gpu %s for reservation %s: %s"
                                            (name device-uuid) holder-identity
                                            (pr-str (:status patch-resp))))
                        false)))))

            :else
            (do (log! :error (format "unexpected error claiming gpu %s for reservation %s: %s"
                                     (name device-uuid) holder-identity (util/pprint-string resp)))
                nil))))
      (catch Throwable e
        (log/error! (str "lease claim error for " (name device-uuid) ": " (.getMessage e)))
        (log! :debug (with-out-str (print-stack-trace e)))
        nil))))

;;;; ==== node annotations

(defn get-node-annotations
  "Retrieve annotations from a Kubernetes node."
  [{:keys [k8s-client]} node]
  (-> (k8s/get-node k8s-client node)
      :metadata
      :annotations))

(defn node-ready?
  "True when the node reports Ready=True."
  [node]
  (boolean (some #(and (= "Ready" (:type %)) (= "True" (:status %)))
                 (get-in node [:status :conditions]))))

(defn node-gpu-allocatable
  "Number of nvidia.com/gpu the kubelet reports as allocatable, or nil."
  [node]
  (let [v (map-value (get-in node [:status :allocatable]) "nvidia.com/gpu")]
    (cond
      (integer? v) (long v)
      (string? v)  (try (Long/parseLong v) (catch NumberFormatException _ nil))
      :else        nil)))

(defn node-assignable?
  "A node may receive GPU pods only if it is Ready, not cordoned, and the
   device plugin advertises at least one GPU. Pods are pinned via
   spec.nodeName, which bypasses the scheduler (and with it, cordons and
   NoSchedule taints), so this filter is the only protection against pinning a
   pod to a dead or draining node."
  [node]
  (let [node-name   (get-in node [:metadata :name])
        allocatable (node-gpu-allocatable node)]
    (cond
      (not (node-ready? node))
      (do (log! :info (format "skipping node %s: not Ready" node-name)) false)

      (get-in node [:spec :unschedulable])
      (do (log! :info (format "skipping node %s: cordoned" node-name)) false)

      (or (nil? allocatable) (< allocatable 1))
      (do (log! :info (format "skipping node %s: no allocatable nvidia.com/gpu" node-name)) false)

      :else true)))

(defn get-all-node-annotations
  "Fetch annotations for every node in the cluster (or only assignable nodes
   when `assignable-only?`)."
  ([ctx] (get-all-node-annotations ctx false))
  ([{:keys [k8s-client]} assignable-only?]
   (into {}
         (comp (filter #(or (not assignable-only?) (node-assignable? %)))
               (map (fn [node]
                      [(-> node :metadata :name)
                       (-> node :metadata :annotations)])))
         (k8s/get-nodes k8s-client))))

(defn fudo-ns?
  "True when a keyword or symbol belongs to the fudo.org namespace."
  [o]
  (= (namespace o) "fudo.org"))

(defn -unpack-device-labels
  "Decode and parse device label data from node annotations."
  [annotations]
  (some->> (annotation-value annotations :fudo.org/gpu.device.labels)
           (util/base64-decode)
           (String.)
           (util/parse-json)))

(defn get-all-device-labels
  "Extract device label metadata for all nodes (or only assignable nodes when
   `assignable-only?`). Returns {device-id {:node <node> :labels #{...}}}."
  ([ctx] (get-all-device-labels ctx false))
  ([ctx assignable-only?]
   (into {}
         (mapcat (fn [[node annos]]
                   (map (fn [[device labels]]
                          [device {:node   node
                                   :labels (set (map keyword labels))}])
                        (-unpack-device-labels annos))))
         (get-all-node-annotations ctx assignable-only?))))

(defn find-matching-devices
  "Filter devices whose labels satisfy the requested label set."
  [device-labels req-labels]
  (log! :debug
        (format "evaluating %s devices against requested labels %s"
                (count device-labels)
                (pr-str req-labels)))
  (let [matching (into {}
                       (filter
                        (fn [[_ {device-labels :labels}]]
                          (subset? req-labels device-labels)))
                       device-labels)]
    (log! :debug (format "matching devices: %s" (pr-str matching)))
    matching))

(defn release-lease!
  "Delete a GPU lease. Used for rollback when a partial multi-GPU claim fails."
  [{:keys [k8s-client] :as ctx} device-uuid]
  (let [ns (claims-namespace ctx)
        nm (lease-name device-uuid)]
    (log! :info (format "rolling back lease for device %s" (name device-uuid)))
    (k8s/delete-lease k8s-client ns nm)))

(defn device-score
  "Best-fit score: lower is preferred. A device with fewer labels is less
   capable/versatile, so handing it out first keeps the more versatile cards
   free for workloads that need them (e.g. a TTS pod lands on a P4 rather than
   a V100)."
  [[_ {:keys [labels]}]]
  (count labels))

(defn- claim-on-node!
  "Try every candidate device on `node` (best fit first) until `n` are claimed.
   Rolls back and returns nil if fewer than `n` could be claimed."
  [ctx holder-identity pod-name node devices n]
  (log! :info (format "attempting to claim %d device(s) on node %s for pod %s" n node pod-name))
  (let [claimed (reduce (fn [claimed [dev-uuid _]]
                          (if (= n (count claimed))
                            (reduced claimed)
                            (if (try-claim-uuid! ctx dev-uuid holder-identity)
                              (do (log! :info (format "claimed device %s for pod %s on node %s"
                                                      (name dev-uuid) pod-name node))
                                  (conj claimed {:device-id dev-uuid :node node}))
                              claimed)))
                        []
                        devices)]
    (if (= n (count claimed))
      claimed
      (do (when (seq claimed)
            (log! :info (format "rolling back %d partial claim(s) on node %s for pod %s"
                                (count claimed) node pod-name)))
          (doseq [{:keys [device-id]} claimed]
            (try (release-lease! ctx device-id)
                 (catch Throwable re
                   (log! :error (format "failed to roll back lease for %s: %s"
                                        (name device-id) (.getMessage re))))))
          nil))))

(defn pick-devices
  "Claim exactly `n` GPUs matching `labels`, all on the same node.
   Returns [{:device-id <uuid> :node <node>} ...] or nil.
   Devices and nodes are tried best-fit first (see `device-score`), with ties
   broken randomly. On partial failure, already-claimed leases are rolled back."
  [ctx holder-identity labels n]
  (try
    (let [device-labels (get-all-device-labels ctx true)
          pod-name      (str (:namespace ctx) "/" (:pod ctx))
          available     (->> device-labels vals (mapcat :labels) set)]
      (log! :info (format "requested tags for pod %s: %s" pod-name (format-labels labels)))
      (log! :info (format "available tags: %s" (format-labels available)))
      (log! :debug (format "device label map: %s" (util/pprint-string device-labels)))
      (if (empty? device-labels)
        (do (log! :info (format "no devices discovered on assignable nodes when scheduling pod %s" pod-name)) nil)
        (let [matching (find-matching-devices device-labels labels)]
          (if (empty? matching)
            (do (log! :info (format "no matching devices available for pod %s" pod-name)) nil)
            ;; Group matching devices by node; only nodes with >= n devices qualify
            (let [by-node (->> matching
                               (group-by (fn [[_ {:keys [node]}]] node))
                               (filter (fn [[_ devs]] (>= (count devs) n)))
                               (map (fn [[node devs]] [node (sort-by device-score (shuffle devs))]))
                               shuffle
                               (sort-by (fn [[_ devs]] (reduce + (map device-score (take n devs))))))]
              (if (empty? by-node)
                (do (log! :info (format "no single node has %d matching devices for pod %s" n pod-name)) nil)
                (some (fn [[node devices]]
                        (claim-on-node! ctx holder-identity pod-name node devices n))
                      by-node)))))))
    (catch Throwable e
      (log/error! e "Failed to pick devices via Lease")
      (log! :debug (with-out-str (print-stack-trace e)))
      nil)))

(defn pod-uid->pod
  "Lookup a pod by UID within the provided namespace."
  [{:keys [k8s-client]} namespace pod-uid]
  (when (and namespace pod-uid)
    (k8s/get-pod-by-uid k8s-client namespace pod-uid)))

(defn- lease->assignment
  "Extract the device and pod assignment info from a Lease resource."
  [lease]
  (let [labels      (get-in lease [:metadata :labels])
        annotations (get-in lease [:metadata :annotations])
        device      (or (get labels "fudo.org/gpu.uuid")
                        (get labels :fudo.org/gpu.uuid))
        pod-ns      (or (get labels "fudo.org/pod.namespace")
                        (get labels :fudo.org/pod.namespace))
        reservation (annotation-value annotations reservation-annotation)
        state       (or (get labels reservation-state-label)
                        (get labels (util/full-name reservation-state-label)))
        pod-uid     (if (= proposed-reservation state)
                      nil
                      (get-in lease [:spec :holderIdentity]))]
    (when device
      [(keyword device)
       {:device-id      device
        :reservation-id reservation
        :state          state
        :pod            {:namespace pod-ns
                         :uid       pod-uid}}])))

(defn device-inventory
  "Return a map of discovered devices and their current assignments.
   Keys are device IDs; values include node, labels, and optional pod assignment."
  [{:keys [k8s-client] :as ctx}]
  (let [device-labels (get-all-device-labels ctx)
        assignments   (->> (k8s/list-leases k8s-client (claims-namespace ctx))
                           :items
                           (keep lease->assignment)
                           (into {}))]
    (into {}
          (map (fn [[device {:keys [node labels]}]]
                 (if-let [assignment (get assignments (keyword device))]
                   (let [reservation-id (:reservation-id assignment)
                         state          (:state assignment)
                         pod            (:pod (log/trace! :device/assignment assignment))
                         pod-detail     (when (and pod (= active-reservation state))
                                          (log/trace! :device/pod-detail
                                                      (pod-uid->pod ctx (:namespace pod) (:uid pod))))
                         exists?        (boolean pod-detail)
                         pod-info       (cond-> {:exists? exists?}
                                          pod              (merge pod)
                                          pod-detail       (assoc :name (get-in pod-detail [:metadata :name]))
                                          (some? reservation-id) (assoc :reservation-id reservation-id)
                                          (some? state)    (assoc :state state))]
                     [device {:node       node
                              :labels     (-> labels sort vec)
                              :assignment pod-info}])
                   [device {:node node :labels labels}])))
          device-labels)))

(defn assign-device
  "Claim one or more GPUs via Lease and return the JSONPatch-ready info.
   `:gpu-count` defaults to 1; all GPUs will be on the same node."
  [ctx {:keys [pod namespace requested-labels reservation-id gpu-count]}]
  (let [requested-labels (set (map keyword requested-labels))
        n                (max 1 (long (or gpu-count 1)))]
    (if-let [devices (log/trace! :device/selection
                                 (pick-devices (assoc ctx
                                                      :namespace namespace
                                                      :pod pod)
                                              reservation-id requested-labels n))]
      (let [node (-> devices first :node)]
        (log! :info (format "claimed %d lease(s) for pod %s/%s on node %s: [%s]"
                            (count devices) namespace pod node
                            (str/join ", " (map (comp name :device-id) devices))))
        {:devices        devices
         :pod            pod
         :namespace      namespace
         :node           node
         :reservation-id reservation-id})
      (do (log! :error (format "no free %d-GPU set (by Lease) for pod %s/%s, labels [%s]"
                               n namespace pod (format-labels requested-labels)))
          nil))))
