(ns gpu-device-assigner.lease-renewer
  (:require [clojure.stacktrace :refer [print-stack-trace]]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [clojure.pprint :refer [pprint]]

            [gpu-device-assigner.core :as core]
            [gpu-device-assigner.k8s-client :as k8s]
            [gpu-device-assigner.time :as time]
            [taoensso.telemere :as log :refer [log!]]
            [gpu-device-assigner.util :as util])
  (:import java.time.OffsetDateTime))

(s/def ::claims-namespace string?)
(s/def ::renew-interval-ms integer?)
(s/def ::jitter number?)

(defn now-rfc3339
  "Return the current time as an RFC3339 string."
  ^String []
  (.toString (OffsetDateTime/now)))

(defn sleep!
  "Sleep for the provided number of milliseconds."
  [ms]
  (Thread/sleep (long ms)))

(def active-pod? core/active-pod?)

(defn finalize-reservation!
  "Verify a reservation lease is held for this pod and promote it to the pod UID.

  The patch carries the resourceVersion of the Lease we just read, so if
  anything else modified the Lease in the meantime (e.g. a takeover by another
  reservation) the API server rejects it with 409 instead of us clobbering it.

  No ownerReference to the Pod is set: Leases live in the claims namespace and
  cross-namespace owner references are invalid — the garbage collector treats
  the owner as absent and deletes the dependent."
  [{:keys [k8s-client claims-namespace]} {:keys [reservation-id device-id namespace pod-uid uid] :as reservation}]
  (let [lease-name (core/lease-name device-id)
        lease      (log/trace! :lease/result (k8s/get-lease k8s-client claims-namespace lease-name))
        holder     (get-in lease [:spec :holderIdentity])
        pod-uid    (or pod-uid uid)
        pod-name   (:name reservation)]
    (cond
      (not (k8s/ok? lease))
      (log! :error (format "could not read lease %s/%s for %s while finalizing reservation %s: %s"
                           claims-namespace lease-name device-id reservation-id
                           (pr-str (select-keys lease [:status :body]))))

      (not= reservation-id holder)
      (log! :error (format "lease %s/%s is held by %s, not reservation %s"
                           claims-namespace lease-name holder reservation-id))

      (nil? pod-uid)
      (log! :error (format "pod %s/%s missing UID; cannot finalize reservation %s yet"
                           namespace pod-name reservation-id))

      :else
      (let [now   (time/now-rfc3339-micro)
            rv    (get-in lease [:metadata :resourceVersion])
            patch {:metadata (cond-> {:labels      {(util/full-name core/reservation-state-label)
                                                    core/active-reservation}
                                      :annotations {(util/full-name core/reservation-annotation)
                                                    reservation-id}}
                               rv (assoc :resourceVersion rv))
                   :spec     {:holderIdentity       pod-uid
                              :leaseDurationSeconds core/default-lease-seconds
                              :acquireTime          (or (get-in lease [:spec :acquireTime]) now)
                              :renewTime            now}}
            {:keys [status] :as res} (k8s/patch-lease k8s-client claims-namespace lease-name patch)]
        (if (k8s/ok? res)
          (log! :info (format "finalized reservation %s for pod %s/%s on %s"
                              reservation-id namespace pod-name device-id))
          (log! :error (format "failed to finalize reservation %s for pod %s/%s on %s: %s"
                               reservation-id namespace pod-name device-id (util/pprint-string res))))
        status))))

(defn- pod-using-device?
  "Return true when the pod's GPU annotations reference `device-id`.
   Both annotations may be comma-separated lists for multi-GPU pods."
  [pod device-id]
  (let [annotations (get-in pod [:metadata :annotations])
        gpu-uuid    (core/annotation-value annotations core/gpu-annotation)
        assignment  (core/annotation-value annotations :cdi.k8s.io/gpu-assignment)
        dev-str     (name device-id)]
    (boolean
     (or (when gpu-uuid
           (some #(= dev-str (str/trim %))
                 (str/split (name gpu-uuid) #",")))
         (when assignment
           (some #(str/ends-with? (str/trim %) dev-str)
                 (str/split assignment #",")))))))

(defn- delete-lease!
  "Delete a lease, but only if it is unchanged since we listed it (DeleteOptions
   precondition on resourceVersion). Without the precondition, a lease that was
   re-claimed by a new pod between our list and our delete would be removed
   out from under that pod."
  [{:keys [k8s-client claims-namespace]} lease reason]
  (let [ln  (get-in lease [:metadata :name])
        rv  (get-in lease [:metadata :resourceVersion])
        res (if rv
              (k8s/delete-lease k8s-client claims-namespace ln rv)
              (k8s/delete-lease k8s-client claims-namespace ln))]
    (cond
      (k8s/ok? res)
      (log! :info (format "deleted lease %s/%s (%s)" claims-namespace ln reason))

      (= 409 (:status res))
      (log! :info (format "lease %s/%s changed since it was read; not deleting (%s)"
                          claims-namespace ln reason))

      (k8s/not-found? res)
      (log! :debug (format "lease %s/%s already gone (%s)" claims-namespace ln reason))

      :else
      (log! :error (format "failed to delete lease %s/%s (%s): %s"
                           claims-namespace ln reason (util/pprint-string res))))))

(defn- pods-cache
  "Per-pass cache of namespace pod lists, so N leases in one namespace cost
   one list call. Failed lists are cached too (as {:ok? false}) so a flapping
   API server is not hammered within a single pass."
  [ctx]
  (let [cache (atom {})]
    (fn [ns]
      (or (get @cache ns)
          (let [r (core/namespace-pods ctx ns)]
            (swap! cache assoc ns r)
            r)))))

(defn- process-proposal!
  [ctx lease {:keys [ok? pods]} {:keys [ln pod-ns device-id reservation-id]}]
  (let [age    (core/lease-age-seconds lease)
        young? (or (nil? age) (< age core/proposal-grace-seconds))]
    (if-not ok?
      (log! :warn (format "cannot list pods in %s; leaving pending lease %s alone this pass" pod-ns ln))
      (if-let [pod (some #(when (= reservation-id (core/pod-reservation-id %)) %) pods)]
        (let [pod-uid  (get-in pod [:metadata :uid])
              pod-name (get-in pod [:metadata :name])]
          (cond
            (not (active-pod? pod))
            (delete-lease! ctx lease (format "pod %s/%s not active for pending reservation"
                                             pod-ns pod-name))

            (pod-using-device? pod device-id)
            (finalize-reservation! ctx {:reservation-id reservation-id
                                        :device-id      device-id
                                        :namespace      pod-ns
                                        :pod-uid        pod-uid
                                        :name           pod-name})

            :else
            (delete-lease! ctx lease (format "pod %s/%s not using assigned device %s"
                                             pod-ns pod-name device-id))))
        ;; No pod yet. Within the grace period the pod may simply not have
        ;; been persisted yet, so leave the proposal in place.
        (if young?
          (log! :debug (format "pending lease %s has no pod yet (age %ss); within grace period" ln age))
          (delete-lease! ctx lease (format "no pod found for pending reservation %s" reservation-id)))))))

(defn- process-active!
  [{:keys [k8s-client claims-namespace] :as ctx} lease {:keys [ok? pods]} {:keys [ln pod-ns uid]}]
  (if-not ok?
    (log! :warn (format "cannot list pods in %s; leaving lease %s alone this pass" pod-ns ln))
    (if-let [pod (some #(when (= uid (get-in % [:metadata :uid])) %) pods)]
      (if (active-pod? pod)
        (let [res (k8s/patch-lease k8s-client claims-namespace ln
                                   {:spec {:renewTime (time/now-rfc3339-micro)}})]
          (if (k8s/ok? res)
            (log! :debug (format "renewed %s/%s for pod %s/%s (uid=%s)"
                                 claims-namespace ln pod-ns (get-in pod [:metadata :name]) uid))
            (log! :warn (format "failed to renew %s/%s: %s"
                                claims-namespace ln (pr-str (select-keys res [:status :body]))))))
        (delete-lease! ctx lease (format "pod %s/%s not active"
                                         pod-ns (get-in pod [:metadata :name]))))
      (delete-lease! ctx lease (format "holder pod uid=%s not found" uid)))))

(defn renew-leases-once!
  "List leases in CLAIMS_NS; renew active ones, finalize pending ones, and delete stale ones.

  A lease is only ever deleted when its holder is *verifiably* gone: if the
  pod list for its namespace cannot be read, the lease is left alone."
  [{:keys [k8s-client claims-namespace] :as ctx}]
  (assert (string? claims-namespace))
  (try
    (let [resp (k8s/list-leases k8s-client claims-namespace)
          pods (pods-cache ctx)]
      (if-not (k8s/ok? resp)
        (log! :warn (format "failed to list leases in %s: %s"
                            claims-namespace (pr-str (select-keys resp [:status :body]))))
        (doseq [lease (:items resp)]
          (log/trace! (format "LEASE: %s" (with-out-str (pprint lease))))
          (let [ln             (get-in lease [:metadata :name])
                pod-ns         (core/lease-pod-namespace lease)
                state          (core/lease-state lease)
                uid            (get-in lease [:spec :holderIdentity])
                device-id      (core/lease-device-id lease)
                reservation-id (core/lease-reservation-id lease)
                info           {:ln ln :pod-ns pod-ns :device-id device-id
                                :uid uid :reservation-id reservation-id}]
            (try
              (cond
                (str/blank? device-id)
                (log! :warn (format "lease %s/%s missing device id label; skipping"
                                    claims-namespace ln))

                (str/blank? pod-ns)
                (log! :warn (format "lease %s/%s missing pod namespace label; skipping"
                                    claims-namespace ln))

                (str/blank? uid)
                (delete-lease! ctx lease "empty holderIdentity")

                (= state core/proposed-reservation)
                (process-proposal! ctx lease (pods pod-ns) info)

                :else
                (process-active! ctx lease (pods pod-ns) info))
              (catch Exception e
                (log/error! e (format "error processing lease %s/%s: %s"
                                      claims-namespace ln (.getMessage e)))
                (log! :debug (with-out-str (print-stack-trace e)))))))))
    (catch Exception e
      (log/error! e (str "renew pass failed: " (.getMessage e)))
      (log! :debug (with-out-str (print-stack-trace e))))))

(defn run-renewer!
  "Start a loop that periodically renews leases.
   ctx keys: :k8s-client :claims-namespace :renew-interval-ms :jitter"
  [{:keys [:renew-interval-ms :jitter] :as ctx}]
  (let [interval (long (or renew-interval-ms 60000))
        jt       (double (or jitter 0.2))
        jittered (fn [ms]
                   (let [d (long (* ms jt)) r (rand-int (inc (* 2 d)))]
                     (- (+ ms r) d)))]
    (log! :info (format "lease-renewer scanning leases every ~%dms (±%.0f%%)"
                        interval (* jt 100.0)))
    (while true
      (renew-leases-once! ctx)
      (sleep! (jittered interval)))))
