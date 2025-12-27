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
(s/def ::jitter decimal?)

(defn now-rfc3339
  "Return the current time as an RFC3339 string."
  ^String []
  (.toString (OffsetDateTime/now)))

(defn sleep!
  "Sleep for the provided number of milliseconds."
  [ms]
  (Thread/sleep (long ms)))

(defn active-pod?
  "Return true when the pod is running or pending and not deleting."
  [pod]
  (let [phase (keyword (or (get-in pod [:status :phase]) "Unknown"))
        deleting? (some? (get-in pod [:metadata :deletionTimestamp]))]
    (and (not deleting?)
         (not (contains? #{:Succeeded :Failed} phase)))))

(defn- owner-reference
  [{:keys [uid name]}]
  (when (and uid name)
    {:apiVersion "v1"
     :kind       "Pod"
     :name       name
     :uid        uid
     :controller false
     :blockOwnerDeletion false}))

(defn finalize-reservation!
  "Verify a reservation lease is held for this pod and promote it to the pod UID."
  [{:keys [k8s-client claims-namespace]} {:keys [reservation-id device-id namespace uid] :as reservation}]
  (let [lease-name (core/lease-name device-id)
        lease      (log/trace! :lease/result (k8s/get-lease k8s-client claims-namespace lease-name))
        holder     (get-in lease [:spec :holderIdentity])]
    (cond
      (nil? lease)
      (log! :error (format "no lease %s/%s found for %s while finalizing reservation %s" claims-namespace lease-name device-id reservation-id))

      (not= reservation-id holder)
      (log! :error (format "lease %s/%s is held by %s, not reservation %s" claims-namespace lease-name holder reservation-id))

      (nil? uid)
      (log! :error (format "pod %s/%s missing UID; cannot finalize reservation %s yet" namespace name reservation-id))

      :else
      (let [existing-owners (get-in lease [:metadata :ownerReferences])
            owner-ref       (owner-reference reservation)
            patch           (cond-> {:metadata {:annotations {(name core/reservation-annotation) reservation-id}}
                                     :spec      {:holderIdentity       uid
                                                 :leaseDurationSeconds core/default-lease-seconds
                                                 :acquireTime          (or (get-in lease [:spec :acquireTime])
                                                                           (time/now-rfc3339-micro))
                                                 :renewTime            (time/now-rfc3339-micro)}}
                              (and owner-ref (not-any? #(= (:uid %) uid) existing-owners))
                              (assoc-in [:metadata :ownerReferences] (conj (vec existing-owners) owner-ref)))
            {:keys [status] :as res} (k8s/patch-lease k8s-client claims-namespace lease-name patch)]
        (if (<= 200 status 299)
          (log! :info (format "finalized reservation %s for pod %s/%s on %s"
                              reservation-id namespace (:name reservation) device-id))
          (log! :error (format "failed to finalize reservation %s for pod %s/%s on %s: %s"
                               reservation-id namespace (:name reservation) device-id (util/pprint-string res))))
        status))))

(defn renew-leases-once!
  "List leases in CLAIMS_NS; renew those whose holder pod is still active."
  [{:keys [k8s-client claims-namespace]}]
  (assert (string? claims-namespace))
  (try
    (let [leases (some-> (k8s/list-leases k8s-client claims-namespace)
                         :items)]
      (doseq [lease leases]
        (log/trace! (format "LEASE: %s" (with-out-str (pprint lease))))
        (let [ln     (get-in lease [:metadata :name])
              pod-ns (or (get-in lease [:metadata :labels :fudo.org/pod.namespace])
                         (get-in lease [:metadata :labels "fudo.org/pod.namespace"]))
              uid    (get-in lease [:spec :holderIdentity])]
          (cond
            (or (nil? uid) (empty? uid))
            (log! :debug (format "lease %s/%s has no holderIdentity; skipping"
                                 claims-namespace ln))

            (str/blank? pod-ns)
            (log! :debug (format "lease %s/%s missing pod namespace label; skipping"
                                 claims-namespace ln))

            :else
            (try
              (if-let [pod (k8s/get-pod-by-uid k8s-client pod-ns uid)]
                (if (active-pod? pod)
                  (do (k8s/patch-lease k8s-client claims-namespace ln
                                       {:spec {:renewTime (time/now-rfc3339-micro)}})
                      (log! :debug (format "renewed %s/%s for pod %s/%s (uid=%s)"
                                           claims-namespace ln
                                           (get-in pod [:metadata :namespace])
                                           (get-in pod [:metadata :name])
                                           uid)))
                  (log! :debug (format "pod %s/%s not active; not renewing %s/%s"
                                       (get-in pod [:metadata :namespace])
                                       (get-in pod [:metadata :name])
                                       claims-namespace ln)))
                ;; Pod not found: let it expire (or delete here if you want eager GC)
                (log! :debug (format "holder pod uid=%s not found; not renewing %s/%s"
                                     uid claims-namespace ln)))
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
