(ns gpu-device-assigner.lease-renewer
  (:require [clojure.stacktrace :refer [print-stack-trace]]
            [clojure.spec.alpha :as s]

            [gpu-device-assigner.k8s-client :as k8s]
            [gpu-device-assigner.time :as time]
            [taoensso.telemere :as log :refer [log!]])
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

(defn renew-leases-once!
  "List leases in CLAIMS_NS; renew those whose holder pod is still active."
  [{:keys [k8s-client claims-namespace]}]
  (assert (string? claims-namespace))
  (try
    (let [leases (some-> (k8s/list-leases k8s-client claims-namespace)
                         :items)]
      (doseq [lease leases]
        (let [ln     (get-in lease [:metadata :name])
              pod-ns (or (get-in lease [:metadata :labels :fudo.org/pod.namespace])
                         (get-in lease [:metadata :labels "fudo.org/pod.namespace"]))
              uid    (get-in lease [:spec :holderIdentity])]
          (cond
            (or (nil? uid) (empty? uid))
            (log! :debug (format "lease %s/%s has no holderIdentity; skipping"
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
                (log/error! (format "error processing lease %s/%s: %s"
                                    claims-namespace ln (.getMessage e)))
                (log! :debug (with-out-str (print-stack-trace e)))))))))
    (catch Exception e
      (log/error! (str "renew pass failed: " (.getMessage e)))
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
