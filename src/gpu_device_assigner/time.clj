(ns gpu-device-assigner.time
  (:import [java.time Instant ZoneOffset]
           [java.time.temporal ChronoUnit]
           [java.time.format DateTimeFormatter]))

(def ^DateTimeFormatter rfc3339-micro
  ;; 6 fractional digits + offset ("Z" for UTC when offset is zero)
  (-> (DateTimeFormatter/ofPattern "yyyy-MM-dd'T'HH:mm:ss.SSSSSSX")
      (.withZone ZoneOffset/UTC)))

(defn now-rfc3339-micro-at
  "Format `inst` as RFC3339 with microsecond precision."
  ^String [^Instant inst]
  (.format rfc3339-micro (.truncatedTo inst ChronoUnit/MICROS)))

(defn now-rfc3339-micro
  "Return the current time formatted as RFC3339 with microsecond precision."
  ^String []
  (now-rfc3339-micro-at (Instant/now)))
