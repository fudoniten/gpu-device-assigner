(ns gpu-device-assigner.time
  (:import [java.time Instant ZoneOffset]
           [java.time.temporal ChronoUnit]
           [java.time.format DateTimeFormatter]))

(def ^DateTimeFormatter rfc3339-micro
  ;; 6 fractional digits + offset ("Z" for UTC when offset is zero)
  (-> (DateTimeFormatter/ofPattern "yyyy-MM-dd'T'HH:mm:ss.SSSSSSX")
      (.withZone ZoneOffset/UTC)))

(defn now-rfc3339-micro ^String []
  (let [inst (.truncatedTo (Instant/now) ChronoUnit/MICROS)]
    (.format rfc3339-micro inst)))
