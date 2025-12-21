(ns gpu-device-assigner.util
  (:require [clojure.pprint :refer [pprint]]
            [clojure.string :as str]
            [cheshire.core :as json])
  (:import java.util.Base64))

(defn pprint-string [o]
  (with-out-str (pprint o)))

(defn try-json-parse [s]
  (try
    (json/parse-string s true)
    (catch Exception e
      (throw (ex-info "exception encountered when parsing json string"
                      {:body s :exception e})))))

(defn try-json-generate [json-val]
  (try
    (json/generate-string json-val)
    (catch Exception e
      (throw (ex-info "exception encountered when generating json string"
                      {:body json-val :exception e})))))

(defn base64-encode
  "Encode a string to Base64."
  [s]
  (let [encoder (Base64/getEncoder)]
    (.encodeToString encoder (.getBytes s "UTF-8"))))

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

(defn sanitize-for-dns ^String [^String s]
  (-> s
      (str/lower-case)
      (str/replace #"[^a-z0-9.-]" "-")
      (str/replace #"^[^a-z0-9]+" "")
      (str/replace #"[^a-z0-9]+$" "")))
