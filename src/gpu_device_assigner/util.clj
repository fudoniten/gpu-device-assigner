(ns gpu-device-assigner.util
  (:require [clojure.pprint :refer [pprint]]
            [clojure.string :as str]
            [cheshire.core :as json])
  (:import java.util.Base64))

(defn pprint-string
  "Pretty-print an object to a string for readable logging."
  [o]
  (with-out-str (pprint o)))

(defn pthru-label
  "Log a labeled value and return it unchanged."
  [lbl o]
  (println (str "###### " lbl))
  (pprint o)
  o)

(defn try-json-parse
  "Parse a JSON string, throwing an informative exception on failure."
  [str]
  (try
    (json/parse-string str true)
    (catch Exception e
      (throw (ex-info "exception encountered when parsing json string"
                      {:body str :exception e})))))

(defn try-json-generate
  "Generate a JSON string, throwing an informative exception on failure."
  [json]
  (try
    (json/generate-string json)
    (catch Exception e
      (throw (ex-info "exception encountered when generating json string"
                      {:body json :exception e})))))

(defn sanitize-for-dns
  "Convert a string into a DNS-safe, lowercase token."
  ^String [^String s]
  (-> s
      (str/lower-case)
      (str/replace #"[^a-z0-9.-]" "-")
      (str/replace #"^[^a-z0-9]+" "")
      (str/replace #"[^a-z0-9]+$" "")))

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
