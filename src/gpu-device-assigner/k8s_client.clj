(ns gpu-device-assigner.k8s-client
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.test.alpha :as stest]
            [clojure.string :as str]

            [kubernetes-api.core :as k8s])
  (:import clojure.lang.ExceptionInfo
           java.net.URL
           java.util.Base64))

(defprotocol IK8SClient
  (get-node    [self node-name])
  (patch-node  [self node-name patch])

  (get-pod     [self pod-name namespace])
  (pod-exists? [self pod-name namespace]))

(defrecord K8SClient
    [client]

  IK8SClient
  (get-node [_ node-name]
    (k8s/invoke client
                {:kind    :Node
                 :action  :get
                 :request {:name node-name}}))

  (patch-node [_ node-name patch]
    (k8s/invoke client
                {:kind    :Node
                 :action  :patch/json
                 :request {:name      node-name
                           :body      patch}}))

  (get-pod [_ pod-name namespace]
    (k8s/invoke client
                {:kind    :Pod
                 :action  :get
                 :request {:name      pod-name
                           :namespace namespace}}))

  (pod-exists? [self pod-name namespace]
    (try (boolean (get-pod self pod-name namespace))
         (catch ExceptionInfo e
           (if (= (:type (ex-data e)) :not-found)
             false
             (throw e))))))

(defn base64-string?
  [o]
  (let [decoder (Base64/getDecoder)]
    (try
      (.decode decoder o)
      true
      (catch Exception e
        (println e)
        nil))))

(defn k8s-client?
  [o]
  (satisfies? K8SClient o))

(defn jwt-string?
  [o]
  (let [jwt-pattern #"^[A-Za-z0-9_-]+\.([A-Za-z0-9_-]+)\.([A-Za-z0-9_-]+)$"]
    (boolean (and (string? o)
                  (re-matches jwt-pattern o)))))

(defn kube-url?
  [o]
  (try
    (let [url (URL. o)]
      (#{"http" "https"} (.getScheme url)))
    (catch Exception _ false)))

(s/def ::url kube-url?)
(s/def ::token jwt-string?)
(s/def ::certificate-authority-data base64-string?)

(s/def ::client k8s-client?)

(defn load-certificate
  [cert-file]
  (-> cert-file (slurp) (str/trim)))

(s/fdef create
  :args (s/keys* :req-un [::url ::token ::certificate-authority-data])
  :ret  ::client)
(defn create [& {:keys [url] :as req}]
  (->K8SClient (k8s/client url (select-keys req [:token :certificate-authority-data]))))

(stest/instrument 'create)
