(ns gpu-device-assigner.k8s-client
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.test.alpha :as stest]
            [clojure.string :as str]

            [gpu-device-assigner.logging :as log]

            [kubernetes-api.core :as k8s])
  (:import clojure.lang.ExceptionInfo
           java.net.URL
           java.util.Base64))

(defprotocol IK8SBaseClient
  (invoke [client req]))

(defrecord K8SBaseClient [client]
  IK8SBaseClient
  (invoke [_ req]
    (println (format "Invoking Kubernetes API with request: %s" req))
    (try
      (let [response (k8s/invoke client req)]
        (println (format "Received response: %s" response))
        response)
      (catch Exception e
        (println (format "Error during Kubernetes API invocation: %s" (.getMessage e)))
        (throw e)))))

(defprotocol IK8SClient
  "Protocol defining Kubernetes client operations."
  (get-node    [self node-name])
  (get-nodes   [self])
  (patch-node  [self node-name patch])

  (get-pod     [self pod-name namespace])
  (get-pods    [self])
  (pod-exists? [self pod-name namespace]))

(defrecord K8SClient
    [client]

    IK8SClient
    (get-node [_ node-name]
      (invoke client
              {:kind    :Node
               :action  :get
               :request {:name node-name}}))

    (get-nodes [_]
      (-> client
          (invoke {:kind    :Node
                   :action  :list})
          :items))

    (patch-node [_ node-name patch]
      (invoke client
              {:kind    :Node
               :action  :patch/json
               :request {:name      node-name
                         :body      patch}}))

    (get-pod [_ pod-name namespace]
      (invoke client
              {:kind    :Pod
               :action  :get
               :request {:name      pod-name
                         :namespace namespace}}))

    (get-pods [_]
      (invoke client
              {:kind    :Pod
               :action  :list
               :request {:raw-path "/api/v1/pods"}}))

    (pod-exists? [self pod-name namespace]
      (try (boolean (get-pod self pod-name namespace))
           (catch ExceptionInfo e
             (if (= (:type (ex-data e)) :not-found)
               false
               (throw e))))))

(defn base64-string?
  "Check if a string is a valid Base64 encoded string."
  [o]
  (let [decoder (Base64/getDecoder)]
    (try
      (.decode decoder o)
      true
      (catch Exception e
        (println e)
        nil))))

(defn k8s-client?
  "Check if an object satisfies the K8SClient protocol."
  [o]
  (satisfies? K8SClient o))

(defn jwt-string?
  "Check if a string is a valid JWT token."
  [o]
  (let [jwt-pattern #"^[A-Za-z0-9_-]+\.([A-Za-z0-9_-]+)\.([A-Za-z0-9_-]+)$"]
    (boolean (and (string? o)
                  (re-matches jwt-pattern o)))))

(defn kube-url?
  "Check if a string is a valid Kubernetes URL."
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
  "Load a certificate from a file and trim whitespace."
  [cert-file]
  (-> cert-file (slurp) (str/trim)))

(defn load-access-token
  [token-file]
  (-> token-file (slurp) (str/trim)))

(s/fdef create
  :args (s/keys* :req-un [::url ::token ::certificate-authority-data :log/logger])
  :ret  ::client)
(defn create
  "Create a new Kubernetes client with the given configuration."
  [& {:keys [logger url] :as req}]
  (try
    (->K8SClient (->K8SBaseClient (k8s/client url (select-keys req [:token :certificate-authority-data]))))
    (catch Exception e
      (log/fatal logger (str "failed to create k8s-client: " (.getMessage e)))
      (throw (ex-info "failed to create k8s-client" {:error e})))))

(stest/instrument 'create)
