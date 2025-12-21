(ns gpu-device-assigner.k8s-client
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.test.alpha :as stest]
            [clojure.string :as str]
            [clojure.stacktrace :refer [print-stack-trace]]

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
    (try
      (assoc (k8s/invoke client req)
             :status 200
             :error  false)
      (catch Exception e
        (let [data (ex-data e)]
          (if-let [status (get-in data [:response :status])]
            {:type    (:type data)
             :status  status
             :body    (get-in data [:response :body])
             :headers (get-in data [:response :headers])
             :error   true}
            (throw (ex-info (format "error during k8s api invocation: %s"
                                    (.getMessage e))
                            {:stack-trace (with-out-str (print-stack-trace e))
                             :error e}))))))))

(defprotocol IK8SClient
  "Protocol defining Kubernetes client operations."
  (get-node           [self node-name])
  (get-nodes          [self])
  (patch-node         [self node-name patch])

  (create-lease       [self namespace name lease-body])
  (get-lease          [self namespace name])
  (patch-lease        [self namespace name merge-path])
  (list-leases        [self namespace])

  (get-pod-by-uid     [self namespace uid])
  (get-pod            [self pod-name namespace])
  (get-pods           [self])
  (pod-exists?        [self pod-name namespace])
  (pod-uid-exists?    [self uid namespace])
  (get-namespace-pods [self namespace]))

;; Internal helpers for Lease API paths
(defn- lease-collection-path
  "Build the Lease collection API path for a namespace."
  ^String [^String namespace]
  (format "/apis/coordination.k8s.io/v1/namespaces/%s/leases" namespace))

(defn- lease-item-path
  "Build the Lease item API path for a namespace/name pair."
  ^String [^String namespace ^String name]
  (str (lease-collection-path namespace) "/" name))

(defrecord K8SClient
    [client logger]

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
               :action  :patch/strategic
               :request {:name node-name
                         :body patch}}))

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

    (get-namespace-pods [_ namespace]
      (invoke client
              {:kind    :Pod
               :action  :list
               :request {:namespace namespace}}))

    (pod-exists? [self pod-name namespace]
      (try (boolean (get-pod self pod-name namespace))
           (catch ExceptionInfo e
             (if (= (:type (ex-data e)) :not-found)
               false
               (throw e)))))

    (pod-uid-exists? [self uid namespace]
      (some (fn [pod]
              (when (= (get-in pod [:metadata :uid]) uid)
                (boolean pod)))
            (get-namespace-pods self namespace)))

    (create-lease [_ namespace name lease-body]
      ;; POST to the collection; ensure metadata.name/namespace are set in body
      (let [body (-> lease-body
                     (assoc-in [:metadata :name] name))]
        (invoke client
                {:kind    :Lease
                 :action  :create
                 :request {:namespace namespace
                           :body      body}})))

    (get-lease [_ namespace name]
      (invoke client
              {:kind    :Lease
               :action  :get
               :request {:name      name
                         :namespace namespace}}))

    (patch-lease [_ namespace name merge-patch]
      ;; Use JSON Merge Patch for Leases (spec.renewTime / holderIdentity, etc.)
      (invoke client
              {:kind    :Lease
               :action  :patch/json-merge
               :request {:name      name
                         :namespace namespace
                         :body      merge-patch}}))

    (list-leases [_ namespace]
      (invoke client
              {:kind    :Lease
               :action  :list
               :request {:namespace namespace}}))

    (get-pod-by-uid [self namespace uid]
      ;; fieldSelector works cluster-wide when you hit /api/v1/pods at the root
      (some (fn [pod]
              (when (= (get-in pod [:metadata :uid]) uid)
                pod))
            (get-namespace-pods self namespace))))

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
  "Load an access token from a file and trim whitespace."
  [token-file]
  (-> token-file (slurp) (str/trim)))

(s/fdef create
  :args (s/keys* :req-un [::url ::token ::certificate-authority-data :log/logger])
  :ret  ::client)
(defn create
  "Create a new Kubernetes client with the given configuration."
  [& {:keys [logger url] :as req}]
  (try
    (->K8SClient (->K8SBaseClient (k8s/client url (select-keys req [:token :certificate-authority-data]))) logger)
    (catch Exception e
      (log/fatal logger (str "failed to create k8s-client: " (.getMessage e)))
      (throw (ex-info "failed to create k8s-client" {:error e})))))

(stest/instrument 'create)
