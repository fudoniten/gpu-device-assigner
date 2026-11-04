(ns gpu-device-assigner.k8s-client
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [clojure.stacktrace :refer [print-stack-trace]]

            [kubernetes-api.core :as k8s]
            [taoensso.telemere :as log :refer [log!]])
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

(defn ok?
  "True when `resp` is a successful API response.

  `K8SBaseClient` returns the response *body* (with `:status 200` added) on
  success, and `{:status <code> :error true ...}` on failure. Callers must use
  this rather than reading `:items`/`:body` directly, so that an API error is
  never mistaken for an empty result."
  [resp]
  (and (map? resp)
       (not (:error resp))
       (let [status (:status resp)]
         (or (nil? status) (<= 200 status 299)))))

(defn not-found?
  [resp]
  (= 404 (:status resp)))

(defprotocol IK8SClient
  "Protocol defining Kubernetes client operations."
  (get-node           [self node-name])
  (get-nodes          [self])
  (patch-node         [self node-name patch])

  (create-lease       [self namespace name lease-body])
  (get-lease          [self namespace name])
  (patch-lease        [self namespace name merge-path])
  (delete-lease       [self namespace name] [self namespace name resource-version]
    "Delete a Lease. With `resource-version`, the delete only succeeds if the
     Lease has not changed since it was read (409 otherwise).")
  (list-leases        [self namespace])

  (get-pod-by-uid     [self namespace uid])
  (get-pod            [self pod-name namespace])
  (get-pods           [self])
  (pod-exists?        [self pod-name namespace])
  (pod-uid-exists?    [self uid namespace]
    "True/false when the pod list could be read; throws on API errors.")
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
      (let [resp (try (get-pod self pod-name namespace)
                      (catch ExceptionInfo e
                        (if (= (:type (ex-data e)) :not-found)
                          {:status 404 :error true}
                          (throw e))))]
        (cond
          (ok? resp)        true
          (not-found? resp) false
          :else (throw (ex-info "error looking up pod" {:response resp})))))

    (pod-uid-exists? [self uid namespace]
      (let [resp (get-namespace-pods self namespace)]
        (when-not (ok? resp)
          (throw (ex-info "error listing pods" {:namespace namespace :response resp})))
        (boolean (some #(= uid (get-in % [:metadata :uid])) (:items resp)))))

    (create-lease [_ namespace name lease-body]
      ;; POST to the collection; ensure metadata.name/namespace are set in body
      (let [body (-> lease-body
                     (assoc-in [:metadata :name] name))]
        (log/trace! :lease/creation
                    (invoke client
                            {:kind    :Lease
                             :action  :create
                             :request {:namespace namespace
                                       :body      body}}))))

    (get-lease [_ namespace name]
      (invoke client
              {:kind    :Lease
               :action  :get
               :request {:name      name
                         :namespace namespace}}))

    (patch-lease [_ namespace name merge-patch]
      ;; Accept either a JSONMergePatch map or a JSONPatch string.
      (let [patch-req {:name      name
                       :namespace namespace
                       :body      merge-patch}
            action    (if (string? merge-patch)
                        :patch/json
                        :patch/json-merge)]
        (invoke client
                {:kind    :Lease
                 :action  action
                 :request patch-req})))

    (delete-lease [_ namespace name]
      (invoke client
              {:kind    :Lease
               :action  :delete
               :request {:name      name
                         :namespace namespace}}))

    (delete-lease [_ namespace name resource-version]
      (invoke client
              {:kind    :Lease
               :action  :delete
               :request {:name      name
                         :namespace namespace
                         :body      {:apiVersion    "v1"
                                     :kind          "DeleteOptions"
                                     :preconditions {:resourceVersion resource-version}}}}))

    (list-leases [_ namespace]
      (invoke client
              {:kind    :Lease
               :action  :list
               :request {:namespace namespace}}))

    (get-pod-by-uid [self namespace uid]
      ;; Returns nil both when the pod is absent and when the list fails; use
      ;; `get-namespace-pods` + `ok?` where that distinction matters.
      (some (fn [pod]
              (when (= (get-in pod [:metadata :uid]) uid)
                pod))
            (-> (get-namespace-pods self namespace) :items))))

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
  (satisfies? IK8SClient o))

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
  :args (s/keys* :req-un [::url ::token ::certificate-authority-data])
  :ret  ::client)
(defn create
  "Create a new Kubernetes client with the given configuration."
  [& {:keys [url] :as req}]
  (try
    (->K8SClient (->K8SBaseClient (k8s/client url (select-keys req [:token :certificate-authority-data]))))
    (catch Exception e
      (log/error! (str "failed to create k8s-client: " (.getMessage e)))
      (throw (ex-info "failed to create k8s-client" {:error e})))))
