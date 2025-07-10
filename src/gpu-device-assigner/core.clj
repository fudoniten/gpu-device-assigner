(ns gpu-device-assigner.core
  (:require [clojure.string :as str]
            [clojure.stacktrace :refer [print-stack-trace]]
            [clojure.set :refer [subset?]]
            [reitit.ring :as ring]
            [cheshire.core :as json]
            [ring.util.response :as response]
            [kubernetes-api.core :as k8s])
  (:import java.util.Base64
           java.time.Instant))

(defn get-node
  [k8s-client node-name]
  (k8s/invoke k8s-client {:kind    :Node
                          :action  :get
                          :request {:name node-name}}))

(defn get-node-labels
  [k8s-client node-name]
  (-> (get-node k8s-client node-name)
      :metadata
      :labels))

(defn get-node-annotations
  [k8s-client node-name]
  (-> (get-node k8s-client node-name)
      :metadata
      :annotations))

(defn base64-decode
  [b64-str]
  (let [decoder (Base64/getDecoder)]
    (.decode decoder b64-str)))

(defn map-vals
  [f m]
  (into {} (map (fn [[k v]] [k (f v)])) m))

(defn parse-json [str] (json/parse-string str true))

(defn get-device-labels
  [k8s-client node-name]
  (some->> (get-node-annotations k8s-client node-name)
           :fudo.org/gpu.device.labels
           (base64-decode)
           (parse-json)
           (map-vals set)))

(defn get-device-reservations
  [k8s-client node-name]
  (let [annotations (get-node-annotations k8s-client node-name)])
  (some->> annotations
           :fudo.org/gpu.device.reservations
           (parse-json)
           (assoc (select-keys annotations [:resourceVersion]) :reservations)))

(defn reserve-device
  [k8s-client {:keys [node-name namespace pod device-id]}]
  (let [{version :resourceVersion reservations :reservations} (get-device-reservations k8s-client node-name)]
    (if-let [reservation (get reservations device-id)]
      (if (!= (:pod reservation) pod)
        (do (log/warn (format "device %s is already reserved by pod %s: rejecting" device-id (:pod reservation)))
            nil)
        (do (log/info (format "device %s is already reserved by requesting pod %s: approving!" device-id (:pod reservation)))
            true))
      (let [updated-reservations (assoc reservations device-id {:pod pod :namespace namespace :timestamp (Instant/now)})
            reservation-patch    {:metadata {:annotations
                                             {:fudo.org/gpu.device.reservations
                                              (json/generate-string updated-reservations)}}
                                  :resourceVersion version}]
        (try (do (k8s/patch k8s-client [:Node node-name] patch)
                 device-id)
             (catch Exception e
               (let [status (ex-data e)]
                 (if (= 409 (:status status))
                   (log/error (format "conflict: reservation was modified before patch was applied. unable to reserve device %s for pod %s."
                                      device-id pod))
                   (log/error (format "error %s: unable to reserve device %s for pod %s: %s"
                                      (:status status) device-id pod (:message e))))
                 nil)))))))

(defn list-ns-pods
  [k8s-client namespace]
  (k8s/invoke k8s-client {:kind    :Pod
                          :action  :list
                          :request {:namespace namespace}}))

;; TODO: Implement!
;; (defn watch-pods [k8s-client namespace callback]
;;   (k8s-api/watch-namespaced-pod k8s-client namespace {:watch true :callback callback}))


(defn get-config-map
  [k8s-client namespace name]
  (k8s/invoke k8s-client {:kind    :ConfigMap
                          :action  :get
                          :request {:name      name
                                    :namespace namespace}}))

;; (defn watch-config [k8s-client namespace config-map callback]
;;   (k8s-api/watch-namespaced-config-map k8s-client
;;                                        namespace
;;                                        config-map
;;                                        {:watch true :callback callback}))

;; (defn patch-annotation [annotation-path value]
;;   [{:op    "add"
;;     :path  annotation-path
;;     :value value}])

(defn assign-gpu [{:keys [k8s-client node-name pod requested-labels]}]
  (let [node-dev-labels (get-device-annotation k8s-client node-name)
        reservations (get-device-reservations k8s-client node-name)
        candidates (filter (fn [[_ dev-labels]] (subset? requested-labels dev-labels))
                           node-dev-labels)]
    (when (empty? candidates)
      (log/error (format "error: no candidate devices found! pod %s was misassigned. requested labels: [%s]"
                         pod (str/join ", " requested-labels))
                 nil))
    ))

;; (defn try-json-parse [str]
;;   (try
;;     (json/parse-string str true)
;;     (catch Exception e
;;       (throw (ex-info "exception encountered when parsing json string"
;;                       {:body str :exception e})))))

;; (defn try-json-generate [json]
;;     (try
;;     (json/generate-string json true)
;;     (catch Exception e
;;       (throw (ex-info "exception encountered when generating json string"
;;                       {:body str :exception e})))))

;; (defn json-middleware
;;   "Middleware to encode/decode the JSON body of requests/responses."
;;   [handler]
;;   (fn [req]
;;     (if-let [body {:body req}]
;;       (-> body
;;           (slurp)
;;           (try-json-parse)
;;           (handler)
;;           (try-json-generate)
;;           (response/response)
;;           (assoc-in [:headers "Content-Type"] "application/json"))
;;       (throw (ex-info "missing request body!" {:req req})))))

;; (defn open-fail-middleware
;;   "Middleware to ensure some response is passed to the caller."
;;   [verbose]
;;   (fn [handler]
;;     (fn [{:keys [uid] :as req}]
;;       (try
;;         (handler req)
;;         (catch Exception e
;;           (log/error (format "error handling request: %s" (str e)))
;;           (when verbose
;;             (log/debug (print-stack-trace e)))
;;           (-> {:apiVersion "admission.k8s.io/v1"
;;                :kind "AdmissionReview"
;;                :response {:uid     uid
;;                           :allowed true}}
;;               (try-json-generate)))))))

;; (defn handle-mutation
;;   [verbose k8s-client]
;;   (fn [{:keys [kind] :as req}]
;;     (when-not (= kind "AdmissionReview")
;;       {:apiVersion "admission.k8s.io/v1"
;;        :kind       "AdmissionReview"
;;        :response   {:uid     (get-in req [:request :uid])
;;                     :allowed false
;;                     :status {:code    400
;;                              :message (format "Unexpected request kind: %s" kind)}}})
;;     ))

;; (defn app [verbose k8s-client]
;;   (ring/ring-handler
;;    (ring/router [["/mutate" {:post (handle-mutation verbose k8s-client)}]]
;;                 {:data {:middleware [json-middleware
;;                                      (open-fail-middleware verbose)]}})))

;; ;; old stuff

;; (defn assign-device [admission-review]
;;   (let [request        (get admission-review "request")
;;         uid            (get request "uid")
;;         namespace      (get-in request ["object" "metadata" "namespace"])
;;         name           (get-in request ["object" "metadata" "name"])
;;         node-name      (get-in request ["object" "spec" "nodeName"])
;;         annotations    (get-in request ["object" "metadata" "annotations"])
;;         needs          (some-> annotations (get "gpu.openai.com/needs") (str/split #","))
;;         selected-device "card0" ; TODO: dynamic selection based on node annotations and needs
;;         patch          (patch-annotation "/metadata/annotations/cdi.k8s.io~1nvidia.com~1gpu" selected-device)]
;;     {:apiVersion "admission.k8s.io/v1"
;;      :kind       "AdmissionReview"
;;      :response   {:uid       uid
;;                   :allowed   true
;;                   :patch     (-> patch json/generate-string (.getBytes "UTF-8") java.util.Base64/getEncoder encodeToString)
;;                   :patchType "JSONPatch"}}))

;; (defn webhook-handler [req]
;;   (let [body-string (slurp (:body req))
;;         admission-review (json/parse-string body-string)]
;;     (-> (assign-device admission-review)
;;         json/generate-string
;;         response
;;         (assoc :headers {"Content-Type" "application/json"}))))

;; (defn -main [& _]
;;   (jetty/run-jetty webhook-handler {:port 8443 :join? false}))
