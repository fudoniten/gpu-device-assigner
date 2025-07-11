(ns gpu-device-assigner.core
  (:require [clojure.string :as str]
            [clojure.stacktrace :refer [print-stack-trace]]
            [clojure.set :refer [subset?]]

            [gpu-device-assigner.logging :as log]
            [gpu-device-assigner.k8s-client :as k8s]

            [reitit.ring :as ring]
            [cheshire.core :as json]
            [ring.util.response :as response])
  (:import java.util.Base64
           java.time.Instant))

(defn get-node-annotations
  [{:keys [k8s-client]} node-name]
  (-> (k8s/get-node k8s-client node-name)
      :metadata
      :annotations))

(defn base64-encode
  [str]
  (let [encoder (Base64/getEncoder)]
    (.encodeToString encoder (.getBytes str "UTF-8"))))

(defn base64-decode
  [b64-str]
  (let [decoder (Base64/getDecoder)]
    (.decode decoder b64-str)))

(defn map-vals
  [f m]
  (into {} (map (fn [[k v]] [k (f v)])) m))

(defn parse-json [str] (json/parse-string str true))

(defn get-device-labels
  [ctx node-name]
  (some->> (get-node-annotations ctx node-name)
           :fudo.org/gpu.device.labels
           (base64-decode)
           (parse-json)
           (map-vals set)))

(defn get-device-reservations
  [ctx node-name]
  (let [annotations (get-node-annotations ctx node-name)]
    (some->> annotations
             :fudo.org/gpu.device.reservations
             (parse-json)
             (assoc (select-keys annotations [:resourceVersion]) :reservations))))

(defn node-patch
  [{:keys [k8s-client]} node-name patch]
  (k8s/patch-node k8s-client node-name patch))

(defn reserve-device
  [{:keys [logger] :as ctx} {:keys [node-name namespace pod device-id]}]
  (let [{version :resourceVersion reservations :reservations} (get-device-reservations ctx node-name)]
    (if-let [reservation (get reservations device-id)]
      (if (not= (:pod reservation) pod)
        (do (log/warn logger (format "device %s is already reserved by pod %s: rejecting" device-id (:pod reservation)))
            nil)
        (do (log/info logger (format "device %s is already reserved by requesting pod %s: approving!" device-id (:pod reservation)))
            true))
      (let [updated-reservations (assoc reservations device-id {:pod pod :namespace namespace :timestamp (Instant/now)})
            reservation-patch    {:metadata {:annotations
                                             {:fudo.org/gpu.device.reservations
                                              (json/generate-string updated-reservations)}}
                                  :resourceVersion version}]
        (try (node-patch ctx node-name reservation-patch)
             device-id
             (catch Exception e
               (let [status (ex-data e)]
                 (if (= 409 (:status status))
                   (log/error logger (format "conflict: reservation was modified before patch was applied. unable to reserve device %s for pod %s."
                                             device-id pod))
                   (log/error logger (format "error %s: unable to reserve device %s for pod %s: %s"
                                             (:status status) device-id pod (:message e))))
                 nil)))))))

;; (defn patch-annotation [annotation-path value]
;;   [{:op    "add"
;;     :path  annotation-path
;;     :value value}])

(defn device-reserved?
  "A device is reserved if it's assigned to a different pod. Otherwise, it's free."
  [reservations dev-id this-pod]
  (some-> reservations
          (get dev-id)
          :pod
          (not= this-pod)))

(defn assign-device [ctx {:keys [node-name pod namespace requested-labels]}]
  (let [node-dev-labels (get-device-labels ctx node-name)
        candidates (filter (fn [[_ dev-labels]] (subset? requested-labels dev-labels))
                           node-dev-labels)]
    (when (empty? candidates)
      (log/error (format "error: no candidate devices found! pod %s was misassigned. requested labels: [%s]"
                         pod (str/join ", " requested-labels))
                 nil))
    (let [reservations (get-device-reservations ctx node-name)
          available (filter (fn [dev-id] (not (device-reserved? reservations dev-id pod)))
                            (keys candidates))
          selected-id (rand-nth available)]
      (reserve-device ctx
                      {:node-name node-name
                       :namespace namespace
                       :pod       pod
                       :device-id selected-id}))))



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

(defn admission-review-response
  [& {:keys [uid allowed?
             status message
             patch]}]
  (assert (or status patch) "one of :status or :patch must be specified")
  {:apiVersion "admission.k8s.io/v1"
   :kind       "AdmissionReview"
   :response   (if status
                 {:uid     uid
                  :allowed allowed?
                  :status  {:code    status
                            :message message}}
                 {:uid       uid
                  :allowed   allowed?
                  :patchType "JSONPatch"
                  :patch     patch})})

(defn device-assignment-patch
  [device-id]
  (-> {:op    "add"
       :path  "/metadata/annotations/cdi.k8s.io~1nvidia.com~1gpu"
       :value device-id}
      (json/generate-string)
      (base64-encode)))

(defn handle-mutation
  [ctx]
  (fn [{:keys [kind] :as req}]
    (when-not (= kind "AdmissionReview")
      {:apiVersion "admission.k8s.io/v1"
       :kind       "AdmissionReview"
       :response   {:uid     (get-in req [:request :uid])
                    :allowed false
                    :status {:code    400
                             :message (format "Unexpected request kind: %s" kind)}}})
    (let [uid (get-in req [:request :uid])
          pod (get-in req [:request :object :metadata :name])
          ns  (get-in req [:request :object :metadata :namespace])
          annotations (get-in req [:request :object :metadata :annotations])
          labels (keys (filter (fn [[_ v]] (= v "true")) annotations))
          node-name (get-in req [:request :object :spec :nodeName])]
      (if-let [assigned-device (assign-device ctx {:node-name node-name
                                                   :pod       pod
                                                   :namespace namespace
                                                   :requested-labels labels})]
        (admission-review-response :uid uid :allowed? true
                                   :patch (device-assignment-patch assigned-device))
        (admission-review-response :uid uid :status 500 :allowed? false
                                   :message (format "failed to find unreserved device for pod %s on node %s."
                                                    pod node-name))))))

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


;; {
;;   "apiVersion": "admission.k8s.io/v1",
;;   "kind": "AdmissionReview",
;;   "request": {
;;     "uid": "123abc...",
;;     "object": {
;;       "metadata": {
;;         "name": "mypod",
;;         "namespace": "default",
;;         "annotations": {
;;           "gpu.openai.com/needs": "himem"
;;         }
;;       },
;;       "spec": {
;;         "nodeName": "gpu-node-1"
;;       }
;;     }
;;   }
;; }

;; {
;;   "apiVersion": "admission.k8s.io/v1",
;;   "kind": "AdmissionReview",
;;   "response": {
;;     "uid": "123abc...",
;;     "allowed": true,
;;     "patchType": "JSONPatch",
;;     "patch": "<base64-encoded JSON patch>"
;;   }
;; }
