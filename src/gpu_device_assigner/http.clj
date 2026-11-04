(ns gpu-device-assigner.http
  (:require [clojure.string :as str]
            [clojure.stacktrace :refer [print-stack-trace]]
            [reitit.ring :as ring]
            [ring.adapter.jetty :as jetty]
            [ring.util.response :as response]
            [hiccup2.core :as h]

            [gpu-device-assigner.core :as core]
            [gpu-device-assigner.util :as util]
            [taoensso.telemere :as log :refer [log!]]))

(declare admission-review-response)

(defn json-middleware
  "Middleware to decode the JSON request body, pass it to `handler` as `:body`,
   and encode the handler's result as a JSON response.

   Exceptions thrown by the handler are turned into a well-formed AdmissionReview
   denial (with the request's uid), so the API server always receives a valid
   response instead of a transport error."
  [handler]
  (fn [req]
    (let [body (:body req)]
      (when-not body
        (throw (ex-info "missing request body!" {})))
      (let [parsed (util/try-json-parse (slurp body))
            result (try
                     (handler (assoc req :body parsed))
                     (catch Exception e
                       (log! :error (format "error handling request: %s" (str e)))
                       (log! :error (util/capture-stack-trace e))
                       (admission-review-response :uid (get-in parsed [:request :uid])
                                                  :allowed? false
                                                  :status 500
                                                  :message (str "gpu-device-assigner error: "
                                                                (.getMessage e)))))]
        (if (and (map? result) (contains? result :status) (not (contains? result :kind)))
          (-> result
              (update :body util/try-json-generate)
              (update :headers #(assoc (or % {}) "Content-Type" "application/json")))
          (-> result
              (util/try-json-generate)
              (response/response)
              (assoc-in [:headers "Content-Type"] "application/json")))))))

(defn admission-review-response
  "Create a response for an AdmissionReview request."
  [& {:keys [uid allowed?
             status message
             patch]}]
  (assert (or status patch) "one of :status or :patch must be specified")
  {:apiVersion "admission.k8s.io/v1"
   :kind       "AdmissionReview"
   :response   (if status
                 {:uid       uid
                  :allowed   allowed?
                  :status    {:code    status
                              :message message}}
                 {:uid       uid
                  :allowed   allowed?
                  :patchType "JSONPatch"
                  :patch     patch})})

(defn error-middleware
  "Last-resort middleware: turn any exception that escaped the route (e.g. an
   unparseable body) into a plain HTTP 500. Admission denials for handler
   errors are produced by `json-middleware`, which knows the request uid.

   Note this fails *closed*: with `failurePolicy: Fail`, an error here means
   the pod is not created."
  [_]
  (fn [handler]
    (fn [req]
      (try
        (handler req)
        (catch Exception e
          (log! :error (format "error handling request: %s" (str e)))
          (log! :error (with-out-str (print-stack-trace e)))
          {:status  500
           :headers {"Content-Type" "text/plain"}
           :body    (str "internal error: " (.getMessage e))})))))

(defn device-assignment-patch
  "Generate JSONPatch that adds CDI assignment + node pin + breadcrumbs.
   `devices` is a seq of {:device-id uuid :node node}; all must share the same node.
   `has-annotations?` says whether the pod already has a metadata.annotations
   map: JSONPatch `add` on an existing member *replaces* it, so the empty map
   is only added when there is none."
  [has-annotations? devices reservation-id]
  (let [node      (-> devices first :node)
        uuids     (mapv #(-> % :device-id name) devices)
        cdi-value (str/join "," (map #(format "nvidia.com/gpu=UUID=%s" %) uuids))
        patch
        [(when-not has-annotations?
           {:op "add" :path "/metadata/annotations" :value {}})
         ;; Breadcrumbs for ops / GC tools (comma-separated for multi-GPU)
         {:op "add" :path "/metadata/annotations/fudo.org~1gpu.uuid"
          :value (str/join "," uuids)}
         {:op "add" :path "/metadata/annotations/fudo.org~1gpu.node" :value node}
         {:op "add" :path "/metadata/annotations/fudo.org~1gpu.reservation-id" :value reservation-id}
         ;; CDI assignment — comma-separated list for multi-GPU
         {:op "add" :path "/metadata/annotations/cdi.k8s.io~1gpu-assignment" :value cdi-value}
         ;; Hard bind to the node that actually has these UUIDs
         {:op "add" :path "/spec/nodeName" :value node}]
        patch (vec (remove nil? patch))]
    (log/trace! (str "\n##########\n#  PATCH\n##########\n\n" (util/pprint-string patch)))
    (-> patch
        (util/try-json-generate)
        (util/base64-encode))))

(defn log-requests-middleware
  [_]
  (fn [handler]
    (fn [req]
      (log/trace! (str "\n\n##########\n# REQUEST\n##########\n\n" (util/pprint-string req)))
      (let [res (handler req)]
        (log/trace! (str "\n\n##########\n# RESPONSE\n##########\n\n" (util/pprint-string res)))
        res))))

(defn- container-gpu-count
  "nvidia.com/gpu requested by one container (requests, falling back to limits)."
  [container]
  (let [qty (fn [m] (some (fn [[k v]] (when (= (util/full-name k) "nvidia.com/gpu") v)) m))
        v   (or (qty (get-in container [:resources :requests]))
                (qty (get-in container [:resources :limits])))]
    (cond
      (string? v)  (try (Long/parseLong v) (catch NumberFormatException _ 0))
      (integer? v) (long v)
      :else        0)))

(defn nvidia-gpu-count
  "Effective nvidia.com/gpu request of a pod, as Kubernetes computes it:
   max(largest init container, sum of regular containers + restartable
   (sidecar) init containers)."
  [pod-spec]
  (let [inits     (get pod-spec :initContainers [])
        sidecar?  #(= "Always" (:restartPolicy %))
        sidecars  (filter sidecar? inits)
        plain     (remove sidecar? inits)
        app-total (reduce + (map container-gpu-count (concat (get pod-spec :containers []) sidecars)))
        init-max  (reduce max 0 (map container-gpu-count plain))]
    (max app-total init-max)))

(defn- nvidia-gpu-requested?
  "Return true if the pod requests any nvidia.com/gpu."
  [pod-spec]
  (pos? (nvidia-gpu-count pod-spec)))

(defn- label-enabled?
  "A requirement label is enabled unless its value is explicitly false-ish."
  [v]
  (and (some? v)
       (not (contains? #{"false" "0" "no" ""} (str/lower-case (str v))))))

(defn handle-mutation
  "Handle an AdmissionReview request for mutating a pod's annotations."
  [ctx]
  (fn [{:keys [kind] :as req}]
    (if (not= kind "AdmissionReview")
      {:apiVersion "admission.k8s.io/v1"
       :kind       "AdmissionReview"
       :response   {:uid     (get-in req [:request :uid])
                    :allowed false
                    :status  {:code    400
                              :message (format "Unexpected request kind: %s" kind)}}}
      (do
        (log! :debug (format "Received AdmissionReview request: %s" (util/pprint-string req)))
        (let [dry-run?          (true? (get-in req [:request :dryRun]))
              fudo-label?       (fn [[k _]] (= "fudo.org" (namespace (keyword (util/full-name k)))))
              enabled?          (fn [[_ v]] (label-enabled? v))
              gpu-label?        (fn [[k _]] (= "gpu" (first (str/split (name k) #"\."))))
              remove-assign     (fn [[k _]] (not= k :fudo.org/gpu.assign))
              uid               (get-in req [:request :uid])
              reservation-id    (core/get-claim-id req)
              pod               (or (get-in req [:request :object :metadata :name])
                                    (get-in req [:request :object :metadata :generateName]))
              namespace         (get-in req [:request :object :metadata :namespace])
              all-labels        (get-in req [:request :object :metadata :labels])
              requested-labels  (->> all-labels
                                     (filter (every-pred fudo-label? enabled? gpu-label? remove-assign))
                                     (keys)
                                     (set))
              has-assign-label? (label-enabled? (core/map-value all-labels :fudo.org/gpu.assign))
              has-annotations?  (map? (get-in req [:request :object :metadata :annotations]))
              pod-spec          (get-in req [:request :object :spec])
              gpu-count         (nvidia-gpu-count pod-spec)]
          (cond
            dry-run?
            (do (log! :info "dry-run AdmissionReview; skipping Lease allocation")
                (admission-review-response :uid uid :allowed? true
                                           :status 200
                                           :message "dry-run: no mutation"))

            ;; Reject only if the pod requests nvidia.com/gpu with no fudo.org/gpu.*
            ;; labels at all — not even fudo.org/gpu.assign. Pods that have opted in
            ;; via fudo.org/gpu.assign are allowed through to the lease system even
            ;; when they carry no additional type/model labels.
            (and (nvidia-gpu-requested? pod-spec)
                 (empty? requested-labels)
                 (not has-assign-label?))
            (do (log! :warn (format "rejecting pod %s/%s: requests nvidia.com/gpu without any fudo.org/gpu.* labels"
                                    namespace pod))
                (admission-review-response :uid uid :allowed? false
                                           :status 403
                                           :message (format "pod %s/%s requests nvidia.com/gpu resources but specifies no fudo.org/gpu.* labels; add the appropriate labels to declare GPU requirements"
                                                            namespace pod)))

            ;; Opted in (or carries GPU labels) but requests no GPU: there is
            ;; nothing for the device plugin to hand out, so leasing a card
            ;; would only strand it. Admit unmodified.
            (zero? gpu-count)
            (do (log! :info (format "pod %s/%s requests no nvidia.com/gpu; admitting without a lease"
                                    namespace pod))
                (admission-review-response :uid uid :allowed? true
                                           :status 200
                                           :message "no nvidia.com/gpu requested; no assignment"))

            :else
            (do (log! :info (format "processing pod %s/%s, requesting %d GPU(s) with labels [%s]"
                                    namespace pod gpu-count (str/join "," (map util/full-name requested-labels))))
                (if-let [assigned-device (core/assign-device ctx {:pod              pod
                                                                  :reservation-id   reservation-id
                                                                  :namespace        namespace
                                                                  :requested-labels requested-labels
                                                                  :gpu-count        gpu-count})]
                  (let [{:keys [devices reservation-id]} assigned-device]
                    (log/trace! :admission-review/accept
                                (admission-review-response :uid uid :allowed? true
                                                           :patch (device-assignment-patch has-annotations? devices reservation-id))))
                  (log/trace! :admission-review/reject
                              (admission-review-response :uid uid :status 429 :allowed? false
                                                         :message (format "no %d-GPU set with requested labels free for pod %s/%s"
                                                                          gpu-count namespace pod)))))))))))

(defn handle-device-inventory
  "Return a snapshot of devices, their labels, and any current assignments."
  [ctx]
  (fn [_]
    (-> (core/device-inventory ctx)
        (util/try-json-generate)
        (response/response)
        (assoc-in [:headers "Content-Type"] "application/json"))))

(defn api-app [ctx]
  (ring/ring-handler
   (ring/router [["/mutate"  {:post (let [h (handle-mutation ctx)] (fn [req] (h (:body req))))
                              :middleware [json-middleware]}]
                 ["/devices" {:get (handle-device-inventory ctx)}]
                 ["/healthz" {:get (fn [_] {:status 200
                                            :headers {"Content-Type" "text/plain"}
                                            :body "ok"})}]]
                {:data {:middleware [(log-requests-middleware ctx)
                                     (error-middleware ctx)]}})
   (constantly {:status 404 :body "not found"})))

(defn start-api-server
  "Start the API web server with the given context and port."
  [ctx port]
  (jetty/run-jetty (api-app ctx)
                   {:port port
                    :join? false
                    :ssl? false
                    :host "0.0.0.0"}))

(defn- device->row [[device {:keys [node labels assignment] :as row}]]
  (let [pod-label (when assignment
                    (let [{:keys [namespace uid name]} assignment]
                      (str namespace "/" (or name uid))))
        exists?   (:exists? assignment)
        state     (:state assignment)
        status    (cond
                    (nil? assignment) "available"
                    (= state core/proposed-reservation) "pending"
                    (true? exists?)   "assigned"
                    (false? exists?)  "expired"
                    :else "assigned")]
      (try
        [:tr
         [:th {:scope "row"} (name device)]
         [:td (if (seq labels)
                (for [label labels]
                  [:div (name label)])
                "-")]
         [:td node]
         [:td (or pod-label "-")]
         [:td status]]
        (catch Throwable e
          (log! :error (format "failed to generate row for device %s: %s"
                               device (util/pprint-string row)))
        (log! :error (util/capture-stack-trace e))
        nil))))

(defn- render-device-table [inventory]
  (let [inventory (log/trace! :device/inventory inventory)
        rows      (if (seq inventory)
                    (map device->row inventory)
                    [[:tr [:td {:colspan 5} "No devices discovered."]]])]
    [:table
     [:thead
      [:tr
       [:th "Device"]
       [:th "Labels"]
       [:th "Node"]
       [:th "Assignment"]
       [:th "Status"]]]
     (into [:tbody] rows)]))

(defn- device-status-page [ctx]
  (let [inventory (core/device-inventory ctx)
        table     (render-device-table inventory)
        body      (str
                   (h/html
                       [:html
                        [:head
                         [:title "GPU Device Assignment"]
                         [:style (str "body{font-family:Arial,Helvetica,sans-serif;margin:2rem;}"
                                      "table{border-collapse:collapse;width:100%;}"
                                      "th,td{border:1px solid #ddd;padding:8px;text-align:left;}"
                                      "th{background:#f5f5f5;}")]]
                        [:body
                         [:h1 "GPU Device Assigner"]
                         [:p "Current device assignments. API JSON available at "
                          [:code "/devices"]
                          " on the API port."]
                         table]]))]
    (-> (response/response body)
        (assoc-in [:headers "Content-Type"] "text/html;charset=utf-8"))))

(defn ui-app [ctx]
  (ring/ring-handler
   (ring/router [["/" {:get (fn [_] (device-status-page ctx))}]
                 ["/devices" {:get (handle-device-inventory ctx)}]])
   (constantly {:status 404 :body "not found"})))

(defn start-ui-server
  "Start the UI web server with the given context and port."
  [ctx port]
  (jetty/run-jetty (ui-app ctx)
                   {:port port
                    :join? false
                    :ssl? false
                    :host "0.0.0.0"}))
