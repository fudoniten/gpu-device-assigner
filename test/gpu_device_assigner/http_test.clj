(ns gpu-device-assigner.http-test
  (:require [clojure.test :refer [deftest is testing]]
            [gpu-device-assigner.http :as http]
            [gpu-device-assigner.util :as util]
            [gpu-device-assigner.lease-renewer :as renewer])
  (:import java.io.ByteArrayInputStream))

(defn- json-body [m]
  (-> m util/try-json-generate (.getBytes "UTF-8") (ByteArrayInputStream.)))

(deftest finalize-endpoint
  (let [ctx {:claims-namespace "gpu-claims"}]
    (testing "missing fields return 400"
      (let [handler (http/json-middleware (http/handle-finalize-reservation ctx))
            {:keys [status body]} (handler {:body (json-body {:namespace "ns"})})]
        (is (= 400 status))
        (is (= {:error "missing required fields: name, uid, reservation-id, gpu-uuid"}
               (util/parse-json body)))))

    (testing "valid payload finalizes reservation"
      (let [finalized (atom nil)
            handler (http/json-middleware (http/handle-finalize-reservation ctx))]
        (with-redefs [renewer/finalize-reservation!
                      (fn [passed-ctx reservation]
                        (reset! finalized [passed-ctx reservation]))]
          (let [payload {:namespace "ns"
                         :name "pod"
                         :uid "uid-123"
                         :reservation-id "res-1"
                         :gpu-uuid "GPU-abc"}
                {:keys [status body]} (handler {:body (json-body payload)})]
            (is (= 200 status))
            (is (= {:status "ok"} (util/parse-json body)))
            (is (= ctx (first @finalized)))
            (is (= {:reservation-id "res-1"
                    :device-id "GPU-abc"
                    :namespace "ns"
                    :uid "uid-123"
                    :name "pod"}
                   (second @finalized)))))))))
