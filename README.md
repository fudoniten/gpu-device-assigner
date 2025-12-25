# GPU Device Assigner

The GPU Device Assigner is a Kubernetes admission controller designed to intercept pod assignments involving GPUs. It patches the request to map the pod to a GPU that matches the pod's requirements. This is particularly useful in heterogeneous environments where different servers contain various types of GPUs.

## Features

- Intercepts Kubernetes pod assignments.
- Matches pods to GPUs based on specified requirements.
- Supports environments with diverse GPU configurations.

## Prerequisites

- Kubernetes cluster with admission controller support.
- Clojure and Leiningen installed on your local machine for development.

## Installation

1. Clone the repository:

   ```bash
   git clone <repository-url>
   cd gpu-device-assigner
   ```

2. Deploy the container images:

   ```bash
   nix run .#deployContainers
   ```

## Configuration

The application requires several configuration parameters, which can be provided via command-line options:

- `--access-token`: Path to the token file for Kubernetes access.
- `--ca-certificate`: Path to the base64-encoded CA certificate for Kubernetes.
- `--kubernetes-url`: URL to the Kubernetes master.
- `--port`: Port on which to listen for incoming AdmissionReview API requests (default: 443).
- `--ui-port`: Port used by the human-readable UI that shows device assignments (default: 8080).
- `--log-level`: Level at which to log output (default: `warn`).
The service starts both an API listener (for AdmissionReview and JSON `/devices`) and a UI listener that presents the current device inventory in HTML.

### Finalize callback contract

Reservation finalization is driven by a POST to the `/finalize` API route. The request must be JSON with the following shape:

```json
{
  "namespace": "pod-namespace",
  "name": "pod-name",
  "uid": "pod-uid",
  "reservation-id": "reservation identifier returned during mutation",
  "gpu-uuid": "GPU UUID assigned to the pod"
}
```

The callback is expected after a pod is created with the annotations written by the mutating webhook: `fudo.org/gpu.uuid`, `fudo.org/gpu.reservation-id`, and `fudo.org/gpu.node`. A successful finalize request returns `200` with `{ "status": "ok" }`; missing fields return `400` with an error message.

Example curl invocation:

```bash
curl -X POST \
  -H 'Content-Type: application/json' \
  -d '{
    "namespace": "default",
    "name": "example",
    "uid": "12d34...",
    "reservation-id": "resv-abc",
    "gpu-uuid": "GPU-d3adbeef"
  }' \
  http://localhost:8080/finalize
```


## Testing

To run the tests, use the following command:

```bash
clojure -M:test
```
