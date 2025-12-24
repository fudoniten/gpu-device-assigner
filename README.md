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


## Testing

To run the tests, use the following command:

```bash
clojure -M:test
```
