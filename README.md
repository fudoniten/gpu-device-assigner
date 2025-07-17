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

2. Build the project:

   ```bash
   lein uberjar
   ```

## Configuration

The application requires several configuration parameters, which can be provided via command-line options:

- `--access-token`: Path to the token file for Kubernetes access.
- `--ca-certificate`: Path to the base64-encoded CA certificate for Kubernetes.
- `--kubernetes-url`: URL to the Kubernetes master.
- `--port`: Port on which to listen for incoming requests (default: 80).
- `--log-level`: Level at which to log output (default: `:warn`).

## Usage

To start the GPU Device Assigner, run the following command with the necessary options:

```bash
java -jar target/gpu-device-assigner-standalone.jar --access-token <path> --ca-certificate <path> --kubernetes-url <url> --port <port> --log-level <level>
```

## Testing

To run the tests, use the following command:

```bash
clojure -M:test
```

## Contributing

Contributions are welcome! Please fork the repository and submit a pull request.

## License

This project is licensed under the MIT License.
