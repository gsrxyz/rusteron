# Containerised Aeron example with Podman and Kubernetes

These examples run an Aeron Archive Media Driver, a ticker publisher and a reader
in containers. Use Docker or Podman locally, or deploy the supplied Kubernetes manifests.
The configuration is for development and testing.

---

## Sponsored by GSR

Rusteron is sponsored and maintained by [GSR](https://www.gsr.io), a global leader in algorithmic trading and market making in digital assets.

It runs in GSR's real-time trading stack and is developed under the GSR GitHub organisation. We welcome contributions, feedback and discussion. To integrate or contribute, open an issue or reach out directly.

---

<a id="key-features-demonstrated"></a>

## Overview

The setup includes:

1. The Aeron Archive Media Driver records streams and handles transport.
2. The ticker writer publishes simulated Binance ticker data for recording.
3. The ticker reader reads recorded and live messages and publishes periodic statistics.

The containers share the driver directory and archive storage through volumes.

---

## Quick start for local testing

### Prerequisites

* Podman or Docker, for building images and running containers.
* Kubernetes (optional), for orchestration. If you use Docker Desktop, enable Kubernetes.

### Podman (recommended for local testing)

Podman runs these examples without a separate daemon.

1. Build Podman images
   ```bash
   just podman-build
   # Builds all container images using Podman.
   ```

2. Deploy locally with Podman

   ```bash
   just podman-deploy
   # Launches the Aeron media driver, writer, and reader using pod definitions.
   ```

3. Stop and clean up

   ```bash
   just podman-stop
   # Stops and removes the example containers and shared volumes.
   ```

---

### Kubernetes deployment (alternative)

If you have a Kubernetes cluster available (e.g. via Docker Desktop or Minikube), you can use the provided manifests.

1. Build Docker images

   ```bash
   just docker-build
   # Builds all required images using Docker (for Kubernetes use).
   ```

2. Deploy to Kubernetes

   ```bash
   just k8s-deploy
   # Applies the Kubernetes manifests to your current cluster context.
   ```

3. Clean up

   ```bash
   just k8s-clean
   # Deletes deployed Kubernetes resources and persistent volumes.
   ```

---

## Shared directories

* Shared memory (`/dev/shm`) allows zero-copy Aeron message passing between containers.
* Shared volumes (`/data`) hold the persistent archive storage used by the writer and reader.
* `just` recipes build, deploy and clean up.
* A simulated publisher and reader interact through the Aeron archive APIs.

Production deployments need access controls, orchestration policies, monitoring and
failover logic beyond this example.

---

<a id="license--acknowledgments"></a>

## License and acknowledgments

Dual-licensed under MIT or Apache-2.0. See the root [README](https://github.com/gsrxyz/rusteron#readme).

Special thanks to:

* [@mimran1980](https://github.com/mimran1980), a core low-latency developer at GSR and the original creator of Rusteron. Your work made this possible.
* [@bspeice](https://github.com/bspeice) for the original [`libaeron-sys`](https://github.com/bspeice/libaeron-sys)
* The [Aeron](https://github.com/real-logic/aeron) community for the open protocol
