# Containerised Aeron example with Podman and Kubernetes

This repository provides a minimal setup for containerising and orchestrating Aeron-based components with Docker, Podman and Kubernetes.
It is not production-ready, but it is a starting point for developing and testing Aeron-based workflows.

---

## Sponsored by GSR

Rusteron is sponsored and maintained by [GSR](https://www.gsr.io), a global leader in algorithmic trading and market making in digital assets.

It runs in GSR's real-time trading stack and is developed under the GSR GitHub organisation. We welcome contributions, feedback and discussion. To integrate or contribute, open an issue or reach out directly.

---

## Overview

The setup includes:

1. Aeron Media Driver: a containerised Aeron Archive Media Driver.
2. Ticker Writer: publishes simulated Binance ticker data and archives it.
3. Ticker Reader: reads from both the archive and the live channel and publishes periodic stats.

Together, these components form a minimal end-to-end Aeron pipeline using shared memory and volumes.

---

## Quick start for local testing

### Prerequisites

* Podman or Docker, for building images and running containers.
* Kubernetes (optional), for orchestration. If you use Docker Desktop, enable Kubernetes.

### Podman (recommended for local testing)

Podman is preferred because it is simple and needs no daemon.

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
   # Gracefully stops and removes all running containers and shared volumes.
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

## Key features demonstrated

* Shared memory (`/dev/shm`) allows zero-copy Aeron message passing between containers.
* Shared volumes (`/data`) hold the persistent archive storage used by the writer and reader.
* `just` recipes build, deploy and clean up.
* A simulated publisher and reader interact through the Aeron archive APIs.

> This example is for experimentation and learning. Production use needs additional security, orchestration policies, monitoring and failover logic.

---

## License & acknowledgments

Dual-licensed under MIT or Apache-2.0. See the root [README](https://github.com/gsrxyz/rusteron#readme).

Special thanks to:

* [@mimran1980](https://github.com/mimran1980), a core low-latency developer at GSR and the original creator of Rusteron. Your work made this possible.
* [@bspeice](https://github.com/bspeice) for the original [`libaeron-sys`](https://github.com/bspeice/libaeron-sys)
* The [Aeron](https://github.com/real-logic/aeron) community for the open protocol