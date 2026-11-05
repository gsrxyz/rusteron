# =============================================================================
# Dev Utilities
# =============================================================================

# Show all available tasks
list:
  cargo update
  just --list

# Automatically format and fix Clippy warnings
fix:
  cd rusteron-dummy-example && cargo fmt --all && cd -
  cd rusteron-dummy-example && cargo clippy --allow-dirty --allow-staged --fix && cd -

# =============================================================================
# Docker / Podman Builds
# =============================================================================

# Build containers using Docker
docker-build:
  docker build -t aeron-media-driver aeron-media-driver
  docker build -t rusteron-dummy-example rusteron-dummy-example

# Build containers using Podman
podman-build:
  podman build -t rusteron-dummy-example -f Dockerfile rusteron-dummy-example
  podman build -t aeron-media-driver -f Dockerfile aeron-media-driver

# =============================================================================
# Podman / K8s Deployment
# =============================================================================

# Deploy the dummy-example pod using Podman
podman-deploy:
  just podman-stop || true
  podman play kube pod.yml

# Stop and remove the dummy-example Podman pod
podman-stop:
  podman pod stop dummy-example
  podman pod rm dummy-example --force

# Deploy the dummy-example pod to Kubernetes
k8s-deploy:
  kubectl apply -f pod.yml

# Delete the dummy-example pod from Kubernetes
k8s-clean:
  kubectl delete pod dummy-example

# =============================================================================
# Runtime
# =============================================================================

# Run the pinned Aeron Archive and Media Driver with Java.
run-aeron-archive-driver:
  just --justfile ../.justfile run-aeron-archive-driver
