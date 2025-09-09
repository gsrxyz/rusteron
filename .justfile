# =============================================================================
# Functions
# =============================================================================

set shell := ["bash", "-cu"]

is_mac := if os() == "macos" { 'true' } else { 'false' }

version := if `git rev-parse --git-dir 2>/dev/null; echo $?` == "0" {
    `git describe --tags --always --dirty 2>/dev/null || echo "dev"`
} else {
    `date -u '+%Y%m%d-%H%M%S'`
}
git_commit := `git rev-parse --short HEAD 2>/dev/null || echo "unknown"`
git_repository := `git config --get remote.origin.url 2>/dev/null || echo "unknown"`
git_branch := `git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown"`
build_time := `date -u '+%Y-%m-%d_%H:%M:%S'`
build_by := `whoami`

# =============================================================================
# Rust
# =============================================================================

# Print options
default:
    @just --list --unsorted

# List all available tasks
list:
    cargo install cargo-udeps
    cargo install cargo-edit
    cargo update
    just --list

# Check formatting, linting, and compile
check:
  cargo check --workspace
  cargo fmt --all -- --check
  cargo clippy --all-targets --all-features --all -- --deny warnings
  cargo clippy -- --deny warnings

# Automatically format code and fix simple Clippy warnings
fix:
  cargo fmt --all
  cargo clippy --allow-dirty --allow-staged --fix --all-targets --all-features -- -W unused_imports -W clippy::all
  cargo clippy --allow-dirty --allow-staged --fix -- -W unused_imports -W clippy::all
  cd rusteron-docker-samples/rusteron-dummy-example && just fix

# Check dependencies
deps:
  cargo +nightly tree --duplicate
  cargo +nightly udeps

# Clean the project by removing the target directory
clean:
  rm -rf rusteron-archive/target
  rm -rf rusteron-client/target
  rm -rf rusteron-media-driver/target
  rm -rf rusteron-archive/artifacts
  rm -rf rusteron-client/artifacts
  rm -rf rusteron-media-driver/artifacts
  rm -rf rusteron-docker-samples/target
  rm -rf rusteron-docker-samples/rusteron-dummy-example/target
  cd rusteron-archive/aeron && git submodule update --init --recursive --checkout && git reset --hard && git clean -fdx && cd -
  cd rusteron-client/aeron && git submodule update --init --recursive --checkout && git reset --hard && git clean -fdx && cd -
  cd rusteron-media-driver/aeron && git submodule update --init --recursive --checkout && git reset --hard && git clean -fdx && cd -
  cargo clean

# Build the project in release mode
release:
  cargo build --workspace --all-targets --release

# Build the project in debug mode
build:
  COPY_BINDINGS=true cargo build --workspace --all-targets

# Build artifacts
build-artifacts:
  PUBLISH_ARTIFACTS=true cargo build --release --workspace --all-targets --features static,precompile

# Build Docker Samples
build-docker-samples:
    cargo update
    cd rusteron-docker-samples/rusteron-dummy-example && cargo build --release && cd ..
    cd rusteron-docker-samples && just build

# Run all criterion benchmarks
bench:
  cargo bench

# Generate rust docs locally
docs:
  cargo clean --doc
  cargo test  --workspace --doc
  cargo doc --workspace --no-deps --open

# Run unit tests
test:
  cargo test --workspace -- --nocapture
  cargo test  --workspace --doc
  cargo test  --workspace --all-targets --all-features -- --nocapture

# =============================================================================
# Aeron Drivers
# =============================================================================

# Run the Aeron Archive Driver using Java
run-aeron-archive-driver:
    cd ./rusteron-client/aeron; ./gradlew :aeron-all:build; cd -
    cd ./rusteron-client/aeron; ./gradlew :aeron-agent:jar; cd -
    java \
      --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
      -javaagent:./rusteron-client/aeron/aeron-agent/build/libs/aeron-agent-1.48.0-SNAPSHOT.jar \
      -cp ./rusteron-client/aeron/aeron-all/build/libs/aeron-all-1.48.0-SNAPSHOT.jar:./rusteron-client/aeron/aeron-archive/build/libs/aeron-archive-1.48.0-SNAPSHOT.jar \
      -Daeron.dir=target/aeron \
      -Daeron.archive.dir=target/aeron/archive \
      -Daeron.event.log=all \
      -Daeron.event.archive.log=all \
      -Daeron.term.buffer.sparse.file=false \
      -Daeron.pre.touch.mapped.memory=true \
      -Daeron.threading.mode=DEDICATED \
      -Daeron.sender.idle.strategy=noop \
      -Daeron.receiver.idle.strategy=noop \
      -Daeron.conductor.idle.strategy=spin \
      -Dagrona.disable.bounds.checks=true \
      -Daeron.dir.delete.on.start=true \
      -Daeron.dir.delete.on.shutdown=true \
      -Daeron.print.configuration=true \
      -Daeron.archive.control.channel=aeron:udp?endpoint=localhost:8010 \
      -Daeron.archive.replication.channel=aeron:udp?endpoint=localhost:0 \
      -Daeron.archive.control.response.channel=aeron:udp?endpoint=localhost:0 \
      io.aeron.archive.ArchivingMediaDriver

# Run the Aeron Media Driver using Java
run-aeron-media-driver-java:
    cd ./rusteron-client/aeron; ./gradlew :aeron-all:build; cd -
    java -cp ./rusteron-client/aeron/aeron-all/build/libs/aeron-all-*.jar \
      --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
      -Daeron.dir=target/aeron \
      -Daeron.term.buffer.sparse.file=false \
      -Daeron.pre.touch.mapped.memory=true \
      -Daeron.threading.mode=DEDICATED \
      -Daeron.sender.idle.strategy=noop \
      -Daeron.receiver.idle.strategy=noop \
      -Daeron.conductor.idle.strategy=spin \
      -Dagrona.disable.bounds.checks=true \
      -Daeron.dir.delete.on.start=true \
      -Daeron.dir.delete.on.shutdown=true \
      -Daeron.print.configuration=true \
      io.aeron.driver.MediaDriver

# Run the Aeron Media Driver using Rust
run-aeron-media-driver-rust:
    AERON_DIR_DELETE_ON_START=true \
    AERON_DIR_DELETE_ON_SHUTDOWN=true \
    AERON_PRINT_CONFIGURATION=true \
    AERON_THREADING_MODE=DEDICATED \
    AERON_CONDUCTOR_IDLE_STRATEGY=spin \
    AERON_SENDER_IDLE_STRATEGY=noop \
    AERON_RECEIVER_IDLE_STRATEGY=noop \
    AERON_DIR=target/aeron \
    AERON_TERM_BUFFER_SPARSE_FILE=false \
    cargo run --release --package rusteron-media-driver --bin media_driver

# =============================================================================
# Benchmarks
# =============================================================================

# Create symlink for benchmarking
create-sym-link:
  rm -rfv target/aeron
  mkdir -p target/aeron
  ls -l target/aeron

# Java IPC throughput benchmark
benchmark-ipc-throughput-java:
  cd ./rusteron-client/aeron; ./gradlew :aeron-all:build; cd -
  cd ./rusteron-client/aeron; ./gradlew :aeron-samples:jar; cd -
  java -cp ./rusteron-client/aeron/aeron-all/build/libs/aeron-all-1.48.0-SNAPSHOT.jar:./rusteron-client/aeron/aeron-samples/build/libs/aeron-samples-1.48.0-SNAPSHOT.jar \
    --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
    -Daeron.dir=target/aeron \
    -Daeron.term.buffer.sparse.file=false \
    -Daeron.pre.touch.mapped.memory=true \
    -Daeron.threading.mode=DEDICATED \
    -Daeron.sender.idle.strategy=noop \
    -Daeron.receiver.idle.strategy=noop \
    -Daeron.conductor.idle.strategy=spin \
    -Dagrona.disable.bounds.checks=true \
    -Daeron.dir.delete.on.start=true \
    -Daeron.dir.delete.on.shutdown=true \
    -Daeron.print.configuration=true \
    -Daeron.sample.messageLength=32 \
    -Daeron.sample.idleStrategy=org.agrona.concurrent.NoOpIdleStrategy \
    io.aeron.samples.EmbeddedExclusiveIpcThroughput

# Rust IPC throughput benchmark
benchmark-ipc-throughput-rust:
  AERON_DIR_DELETE_ON_START=true \
  AERON_DIR_DELETE_ON_SHUTDOWN=true \
  AERON_PRINT_CONFIGURATION=true \
  AERON_THREADING_MODE=SHARED \
  AERON_CONDUCTOR_IDLE_STRATEGY=spin \
  AERON_SENDER_IDLE_STRATEGY=noop \
  AERON_RECEIVER_IDLE_STRATEGY=noop \
  AERON_DIR=target/aeron \
  AERON_TERM_BUFFER_SPARSE_FILE=false \
  cargo run --release --package rusteron-client --example embedded_exclusive_ipc_throughput

# Rust IPC throughput with perf profiling
benchmark-ipc-throughput-rust-profiler:
  sudo AERON_DIR_DELETE_ON_START=true \
  AERON_DIR_DELETE_ON_SHUTDOWN=true \
  AERON_PRINT_CONFIGURATION=true \
  AERON_THREADING_MODE=DEDICATED \
  AERON_CONDUCTOR_IDLE_STRATEGY=spin \
  AERON_SENDER_IDLE_STRATEGY=noop \
  AERON_RECEIVER_IDLE_STRATEGY=noop \
  AERON_DIR=target/aeron \
  AERON_TERM_BUFFER_SPARSE_FILE=false \
  LD_LIBRARY_PATH=target/release/build/rusteron-client-*/out/build/lib \
  perf record -g ./target/release/examples/embedded_exclusive_ipc_throughput
  sudo perf report

# Java embedded ping-pong
benchmark-embedded-ping-pong-java:
  cd ./rusteron-client/aeron; ./gradlew :aeron-samples:jar; cd -
  java -cp ./rusteron-client/aeron/aeron-all/build/libs/aeron-all-1.48.0-SNAPSHOT.jar:./rusteron-client/aeron/aeron-samples/build/libs/aeron-samples-1.48.0-SNAPSHOT.jar \
    --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
    -Daeron.dir=target/aeron \
    -Daeron.term.buffer.sparse.file=false \
    -Daeron.pre.touch.mapped.memory=true \
    -Daeron.threading.mode=DEDICATED \
    -Daeron.sender.idle.strategy=noop \
    -Daeron.receiver.idle.strategy=noop \
    -Daeron.conductor.idle.strategy=spin \
    -Dagrona.disable.bounds.checks=true \
    -Daeron.dir.delete.on.start=true \
    -Daeron.dir.delete.on.shutdown=true \
    -Daeron.print.configuration=true \
    -Daeron.sample.messageLength=32 \
    -Daeron.sample.idleStrategy=org.agrona.concurrent.NoOpIdleStrategy \
    -Daeron.sample.exclusive.publications=false \
    -Daeron.sample.ping.channel=aeron:udp?endpoint=localhost:20123 \
    -Daeron.sample.pong.channel=aeron:udp?endpoint=localhost:20124 \
    io.aeron.samples.EmbeddedPingPong

# Rust embedded ping-pong
benchmark-embedded-ping-pong-rust:
  AERON_DIR_DELETE_ON_START=true \
  AERON_DIR_DELETE_ON_SHUTDOWN=true \
  AERON_PRINT_CONFIGURATION=true \
  AERON_THREADING_MODE=DEDICATED \
  AERON_CONDUCTOR_IDLE_STRATEGY=spin \
  AERON_SENDER_IDLE_STRATEGY=noop \
  AERON_RECEIVER_IDLE_STRATEGY=noop \
  AERON_DIR=target/aeron \
  AERON_TERM_BUFFER_SPARSE_FILE=false \
  cargo run --release --package rusteron-client --example embedded_ping_pong

# Rust ping-pong with profiler codesign
benchmark-embedded-ping-pong-rust-profiler:
  cargo build --features static --release --package rusteron-client --example embedded_ping_pong
  codesign -s - -vvv --entitlements instruments.plist ./target/release/examples/embedded_ping_pong
  AERON_DIR=target/aeron \
  ./target/release/examples/embedded_ping_pong

# =============================================================================
# Aeron Archive Tools
# =============================================================================

# Run aeron-archive-tool with given dir and action
aeron-archive-tool dir action:
  java --add-opens java.base/jdk.internal.misc=ALL-UNNAMED -cp ./rusteron-client/aeron/aeron-all/build/libs/aeron-all-*.jar io.aeron.archive.ArchiveTool {{dir}} {{action}}

# Run aeron-stat tool on a given directory
aeron-stat dir:
  java --add-opens java.base/jdk.internal.misc=ALL-UNNAMED -cp ./rusteron-archive/aeron/aeron-all/build/libs/aeron-all-*.jar -Daeron.dir={{dir}} io.aeron.samples.AeronStat

# =============================================================================
# Aeron Version
# =============================================================================

# Update Aeron submodules to a tag, branch, or commit.
# Usage: just update-aeron-version 1.48.5   (auto-detects tag)
#        just update-aeron-version master
#        just update-aeron-version <commit-sha>
update-aeron-version version:
    #!/usr/bin/env bash
    set -euo pipefail
    RAW="{{version}}"
    TARGET=${RAW#refs/}
    TARGET=${TARGET#tags/}
    echo "Updating Aeron submodules to tag/branch/commit input='${RAW}' normalised='${TARGET}'"
    for DIR in rusteron-client/aeron rusteron-archive/aeron rusteron-media-driver/aeron; do
      echo "-- $DIR"
      (
        cd "$DIR"
        git fetch --tags --prune origin >/dev/null 2>&1 || true
        MODE=""; REF=""
        if git show-ref --verify --quiet "refs/tags/$TARGET"; then
          MODE=tag; REF="tags/$TARGET"
        elif git show-ref --verify --quiet "refs/heads/$TARGET"; then
          MODE=branch; REF="$TARGET"
        elif git rev-parse --verify --quiet "$TARGET^{commit}" >/dev/null 2>&1; then
          MODE=commit; REF="$TARGET"
        else
          echo "ERROR: Cannot resolve '$TARGET' as tag, branch, or commit in $DIR" >&2; exit 1
        fi
        case "$MODE" in
          tag)
            COMMIT=$(git rev-list -n1 "$REF"); LOCAL_BRANCH="aeron-$TARGET"; git checkout -B "$LOCAL_BRANCH" "$COMMIT" >/dev/null ;;
          branch)
            git checkout "$REF" >/dev/null; git pull --ff-only || true; COMMIT=$(git rev-parse HEAD) ;;
          commit)
            COMMIT=$(git rev-parse "$REF"); LOCAL_BRANCH="aeron-${COMMIT:0:8}"; git checkout -B "$LOCAL_BRANCH" "$COMMIT" >/dev/null ;;
        esac
        SHORT=$(git rev-parse --short "$COMMIT")
        DESC=$(git describe --tags --exact-match "$COMMIT" 2>/dev/null || git describe --tags --always "$COMMIT" 2>/dev/null || echo "$SHORT")
        echo "   Mode=$MODE Commit=$SHORT Describe=$DESC Branch=$(git branch --show-current || echo detached)"
      ) || exit 1
    done

# Show current Aeron submodule commits (describe + branch/detached)
show-aeron-version:
    set -euo pipefail
    for DIR in rusteron-client/aeron rusteron-archive/aeron rusteron-media-driver/aeron; do \
      ( cd $DIR; \
        BR=$(git branch --show-current || echo detached); \
        CM=$(git rev-parse --short HEAD); \
        DESC=$(git describe --tags --always --dirty 2>/dev/null || echo "$CM"); \
        echo "$DIR => $DESC ($CM) branch=$BR"; \
      ); \
    done

# Update to the latest Aeron release tag from GitHub
update-to-latest-aeron-version:
    just update-aeron-version `curl -s https://api.github.com/repos/real-logic/aeron/releases | grep '"tag_name"' | head -1 | cut -d '"' -f4`
