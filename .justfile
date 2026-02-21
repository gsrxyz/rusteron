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
  cargo fmt --all -- --check
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

# CANNOT USE MIRI due to ffi :(
#check-miri:
#  rustup toolchain install nightly --component miri
#  rustup run nightly cargo miri setup
#  MIRIFLAGS="" rustup run nightly cargo miri test -p rusteron-code-gen --lib -- test_drop_

test-valgrind:
    test -f ./id_ed25519 || ssh-keygen -t ed25519 -N "" -C "container@$(hostname)" -f ./id_ed25519
    docker build --platform=linux/amd64 -f Dockerfile -t rusteron-valgrind .
    docker run --rm --platform=linux/amd64 \
      --shm-size=2g \
      -e HOME=/work/target/asan \
      -e TMP=/work/target/asan/tmp \
      -e TEMP=/work/target/asan/tmp \
      -e GRADLE_USER_HOME=/work/target/asan/gradle \
      -e CARGO_HOME=/work/target/asan/cargo-home \
      -e CARGO_TARGET_DIR=/work/target/asan/target \
      -e RUST_TEST_THREADS=1 \
      -v "$PWD:/work" \
      --entrypoint valgrind \
      rusteron-valgrind \
      --tool=memcheck \
      --error-exitcode=1 \
      --track-origins=yes \
      --leak-check=full \
      --show-leak-kinds=all \
      --track-origins=yes \
      --track-fds=yes \
      --show-reachable=yes \
      --show-possibly-lost=yes \
      --errors-for-leak-kinds=all \
      --gen-suppressions=no \
      --num-callers=30 \
      --suppressions=/dev/null \
      cargo test --workspace -- --test-threads=1

test-valgrind2:
    test -f ./id_ed25519 || ssh-keygen -t ed25519 -N "" -C "container@$(hostname)" -f ./id_ed25519
    docker build --platform=linux/amd64 -f Dockerfile -t rusteron-valgrind .
    docker container inspect rusteron-valgrind-test >/dev/null 2>&1 || \
      docker create --platform=linux/amd64 \
        --name rusteron-valgrind-test \
        --shm-size=2g \
        -e HOME=/work/target/asan \
        -e TMP=/work/target/asan/tmp \
        -e TEMP=/work/target/asan/tmp \
        -e GRADLE_USER_HOME=/work/target/asan/gradle \
        -e CARGO_HOME=/work/target/asan/cargo-home \
        -e CARGO_TARGET_DIR=/work/target/asan/target \
        -e RUST_TEST_THREADS=1 \
        -v "$PWD:/work" \
        --entrypoint valgrind \
        rusteron-valgrind \
        --tool=memcheck \
        --error-exitcode=1 \
        --track-origins=yes \
        --leak-check=full \
        --show-leak-kinds=all \
        --track-origins=yes \
        --track-fds=yes \
        --show-reachable=yes \
        --show-possibly-lost=yes \
        --errors-for-leak-kinds=all \
        --gen-suppressions=no \
        --num-callers=30 \
        --suppressions=/dev/null \
        cargo test --workspace -- --test-threads=1
    docker start -a rusteron-valgrind-test

# starts docker image with valgrind and ssh, can ssh with ssh -i $PWD/id_ed25519 -p 2222 root@localhost
start-valgrind-sshd:
    test -f ./id_ed25519 || ssh-keygen -t ed25519 -N "" -C "container@$(hostname)" -f ./id_ed25519
    docker build --platform=linux/amd64 -f Dockerfile -t rusteron-valgrind .
    docker rm -f rusteron-valgrind-sshd >/dev/null 2>&1 || true
    docker run --rm --platform=linux/amd64 \
      --shm-size=2g \
      -v "$PWD:/work" \
      -p 2222:22 \
      --name rusteron-valgrind-sshd \
      --entrypoint /bin/sh \
      rusteron-valgrind \
      -c "sed -i '/^ListenAddress /d' /etc/ssh/sshd_config && exec /usr/sbin/sshd -D -e -o ListenAddress=0.0.0.0"

# Run unit tests
test:
  cargo test --workspace -- --nocapture
  cargo test  --workspace --doc
  cargo test  --workspace --all-targets --all-features -- --nocapture

# Run slow consumer tests (normally ignored)
slow-tests:
  cargo test --package rusteron-archive --lib --features "precompile static" -- --ignored --nocapture

# =============================================================================
# MDC Loss Testing (macOS)
# =============================================================================

# Run MDC diagnostics with packet loss on subscriber port 32930 (macOS).
# Usage: just mdc-loss-run [duration_secs] [report_interval_secs] [loss_rate] [host] [force_lo0]
# Example: just mdc-loss-run 120 10 0.10
# Example: just mdc-loss-run 120 10 1.0 127.0.0.1
# Example: just mdc-loss-run 120 10 0.10 '' 0  # disable loopback PF workaround
mdc-loss-run duration='180' report_interval='30' loss='0.10' host='' force_lo0='1':
  #!/usr/bin/env bash
  set -euo pipefail
  if [[ "$(uname)" != "Darwin" ]]; then
    echo "mdc-loss-run is macOS-only"
    exit 1
  fi

  HOST="{{host}}"
  if [[ -z "$HOST" ]]; then
    HOST="$(ipconfig getifaddr en0 2>/dev/null || true)"
  fi
  if [[ -z "$HOST" ]]; then
    HOST="$(ipconfig getifaddr en1 2>/dev/null || true)"
  fi
  if [[ -z "$HOST" ]]; then
    HOST="127.0.0.1"
  fi
  echo "Using MDC host: $HOST"
  ROUTE_IF="$(route -n get "$HOST" 2>/dev/null | awk '/interface:/{print $2; exit}')"
  echo "Route interface for $HOST: ${ROUTE_IF:-unknown}"

  PF_MAIN_TMP=""
  LOADED_MAIN_PF=0

  cleanup() {
    sudo pfctl -a com.apple/rusteron-mdc-loss -F all || true
    sudo dnctl -q flush || true
    if [[ "$LOADED_MAIN_PF" == "1" ]]; then
      sudo pfctl -f /etc/pf.conf || true
    fi
    if [[ -n "$PF_MAIN_TMP" ]] && [[ -f "$PF_MAIN_TMP" ]]; then
      rm -f "$PF_MAIN_TMP" || true
    fi
  }
  trap cleanup EXIT INT TERM

  if [[ "${ROUTE_IF:-}" == "lo0" ]] && [[ "{{force_lo0}}" == "0" ]]; then
    echo "Route to $HOST uses lo0 and force_lo0=0."
    echo "PF on default macOS rules typically skips lo0, so no loss will be observed."
    echo "Re-run with force_lo0=1 or choose a non-local destination host."
    exit 2
  fi

  sudo pfctl -E
  if [[ "${ROUTE_IF:-}" == "lo0" ]]; then

    PF_MAIN_TMP="$(mktemp /tmp/rusteron-pf-main.XXXXXX.conf)"
    cat > "$PF_MAIN_TMP" <<EOF
  # Temporary PF ruleset for rusteron mdc-loss-run.
  # Intentionally omits "set skip on lo0" so dummynet can affect local traffic.
  scrub-anchor "com.apple/*" all fragment reassemble
  nat-anchor "com.apple/*"
  rdr-anchor "com.apple/*"
  dummynet-anchor "com.apple/*"
  anchor "com.apple/*"
  load anchor "com.apple/rusteron-mdc-loss" from "$PWD/rusteron-client/examples/pf-macos-mdc-loss.conf"
  pass quick all flags S/SA keep state
  EOF
    echo "Route uses lo0; loading temporary PF main ruleset without lo0 skip."
    sudo pfctl -f "$PF_MAIN_TMP"
    LOADED_MAIN_PF=1
  fi

  sudo dnctl -q flush
  sudo dnctl pipe 1 config plr {{loss}} delay 0ms bw 100Mbit/s
  echo "=== Dummynet pipe config ==="
  sudo dnctl pipe show 1
  sudo pfctl -a com.apple/rusteron-mdc-loss -F all
  sudo pfctl -a com.apple/rusteron-mdc-loss -f "$PWD/rusteron-client/examples/pf-macos-mdc-loss.conf"
  echo "=== PF active rules (anchor) ==="
  sudo pfctl -a com.apple/rusteron-mdc-loss -sr
  echo "=== PF skip-on-lo0 check (main ruleset) ==="
  sudo pfctl -sr | rg -n "skip on lo0" || true
  echo "=== Dummynet counters before test ==="
  sudo dnctl -s pipe show 1 || true
  echo "=== UDP probe to $HOST:32930 ==="
  python3 - "$HOST" <<'PY'
  import socket
  import sys
  
  host = sys.argv[1]
  port = 32930
  sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
  for _ in range(250):
      sock.sendto(b"probe", (host, port))
  sock.close()
  PY
  sleep 1
  echo "=== Dummynet counters after probe ==="
  sudo dnctl -s pipe show 1 || true
  echo "=== Running test with RUSTERON_MDC_HOST=$HOST ==="
  RUSTERON_MDC_HOST=$HOST \
  RUSTERON_MDC_TEST_DURATION_SECS={{duration}} \
  RUSTERON_MDC_REPORT_INTERVAL_SECS={{report_interval}} \
    cargo test -p rusteron-client --lib mdc_unreliable_gap_latency_histogram_report -- --ignored --nocapture
  echo "=== Dummynet counters after test ==="
  sudo dnctl -s pipe show 1 || true

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

#test-asan:
#  rustup toolchain install nightly-2024-12-05
#  rustup component add rust-src --toolchain nightly-aarch64-apple-darwin
#  rustup default nightly-2024-12-05
#  RUSTUP_TOOLCHAIN=nightly-2024-12-05 RUSTC="$(rustup which rustc --toolchain nightly-2024-12-05)" DYLD_INSERT_LIBRARIES="$(rustup run nightly-2024-12-05 rustc --print target-libdir)/librustc-nightly_rt.asan.dylib" ASAN_OPTIONS=detect_leaks=1,abort_on_error=1 CFLAGS="-fsanitize=address" RUSTFLAGS="-Zsanitizer=address" "$(rustup which cargo --toolchain nightly-2024-12-05)" -Z build-std test --package rusteron-client --lib -- --nocapture
#  RUSTUP_TOOLCHAIN=nightly-2024-12-05 RUSTC="$(rustup which rustc --toolchain nightly-2024-12-05)" DYLD_INSERT_LIBRARIES="$(rustup run nightly-2024-12-05 rustc --print target-libdir)/librustc-nightly_rt.asan.dylib" ASAN_OPTIONS=detect_leaks=1,abort_on_error=1 CFLAGS="-fsanitize=address" RUSTFLAGS="-Zsanitizer=address" "$(rustup which cargo --toolchain nightly-2024-12-05)" -Z build-std test --package rusteron-archive --lib -- --nocapture
