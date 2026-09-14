#!/usr/bin/env bash
set -euo pipefail
export PATH="/usr/local/cargo/bin:${PATH}"
export RUSTUP_TOOLCHAIN=1.95.0-x86_64-unknown-linux-gnu
export RUSTERON_VALGRIND=1
export CARGO_NET_OFFLINE=true
export RUSTFLAGS="-C target-cpu=x86-64"
export HOME=/tmp/valgrind
export TMP=/tmp/valgrind/tmp
export TEMP=/tmp/valgrind/tmp
export CARGO_HOME=/tmp/valgrind/cargo-home
export CARGO_TARGET_DIR=/tmp/valgrind/target
mkdir -p "$TMP" "$CARGO_HOME" "$CARGO_TARGET_DIR"

echo "=== Building rusteron-client test binary ==="
BIN=$(COPY_BINDINGS=true cargo test -p rusteron-client --lib --no-run --message-format=json \
  | jq -r 'select(.reason == "compiler-artifact" and .profile.test == true and .executable != null) | .executable' | head -1)
echo "BIN=$BIN"

LIB_DIRS=$(find "${CARGO_TARGET_DIR}/debug/build" -name "*.so" -exec dirname {} \; 2>/dev/null | sort -u | tr '\n' ':')
export LD_LIBRARY_PATH="${LIB_DIRS}${LD_LIBRARY_PATH:-}"
echo "LD_LIBRARY_PATH=$LD_LIBRARY_PATH"

echo "=== Running valgrind memcheck on new async cancel/leak tests ==="
valgrind \
  --tool=memcheck \
  --error-exitcode=1 \
  --track-origins=yes \
  --leak-check=full \
  --show-leak-kinds=all \
  --errors-for-leak-kinds=definite,possible \
  --num-callers=30 \
  -s \
  --suppressions=/work/valgrind.supp \
  "$BIN" --test-threads=1 --nocapture \
  dropping_unpolled_async_subscription_does_not_leak_driver_counter \
  dropping_unpolled_async_publication_does_not_leak_driver_counter \
  dropping_unpolled_async_exclusive_publication_does_not_leak_driver_counter \
  dropping_unpolled_async_counter_does_not_leak_driver_counter \
  async_add_subscription_cancel_is_explicit_and_idempotent \
  async_add_subscription_cancel_after_resolved_poll_is_inert \
  async_add_publication_exclusive_publication_and_counter_cancel_is_explicit_idempotent_and_inert_after_resolve \
  async_add_subscription_invalid_uri_fails_cleanly \
  async_add_publication_invalid_uri_fails_cleanly \
  subscription_remove_destination_round_trips_and_rejects_unknown_destination
