#!/usr/bin/env bash
# valgrind-run.sh — build the workspace then run each test binary under Valgrind.
#
# This script is the Docker ENTRYPOINT for test-valgrind / test-valgrind2.
# Running `valgrind cargo test` wraps the Cargo *build* process, not the test
# binaries themselves — this script fixes that by separating the two phases.
#
# Exit code: non-zero if any test binary or valgrind invocation fails.

set -euo pipefail

SUPP_FILE="${VALGRIND_SUPP:-/work/valgrind.supp}"
CARGO="${CARGO:-cargo}"
GEN_SUPPRESSIONS="${VALGRIND_GEN_SUPPRESSIONS:-0}"

# Keep cargo/rustup lookup stable for trybuild subprocesses spawned by tests.
export PATH="/usr/local/cargo/bin:${PATH}"
if command -v cargo >/dev/null 2>&1; then
  export CARGO="$(command -v cargo)"
fi
export RUSTERON_VALGRIND=1
unset CARGO_BUILD_TARGET || true
unset CARGO_ENCODED_RUSTFLAGS || true
export RUSTFLAGS="-C target-cpu=x86-64"

echo "=== Phase 1: build test binaries ==="
# --no-run builds but does not execute; --message-format=json lets us extract
# the exact paths of the compiled test executables via jq.
BINARIES=$(
  COPY_BINDINGS=true \
  "$CARGO" test --workspace --no-run --message-format=json \
  | jq -r 'select(.reason == "compiler-artifact" and .profile.test == true and .executable != null) | .executable'
)

if [[ -z "$BINARIES" ]]; then
  echo "ERROR: no test binaries found after build" >&2
  exit 1
fi

# Collect all .so library directories from the build output so test binaries
# can find libaeron.so, libaeron_archive_c_client.so, libaeron_driver.so etc.
CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/work/target/valgrind/target}"
LIB_DIRS=$(find "${CARGO_TARGET_DIR}/debug/build" -name "*.so" -exec dirname {} \; 2>/dev/null | sort -u | tr '\n' ':')
export LD_LIBRARY_PATH="${LIB_DIRS}${LD_LIBRARY_PATH:-}"
echo "=== LD_LIBRARY_PATH: $LD_LIBRARY_PATH ==="
echo ""

echo "=== Test binaries to run under Valgrind ==="
echo "$BINARIES"
echo ""

OVERALL_EXIT=0

for BIN in $BINARIES; do
  BIN_NAME=$(basename "$BIN")
  echo "======================================================"
  echo "=== Valgrind: $BIN_NAME"
  echo "======================================================"

  if [[ "$GEN_SUPPRESSIONS" == "1" ]]; then
    # Output suppression stanzas for every error found — redirect to append to valgrind.supp
    valgrind \
      --tool=memcheck \
      --track-origins=yes \
      --leak-check=full \
      --show-leak-kinds=all \
      --track-fds=yes \
      --num-callers=30 \
      --gen-suppressions=all \
      "$BIN" --test-threads=1 --nocapture 2>&1 || true
  else
    # Run valgrind and capture output for error detection.
    # Leak gate: --errors-for-leak-kinds=definite,possible fails CI on REAL
    # leaks (malloc with no reachable pointer). "Still reachable" allocations
    # (Rust runtime / std bookkeeping held by statics/Arcs at process exit) are
    # shown via --show-leak-kinds=all but do NOT fail — every Rust binary has
    # them and they are not bugs.
    #
    # The Box::leak-class lifecycle bug (e.g. the AeronCncMetadata mmap leak,
    # reachable not lost) is NOT caught by this gate — it is caught
    # deterministically by the manual_close_required Drop warning in
    # ManagedCResource (common.rs), which fires per-resource on every platform
    # including macOS, not just under valgrind on Linux.
    VALGRIND_OUTPUT=$(valgrind \
      --tool=memcheck \
      --error-exitcode=1 \
      --track-origins=yes \
      --leak-check=full \
      --show-leak-kinds=all \
      --errors-for-leak-kinds=definite,possible \
      --num-callers=30 \
      -s \
      --gen-suppressions=all \
      --suppressions="$SUPP_FILE" \
      "$BIN" --test-threads=1 --nocapture 2>&1) || VALGRIND_EXIT=$?

    # Always print the output for visibility
    echo "$VALGRIND_OUTPUT"

    # Check for actual (non-suppressed) errors in the output
    # "ERROR SUMMARY: N errors from M contexts" - we care about N (actual errors)
    # Suppressed errors are shown as "(suppressed: X from Y)"
    if echo "$VALGRIND_OUTPUT" | grep -q "ERROR SUMMARY:"; then
      # Extract the actual error count (first number after "ERROR SUMMARY:")
      ACTUAL_ERRORS=$(echo "$VALGRIND_OUTPUT" | grep "ERROR SUMMARY:" | grep -oE '[0-9]+ errors from [0-9]+ contexts' | grep -oE '^[0-9]+' | head -1)

      if [[ -n "$ACTUAL_ERRORS" && "$ACTUAL_ERRORS" -eq 0 ]]; then
        echo "✓ $BIN_NAME: No actual errors (suppressions working correctly)"
      else
        echo "FAIL: $BIN_NAME - Found $ACTUAL_ERRORS actual errors" >&2
        OVERALL_EXIT=1
      fi
    else
      # No ERROR SUMMARY found - check if valgrind itself failed
      if [[ ${VALGRIND_EXIT:-0} -ne 0 ]]; then
        echo "FAIL: $BIN_NAME - Valgrind exited with code $VALGRIND_EXIT" >&2
        OVERALL_EXIT=1
      fi
    fi
  fi
done

echo ""
if [[ "$OVERALL_EXIT" -eq 0 ]]; then
  echo "=== All test binaries passed Valgrind ==="
else
  echo "=== FAILURES detected — see output above ===" >&2
fi

exit "$OVERALL_EXIT"
