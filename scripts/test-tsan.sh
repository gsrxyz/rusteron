#!/usr/bin/env bash
# Instrument Rust, its standard library, and the Aeron C libraries together.
set -euo pipefail

export CC="${CC:-clang}"
export CXX="${CXX:-clang++}"
export RUSTC_BOOTSTRAP=1
export RUSTFLAGS="${RUSTFLAGS:-} -Zsanitizer=thread"
export RUSTERON_SANITIZE=thread
export RUSTERON_BUILD_FROM_SOURCE=1
suppression_file="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/tsan.supp"
export TSAN_OPTIONS="${TSAN_OPTIONS:+${TSAN_OPTIONS}:}halt_on_error=1:exitcode=66:suppressions=${suppression_file}"
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG="${CARGO_PROFILE_DEV_DEBUG:-1}"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-target/tsan}"
export AERON_DIR="${AERON_DIR:-${TMPDIR:-/tmp}/rusteron-tsan}"
target="$(rustc -vV | sed -n 's/^host: //p')"

cargo test -Zbuild-std --target "$target" -p rusteron-media-driver --features static \
    --lib extensions -- --test-threads=1 --skip panicking_extension_aborts
cargo test -Zbuild-std --target "$target" -p rusteron-media-driver --features static \
    --test driver_extensions --test thread_safety -- --test-threads=1
cargo test -Zbuild-std --target "$target" -p rusteron-media-driver-links-test \
    --features rusteron-media-driver/static --tests -- --test-threads=1
# The skipped test deliberately leaves a conductor alive after a self-join fails.
# Its ordinary run checks the leak fallback; TSan correctly reports a thread leak.
cargo test -Zbuild-std --target "$target" -p rusteron-client --features static \
    --test thread_safety --test edge_cases -- --test-threads=1 \
    --skip releasing_the_last_client_from_its_own_error_callback_returns
cargo test -Zbuild-std --target "$target" -p rusteron-archive --features static \
    --test thread_safety --test lifecycle --test callback_ownership \
    --test recording_position --test soundness_regression -- --test-threads=1

# A passing suite means little if its native code was not instrumented, or if a
# suppression hid callback races. Require a report for each deliberate defect.
negative_log="$(mktemp)"
trap 'rm -f "$negative_log"' EXIT
for control in native_race_is_detected rust_callback_race_is_detected; do
    if cargo test -Zbuild-std --target "$target" -p rusteron-media-driver --features static \
        --test tsan_detection -- --ignored --exact "$control" > "$negative_log" 2>&1; then
        cat "$negative_log"
        echo "TSan did not reject $control" >&2
        exit 1
    fi
    case "$control" in
        native_race_is_detected) frame=aeron_driver_context_set_dir_delete_on_shutdown ;;
        rust_callback_race_is_detected) frame=tsan_rust_callback_write ;;
    esac
    if ! grep -q 'WARNING: ThreadSanitizer: data race' "$negative_log" \
        || ! grep -q "$frame" "$negative_log" \
        || ! grep -q 'exit status: 66' "$negative_log"; then
        cat "$negative_log"
        echo "TSan negative control failed without its expected race report: $control" >&2
        exit 1
    fi
    echo "TSan detected the deliberate race: $control"
done
