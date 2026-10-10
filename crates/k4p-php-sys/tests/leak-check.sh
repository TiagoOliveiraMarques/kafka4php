#!/usr/bin/env bash
# Request-memory leak check for the kafka4php SAPI (PLAN.md S2), against a ZTS *debug*
# build of PHP (tests/debug-php.Dockerfile). A debug PHP tracks every emalloc() and
# prints each leak to stderr at request shutdown ("... Freeing 0x... (N bytes) ...");
# it also enables ZEND_ASSERT()s.
#
# Runs, in the debug image:
#   1. the `leaks` test (1,000 requests over every fixture/outcome kind),
#   2. the `sapi` test suite,
#   3. the positive control (zend_leak_bytes()), which must be reported,
# and fails if 1. or 2. report any leak, or 3. doesn't.
#
#   crates/k4p-php-sys/tests/leak-check.sh          # from Git Bash or Linux, repo root
#   PHP_VERSION=8.5 crates/k4p-php-sys/tests/leak-check.sh
#
# The debug image takes a few minutes to build the first time.
set -euo pipefail

leak_pattern='Freeing 0x|memory leaks detected|Last leak repeated|Leaked [0-9]'

if [[ "${K4P_LEAK_CHECK_INNER:-}" == 1 ]]; then
    # Inside the debug container, repo at /work.
    php -v | grep -q DEBUG || { echo "leak-check: PHP is not a debug build" >&2; exit 1; }
    cargo build --quiet -p k4p-php-sys --tests
    out=/tmp/k4p-leak-check
    mkdir -p "$out"
    status=0

    for t in leaks sapi; do
        echo "leak-check: running the '$t' test on debug PHP"
        if ! cargo test --quiet -p k4p-php-sys --test "$t" 2>"$out/$t.err"; then
            echo "leak-check: '$t' test FAILED; stderr:" >&2
            cat "$out/$t.err" >&2
            status=1
        fi
        if grep -Eq "$leak_pattern" "$out/$t.err"; then
            echo "leak-check: LEAKS reported during the '$t' test:" >&2
            grep -E -A1 "$leak_pattern" "$out/$t.err" >&2
            status=1
        else
            echo "leak-check: no leaks reported during the '$t' test"
        fi
    done

    echo "leak-check: positive control (zend_leak_bytes)"
    K4P_LEAK_CONTROL=1 cargo test --quiet -p k4p-php-sys --test leaks 2>"$out/control.err" || true
    if grep -Eq "$leak_pattern" "$out/control.err"; then
        echo "leak-check: positive control reported as expected:"
        grep -E "$leak_pattern" "$out/control.err" | head -3
    else
        echo "leak-check: positive control NOT reported: leak detection is not working" >&2
        cat "$out/control.err" >&2
        status=1
    fi

    if [[ $status == 0 ]]; then
        echo "leak-check: OK"
    fi
    exit $status
fi

# Host side: build the images, run this script inside the debug image.
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
php_version="${PHP_VERSION:-8.4}"
image="kafka4php-dev:php${php_version}-debug"

host_root="$root"
if command -v cygpath >/dev/null 2>&1; then
    host_root="$(cygpath -m "$root")"
    export MSYS_NO_PATHCONV=1
fi

# Base toolchain image (built/cached by dev.sh), then the debug PHP on top of it.
PHP_VERSION="$php_version" "$root/scripts/dev.sh" true
docker build \
    -f "$host_root/crates/k4p-php-sys/tests/debug-php.Dockerfile" \
    --build-arg "PHP_VERSION=$php_version" \
    -t "$image" \
    "$host_root/crates/k4p-php-sys/tests"

exec docker run --rm -i \
    -v "$host_root:/work" \
    -v k4p-cargo-registry:/usr/local/cargo/registry \
    -v k4p-cargo-git:/usr/local/cargo/git \
    -v "k4p-target-php${php_version}-debug:/work/target" \
    -w /work \
    -e K4P_LEAK_CHECK_INNER=1 \
    "$image" bash crates/k4p-php-sys/tests/leak-check.sh
