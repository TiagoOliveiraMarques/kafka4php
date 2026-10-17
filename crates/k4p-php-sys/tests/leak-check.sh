#!/usr/bin/env bash
# Request-memory leak check for the kafka4php SAPI (PLAN.md S2), against a ZTS *debug*
# build of PHP: the toolchain image built with PHP_DEBUG=1 (docker/dev.Dockerfile). A
# debug PHP tracks every emalloc() and prints each leak to stderr at request shutdown
# ("... Freeing 0x... (N bytes) ..."); it also enables ZEND_ASSERT()s.
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
# The debug image takes a few minutes to build the first time. CI runs the
# `--in-container` part directly in the debug image it built (.github/workflows/build.yml).
set -euo pipefail

leak_pattern='Freeing 0x|memory leaks detected|Last leak repeated|Leaked [0-9]'

if [[ "${1:-}" == --in-container ]]; then
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

# Host side: run this script inside the debug variant of the toolchain image.
exec env PHP_DEBUG=1 "$(dirname "${BASH_SOURCE[0]}")/../../../scripts/dev.sh" \
    bash crates/k4p-php-sys/tests/leak-check.sh --in-container
