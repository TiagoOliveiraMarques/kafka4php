#!/usr/bin/env bash
# Run a command inside the kafka4php toolchain container (docker/dev.Dockerfile).
#
#   scripts/dev.sh cargo test --workspace
#   scripts/dev.sh --with-kafka cargo test -p k4p-host     # also starts test Kafka
#   PHP_VERSION=8.5 scripts/dev.sh php -v
#   scripts/dev.sh                                          # interactive shell
#
# The repo is mounted at /work. The cargo registry/git caches and target/ live in
# named Docker volumes (target/ per image variant, since it links against libphp).
# With --with-kafka, docker/compose.test.yml is started (and waited for) and the
# container joins its network, where the broker is `kafka:9092`.
#
# Env: PHP_VERSION (default 8.4); PHP_DEBUG=1 for the ZTS debug PHP variant (tests only,
# image kafka4php-dev:php<ver>-debug); K4P_TTY=0|1 to force TTY allocation off/on.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
php_version="${PHP_VERSION:-8.4}"
variant="php${php_version}"
if [[ "${PHP_DEBUG:-0}" == 1 ]]; then
    variant="${variant}-debug"
fi
image="kafka4php-dev:${variant}"

with_kafka=0
if [[ "${1:-}" == "--with-kafka" ]]; then
    with_kafka=1
    shift
fi

host_root="$root"
if command -v cygpath >/dev/null 2>&1; then
    # Git Bash / MSYS: give Docker a Windows path and stop MSYS from rewriting
    # container paths such as /work in our arguments.
    host_root="$(cygpath -m "$root")"
    export MSYS_NO_PATHCONV=1
fi

docker build --quiet \
    -f "$host_root/docker/dev.Dockerfile" \
    --build-arg "PHP_VERSION=$php_version" \
    --build-arg "PHP_DEBUG=${PHP_DEBUG:-0}" \
    -t "$image" \
    "$host_root" >/dev/null

run_args=(--rm -i)
if [[ "${K4P_TTY:-}" == 1 ]] || { [[ -z "${K4P_TTY:-}" ]] && [[ -t 0 && -t 1 ]]; }; then
    run_args+=(-t)
fi

if [[ $with_kafka == 1 ]]; then
    docker compose -f "$host_root/docker/compose.test.yml" up -d --wait >&2
    run_args+=(--network kafka4php-test -e K4P_KAFKA_BOOTSTRAP=kafka:9092)
fi

if [[ $# -eq 0 ]]; then
    set -- bash
fi

exec docker run "${run_args[@]}" \
    -v "$host_root:/work" \
    -v k4p-cargo-registry:/usr/local/cargo/registry \
    -v k4p-cargo-git:/usr/local/cargo/git \
    -v "k4p-target-${variant}:/work/target" \
    -w /work \
    -e CARGO_TERM_COLOR \
    -e RUST_BACKTRACE \
    -e RUST_LOG \
    "$image" "$@"
