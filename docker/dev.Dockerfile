# kafka4php toolchain image: ZTS + embed PHP (libphp.so), Rust, and the native build
# tools. All builds and tests run inside it (DESIGN.md §8). Build context: repo root.
#
#   docker build -f docker/dev.Dockerfile --build-arg PHP_VERSION=8.4 -t kafka4php-dev:php8.4 .
#   ... --build-arg PHP_DEBUG=1 -t kafka4php-dev:php8.4-debug .   # ZTS debug PHP, tests only
#
# Normally used through scripts/dev.sh / scripts/dev.ps1.

ARG PHP_VERSION=8.4

FROM composer:2.10.3 AS composer

FROM php:${PHP_VERSION}-zts-bookworm

# PHP must be ZTS with the embed SAPI (libphp.so), per-thread execution timers and
# OPcache. The official ZTS images provide all of it; fail the build if they don't.
COPY docker/php-check.sh /usr/local/bin/k4p-php-check
RUN k4p-php-check

# Native toolchain: C for the SAPI (cc crate), clang for bindgen, cmake + headers for
# librdkafka (rust-rdkafka cmake-build, ssl, zstd, sasl).
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        clang \
        cmake \
        git \
        libclang-dev \
        libsasl2-dev \
        libssl-dev \
        libzstd-dev \
        pkg-config \
        unzip \
    ; \
    rm -rf /var/lib/apt/lists/*

COPY --from=composer /usr/bin/composer /usr/local/bin/composer

# Rust, pinned by rust-toolchain.toml (installed system-wide, like the rust images).
ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH
COPY rust-toolchain.toml /tmp/rust-toolchain/
RUN set -eux; \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
        | sh -s -- -y --no-modify-path --profile minimal --default-toolchain none; \
    cd /tmp/rust-toolchain; \
    rustup toolchain install; \
    toolchain="$(sed -n 's/^channel *= *"\(.*\)"/\1/p' rust-toolchain.toml)"; \
    rustup default "$toolchain"; \
    cd /; \
    rm -rf /tmp/rust-toolchain; \
    chmod -R a+w "$RUSTUP_HOME" "$CARGO_HOME"; \
    rustc --version; cargo --version; cargo clippy --version; cargo fmt --version

# Opt-in, tests only: PHP_DEBUG=1 rebuilds PHP from the same source as a ZTS debug
# build (the k4p-php-sys leak check). Last, so both variants share every layer above.
ARG PHP_DEBUG=0
COPY docker/php-debug-build.sh /usr/local/bin/k4p-php-debug-build
RUN if [ "$PHP_DEBUG" = 1 ]; then k4p-php-debug-build && k4p-php-check; fi

WORKDIR /work
CMD ["bash"]
