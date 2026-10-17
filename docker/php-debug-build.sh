#!/bin/sh
# Rebuilds the base image's PHP as a ZTS *debug* build (dev.Dockerfile, PHP_DEBUG=1),
# for tests only: a debug PHP tracks every emalloc() and reports request-memory leaks
# on stderr at request shutdown, and enables ZEND_ASSERT()s. Used by
# crates/k4p-php-sys/tests/leak-check.sh. Never part of the normal or production image.
#
# Same source (docker-php-source) and configure flags as the installed PHP, plus
# --enable-debug and --enable-zend-test (zend_leak_bytes(): the leak check's positive
# control). Installed into /usr/local, so php-config, pecl and docker-php-ext-install
# keep working.
set -eux

# Build deps of the official image (its Dockerfile purges them after building).
apt-get update
# shellcheck disable=SC2086
apt-get install -y --no-install-recommends $PHPIZE_DEPS \
    libargon2-dev libcurl4-openssl-dev libonig-dev libreadline-dev \
    libsodium-dev libsqlite3-dev libssl-dev libxml2-dev zlib1g-dev
rm -rf /var/lib/apt/lists/*

# Original configure flags, minus the trailing VAR=value pairs (PHP_UNAME has spaces).
configure_options="$(php-config --configure-options | sed -E 's/ build_alias=.*$//')"
export CFLAGS="$PHP_CFLAGS" CPPFLAGS="$PHP_CPPFLAGS" LDFLAGS="$PHP_LDFLAGS"

docker-php-source extract
cd /usr/src/php
# shellcheck disable=SC2086
./configure $configure_options --enable-debug --enable-zend-test
make -j"$(nproc)"
find . -type f -name '*.a' -delete
# Shared extensions of the official image (sodium; opcache on 8.4) are rebuilt for the
# debug ABI by `make install`; drop the old directory so no stale module is left.
rm -rf "$(php-config --extension-dir)"
make install
cd /
docker-php-source delete

php -v | grep -q DEBUG || { echo "PHP is not a debug build" >&2; exit 1; }
php -r 'exit(function_exists("zend_leak_bytes") ? 0 : 1);' \
    || { echo "zend_test (zend_leak_bytes) missing" >&2; exit 1; }
