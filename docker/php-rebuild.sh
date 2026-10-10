#!/bin/sh
# Ensures the PHP in a php:*-zts image is ZTS + embed (libphp.so) + Zend max execution
# timers, with OPcache enabled. Rebuilds PHP from the image's own source
# (docker-php-source) with the image's original configure flags when needed.
#
# Env: FORCE_PHP_REBUILD=1 always rebuilds; PHP_EXTRA_CONFIGURE_ARGS are appended to
# configure and imply a rebuild.
set -eu

problems=""
php -r 'exit(PHP_ZTS ? 0 : 1);' || problems="$problems not-zts"
php-config --php-sapis | grep -qw embed || problems="$problems no-embed-sapi"
[ -f "$(php-config --prefix)/lib/libphp.so" ] || problems="$problems no-libphp.so"
php -i | grep -q '^Zend Max Execution Timers => enabled' || problems="$problems no-max-execution-timers"
[ "${FORCE_PHP_REBUILD:-0}" = 1 ] && problems="$problems forced"
[ -n "${PHP_EXTRA_CONFIGURE_ARGS:-}" ] && problems="$problems extra-configure-args"

if [ -n "$problems" ]; then
    echo "k4p-php-rebuild: rebuilding PHP ($problems )"
    set -x

    # Build deps of the official image (its Dockerfile purges them after building).
    apt-get update
    # shellcheck disable=SC2086
    apt-get install -y --no-install-recommends $PHPIZE_DEPS \
        libargon2-dev libcurl4-openssl-dev libonig-dev libreadline-dev \
        libsodium-dev libsqlite3-dev libssl-dev libxml2-dev zlib1g-dev
    rm -rf /var/lib/apt/lists/*

    # Original flags, minus the trailing VAR=value pairs (PHP_UNAME has spaces).
    configure_options="$(php-config --configure-options | sed -E 's/ build_alias=.*$//')"

    export CFLAGS="$PHP_CFLAGS" CPPFLAGS="$PHP_CPPFLAGS" LDFLAGS="$PHP_LDFLAGS"
    export PHP_UNAME='Linux - Docker' PHP_BUILD_PROVIDER='https://github.com/docker-library/php'

    docker-php-source extract
    cd /usr/src/php
    # shellcheck disable=SC2086
    ./configure $configure_options \
        --enable-zts --enable-embed --enable-zend-max-execution-timers \
        ${PHP_EXTRA_CONFIGURE_ARGS:-}
    make -j"$(nproc)"
    find . -type f -name '*.a' -delete
    make install
    make clean
    cp -v php.ini-* /usr/local/etc/php/
    cd /
    docker-php-source delete
    pecl update-channels
    rm -rf /tmp/pear ~/.pearrc
    set +x
fi

# OPcache: a shared extension up to PHP 8.4, always built in from 8.5.
php -m | grep -q '^Zend OPcache$' || docker-php-ext-enable opcache

# Fail the image build if anything is still missing.
php -v | grep -q '(ZTS' || { echo "PHP is not ZTS" >&2; exit 1; }
php-config --php-sapis | grep -qw embed || { echo "embed SAPI missing" >&2; exit 1; }
[ -f "$(php-config --prefix)/lib/libphp.so" ] || { echo "libphp.so missing" >&2; exit 1; }
php -i | grep -q '^Zend Max Execution Timers => enabled' \
    || { echo "Zend max execution timers disabled" >&2; exit 1; }
php -m | grep -q '^Zend OPcache$' || { echo "OPcache not loaded" >&2; exit 1; }
php -v
