# Test-only image for the k4p-php-sys leak check (PLAN.md S2): the kafka4php toolchain
# image with PHP rebuilt from the same source as a ZTS *debug* build (--enable-debug).
# A debug build tracks every emalloc() and reports request-memory leaks on stderr at
# request shutdown, and enables ZEND_ASSERT()s. Used by
# crates/k4p-php-sys/tests/leak-check.sh; not used for anything else.
#
#   docker build -f crates/k4p-php-sys/tests/debug-php.Dockerfile \
#       --build-arg PHP_VERSION=8.4 -t kafka4php-dev:php8.4-debug crates/k4p-php-sys/tests
#
# The base (kafka4php-dev:php<ver>) is built by scripts/dev.sh. It is
# php:<ver>-zts-bookworm plus the toolchain, so docker-php-source still has the exact
# source of the installed PHP.

ARG PHP_VERSION=8.4
FROM kafka4php-dev:php${PHP_VERSION}

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends $PHPIZE_DEPS \
        libargon2-dev libcurl4-openssl-dev libonig-dev libreadline-dev \
        libsodium-dev libsqlite3-dev libssl-dev libxml2-dev zlib1g-dev; \
    rm -rf /var/lib/apt/lists/*; \
    # Original configure flags, minus the trailing VAR=value pairs.
    configure_options="$(php-config --configure-options | sed -E 's/ build_alias=.*$//')"; \
    export CFLAGS="$PHP_CFLAGS" CPPFLAGS="$PHP_CPPFLAGS" LDFLAGS="$PHP_LDFLAGS"; \
    docker-php-source extract; \
    cd /usr/src/php; \
    ./configure $configure_options \
        --enable-zts --enable-embed --enable-zend-max-execution-timers --enable-debug \
        # zend_test provides zend_leak_bytes(): the leak check's positive control.
        --enable-zend-test; \
    make -j"$(nproc)"; \
    find . -type f -name '*.a' -delete; \
    # Extensions installed by the official image as shared modules (sodium, and opcache
    # on 8.4) must be rebuilt for the debug ABI: make install replaces the ones built
    # here; drop any stale ones that would fail to load.
    rm -rf "$(php-config --extension-dir)"; \
    make install; \
    cd /; \
    docker-php-source delete; \
    php -v | grep -q 'DEBUG'; \
    php -v | grep -q 'ZTS'; \
    php -r 'exit(function_exists("zend_leak_bytes") ? 0 : 1);'; \
    [ -f "$(php-config --prefix)/lib/libphp.so" ]; \
    php -m | grep -q '^Zend OPcache$' || docker-php-ext-enable opcache; \
    php -m; \
    php -v
