#!/bin/sh
# Fails the image build unless the base image's PHP is ZTS with the embed SAPI
# (libphp.so), Zend max execution timers and OPcache (DESIGN.md §8). We do not
# rebuild PHP: if an official php:*-zts image ever lacks one of these, the build
# must fail loudly rather than silently diverge from the base image.
set -eu

fail=0
check() {
    if eval "$2"; then
        echo "ok:      $1"
    else
        echo "MISSING: $1" >&2
        fail=1
    fi
}

check "ZTS" 'php -r "exit(PHP_ZTS ? 0 : 1);"'
check "embed SAPI" 'php-config --php-sapis | grep -qw embed'
check "libphp.so" '[ -f "$(php-config --prefix)/lib/libphp.so" ]'
check "Zend max execution timers" 'php -i | grep -q "^Zend Max Execution Timers => enabled"'
check "OPcache" 'php -m | grep -q "^Zend OPcache$"'

php -v
if [ "$fail" -ne 0 ]; then
    echo "The base image's PHP ${PHP_VERSION:-} lacks what kafka4php needs (see above)." >&2
    exit 1
fi
