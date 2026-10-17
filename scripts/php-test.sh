#!/usr/bin/env bash
# Run the checks of the PHP framework (php/): `composer test` (PHPUnit) and
# `composer stan` (PHPStan), on a stock php:<ver>-cli image - no toolchain image.
#
#   scripts/php-test.sh                    # PHP 8.4
#   PHP_VERSION=8.5 scripts/php-test.sh
#
# Dependencies are installed into php/vendor/ by the composer image (php:*-cli has
# neither unzip nor git); composer.json pins the platform to PHP 8.4, so the same
# vendor/ works for every supported version. The composer binary is then copied from
# that image into a named volume and run with the PHP under test.
#
# Env: PHP_VERSION (default 8.4).
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
php_version="${PHP_VERSION:-8.4}"
composer_image="composer:2.8"

host_php="$root/php"
if command -v cygpath >/dev/null 2>&1; then
    # Git Bash / MSYS: see scripts/dev.sh.
    host_php="$(cygpath -m "$host_php")"
    export MSYS_NO_PATHCONV=1
fi

project=(-v "$host_php:/app" -w /app -e COMPOSER_ROOT_VERSION=dev-main)

docker run --rm "${project[@]}" -v k4p-composer-cache:/tmp/cache -e COMPOSER_CACHE_DIR=/tmp/cache \
    "$composer_image" composer install --no-interaction --no-progress >&2
docker run --rm -v k4p-composer-bin:/out --entrypoint cp "$composer_image" /usr/bin/composer /out/composer

docker run --rm "${project[@]}" -v k4p-composer-bin:/opt/composer:ro \
    "php:${php_version}-cli" sh -euc '
        php -v | head -n 1
        php /opt/composer/composer test
        php /opt/composer/composer stan
    '
