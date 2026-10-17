<?php
// opcache.preload script of the "preload" scenario: the composer autoloader and every
// class of the app and of kafka4php/framework.
require __DIR__ . '/vendor/autoload.php';

foreach (array_keys(require __DIR__ . '/vendor/composer/autoload_classmap.php') as $class) {
    if (str_starts_with($class, 'Bench\\') || str_starts_with($class, 'Kafka4php\\')) {
        class_exists($class) || interface_exists($class);
    }
}
