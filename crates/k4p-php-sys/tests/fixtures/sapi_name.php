<?php
\Kafka4php\Native\result([
    'type' => 'content',
    'messages' => [[
        'topic' => 'sapi',
        'value' => json_encode([
            'PHP_SAPI' => PHP_SAPI,
            'php_sapi_name' => php_sapi_name(),
            'is_kafka4php' => PHP_SAPI === 'kafka4php',
            'extension_loaded' => extension_loaded('kafka4php'),
            'zts' => PHP_ZTS,
            'script_filename' => $_SERVER['SCRIPT_FILENAME'] ?? null,
            'display_errors' => ini_get('display_errors'),
            'log_errors' => ini_get('log_errors'),
            'max_execution_time' => ini_get('max_execution_time'),
            'k4p_test_ini' => ini_get('precision'),
        ]),
    ]],
]);
